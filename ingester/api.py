"""
REST API server that receives health data from the iOS app "Health Auto Export"
and writes it into InfluxDB, reusing the same schema as the zip-based ingester.

Endpoint:  POST /api/healthautoexport
Payload:   JSON produced by Health Auto Export (metrics / workouts / …)

Environment variables:
  INFLUX_HOST   – InfluxDB hostname   (default: influx)
  INFLUX_PORT   – InfluxDB port       (default: 8086)
  INFLUX_DB     – database name       (default: health)
  API_KEY       – optional, if set the client must send header  X-API-Key
"""

import os
import time
import logging
from datetime import datetime
from typing import Any

from flask import Flask, request, jsonify
from influxdb import InfluxDBClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INFLUX_HOST = os.getenv("INFLUX_HOST", "influx")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "health")
API_KEY = os.getenv("API_KEY", "")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("health-api")

# ---------------------------------------------------------------------------
# Health Auto Export snake_case → existing InfluxDB measurement name
# ---------------------------------------------------------------------------
METRIC_NAME_MAP: dict[str, str] = {
    "step_count": "StepCount",
    "heart_rate": "HeartRate",
    "resting_heart_rate": "RestingHeartRate",
    "walking_heart_rate_average": "WalkingHeartRateAverage",
    "heart_rate_variability_sdnn": "HeartRateVariabilitySDNN",
    "heart_rate_recovery_one_minute": "HeartRateRecoveryOneMinute",
    "active_energy": "ActiveEnergyBurned",
    "basal_energy_burned": "BasalEnergyBurned",
    "dietary_energy": "DietaryEnergyConsumed",
    "walking_running_distance": "DistanceWalkingRunning",
    "cycling_distance": "DistanceCycling",
    "swimming_distance": "DistanceSwimming",
    "flights_climbed": "FlightsClimbed",
    "exercise_time": "AppleExerciseTime",
    "stand_time": "AppleStandTime",
    "stand_hour": "AppleStandHour",
    "walking_speed": "WalkingSpeed",
    "walking_step_length": "WalkingStepLength",
    "walking_double_support_percentage": "WalkingDoubleSupportPercentage",
    "walking_asymmetry_percentage": "WalkingAsymmetryPercentage",
    "stair_ascent_speed": "StairAscentSpeed",
    "stair_descent_speed": "StairDescentSpeed",
    "six_minute_walk_test_distance": "SixMinuteWalkTestDistance",
    "vo2_max": "VO2Max",
    "respiratory_rate": "RespiratoryRate",
    "blood_oxygen_saturation": "OxygenSaturation",
    "body_temperature": "BodyTemperature",
    "blood_pressure": "BloodPressure",
    "blood_glucose": "BloodGlucose",
    "weight_body_mass": "BodyMass",
    "body_mass_index": "BodyMassIndex",
    "body_fat_percentage": "BodyFatPercentage",
    "lean_body_mass": "LeanBodyMass",
    "height": "Height",
    "waist_circumference": "WaistCircumference",
    "dietary_water": "DietaryWater",
    "dietary_caffeine": "DietaryCaffeine",
    "mindful_minutes": "MindfulSession",
    "handwashing": "HandwashingEvent",
    "toothbrushing": "AppleSleepingWristTemperature",
    "noise_exposure": "EnvironmentalAudioExposure",
    "headphone_audio_exposure": "HeadphoneAudioExposure",
    "uv_exposure": "UVExposure",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_influx() -> InfluxDBClient:
    """Return a module-level InfluxDB client, reconnecting if needed."""
    global _influx_client                                       # noqa: PLW0603
    try:
        _influx_client.ping()
    except Exception:
        _influx_client = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, database=INFLUX_DB)
    return _influx_client


def _parse_date(date_str: str) -> int:
    """Parse Health Auto Export date string → unix epoch seconds.
    Accepted formats:
      '2024-02-06 14:30:00 -0800'
      '2024-02-06'
      ISO-8601 variants
    """
    date_str = date_str.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return int(datetime.strptime(date_str, fmt).timestamp())
        except ValueError:
            continue
    try:
        return int(datetime.fromisoformat(date_str).timestamp())
    except Exception:
        return int(time.time())


def _snake_to_measurement(name: str) -> str:
    """Convert snake_case metric name to InfluxDB measurement."""
    if name in METRIC_NAME_MAP:
        return METRIC_NAME_MAP[name]
    return "".join(w.capitalize() for w in name.split("_"))


# ---------------------------------------------------------------------------
# Metric → InfluxDB points
# ---------------------------------------------------------------------------

def _convert_metric(metric: dict[str, Any]) -> list[dict]:
    """Convert one Health Auto Export metric object to influx points."""
    name = metric.get("name", "unknown")
    units = metric.get("units", "unit")
    measurement = _snake_to_measurement(name)
    points: list[dict] = []

    for dp in metric.get("data", []):
        ts = _parse_date(dp.get("date", ""))
        source = dp.get("source", "Health Auto Export")

        if name == "sleep_analysis":
            fields = {}
            for key in ("totalSleep", "asleep", "core", "deep", "rem", "inBed"):
                if key in dp:
                    fields[key] = float(dp[key])
            if not fields:
                continue
            points.append({
                "measurement": "SleepAnalysis",
                "time": ts,
                "fields": fields,
                "tags": {"unit": units, "device": source},
            })
            continue

        if name == "blood_pressure":
            fields = {}
            if "systolic" in dp:
                fields["systolic"] = float(dp["systolic"])
            if "diastolic" in dp:
                fields["diastolic"] = float(dp["diastolic"])
            if not fields:
                continue
            points.append({
                "measurement": measurement,
                "time": ts,
                "fields": fields,
                "tags": {"unit": units, "device": source},
            })
            continue

        if name == "heart_rate":
            fields: dict[str, float] = {}
            for key in ("Min", "Avg", "Max", "qty"):
                if key in dp:
                    fields[key if key != "qty" else "value"] = float(dp[key])
            if not fields:
                continue
            points.append({
                "measurement": measurement,
                "time": ts,
                "fields": fields,
                "tags": {"unit": units, "device": source},
            })
            continue

        value = dp.get("qty")
        if value is None:
            continue
        try:
            value = float(value)
        except (ValueError, TypeError):
            continue

        points.append({
            "measurement": measurement,
            "time": ts,
            "fields": {"value": value},
            "tags": {"unit": units, "device": source},
        })

    return points


# ---------------------------------------------------------------------------
# Workout → InfluxDB points
# ---------------------------------------------------------------------------

def _convert_workout(w: dict[str, Any]) -> list[dict]:
    """Convert one Health Auto Export workout object to influx points."""
    workout_name = w.get("name", "Workout")
    ts = _parse_date(w.get("start", w.get("date", "")))
    duration = w.get("duration", 0)
    source = w.get("source", "Health Auto Export")

    points: list[dict] = []
    points.append({
        "measurement": workout_name,
        "time": ts,
        "fields": {"value": float(duration)},
        "tags": {"unit": "sec", "device": source},
    })

    route = w.get("route", w.get("routeData", []))
    if route:
        slug = workout_name.replace(" ", "-").lower()
        for rp in route:
            lat = rp.get("lat")
            lon = rp.get("lon")
            if lat is None or lon is None:
                continue
            rts = _parse_date(rp.get("date", rp.get("timestamp", "")))
            fields: dict[str, float] = {
                "latitude": float(lat),
                "longitude": float(lon),
            }
            if "altitude" in rp:
                fields["elevation"] = float(rp["altitude"])
            if "speed" in rp:
                fields["speed"] = float(rp["speed"])
            points.append({
                "measurement": "workout-routes",
                "tags": {"workout": slug},
                "time": rts,
                "fields": fields,
            })

    return points


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.before_request
def _check_api_key():
    if API_KEY and request.endpoint not in ("health",):
        key = request.headers.get("X-API-Key", "")
        if key != API_KEY:
            return jsonify({"error": "无效的 API Key"}), 401


@app.route("/health", methods=["GET"])
def health():
    """简单健康检查，也可用于 Health Auto Export 连通性测试。"""
    return jsonify({"status": "ok"})


@app.route("/api/healthautoexport", methods=["POST"])
def ingest():
    """接收 Health Auto Export 发送的 JSON 数据。"""
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"error": "无法解析 JSON"}), 400

    data = payload.get("data", payload)

    metrics_list = data.get("metrics", [])
    workouts_list = data.get("workouts", [])

    points: list[dict] = []
    for m in metrics_list:
        points.extend(_convert_metric(m))
    for w in workouts_list:
        points.extend(_convert_workout(w))

    if not points:
        log.info("收到请求但未解析到有效数据点。")
        return jsonify({"status": "ok", "points": 0})

    client = _get_influx()
    client.write_points(points, time_precision="s")
    log.info("写入 %d 个数据点（%d 个指标，%d 个运动）",
             len(points), len(metrics_list), len(workouts_list))

    return jsonify({"status": "ok", "points": len(points)})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

_influx_client: InfluxDBClient = None  # type: ignore[assignment]

def _wait_for_influx():
    global _influx_client  # noqa: PLW0603
    while True:
        try:
            c = InfluxDBClient(INFLUX_HOST, INFLUX_PORT, database=INFLUX_DB)
            c.ping()
            c.create_database(INFLUX_DB)
            _influx_client = c
            log.info("InfluxDB 已连接 (%s:%s/%s)", INFLUX_HOST, INFLUX_PORT, INFLUX_DB)
            return
        except Exception:
            log.info("等待 InfluxDB 就绪 …")
            time.sleep(2)


if __name__ == "__main__":
    _wait_for_influx()
    port = int(os.getenv("API_PORT", "5353"))
    log.info("Health Auto Export API 已启动，监听端口 %d", port)
    app.run(host="0.0.0.0", port=port)
