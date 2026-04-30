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
from datetime import datetime, timezone
from typing import Any

from flask import Flask, jsonify, redirect, request, send_from_directory, url_for
from influxdb import InfluxDBClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INFLUX_HOST = os.getenv("INFLUX_HOST", "influx")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "health")
API_KEY = os.getenv("API_KEY", "")

HEARTBEAT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "heartbeat")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB 请求体上限
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
    "toothbrushing": "ToothbrushingEvent",
    "noise_exposure": "EnvironmentalAudioExposure",
    "headphone_audio_exposure": "HeadphoneAudioExposure",
    "uv_exposure": "UVExposure",
}

# 每天只有一条汇总记录的指标：写入时将时间戳归一化到当天 UTC 零点，
# 使重复导入自动覆盖而非追加。心率、血压等逐次读数指标不在此列。
_DAILY_AGGREGATE_METRICS: frozenset[str] = frozenset({
    "step_count",
    "active_energy",
    "basal_energy_burned",
    "dietary_energy",
    "walking_running_distance",
    "cycling_distance",
    "swimming_distance",
    "flights_climbed",
    "exercise_time",
    "stand_time",
    "stand_hour",
    "walking_speed",
    "walking_step_length",
    "walking_double_support_percentage",
    "walking_asymmetry_percentage",
    "stair_ascent_speed",
    "stair_descent_speed",
    "six_minute_walk_test_distance",
    "vo2_max",
    "weight_body_mass",
    "body_mass_index",
    "body_fat_percentage",
    "lean_body_mass",
    "height",
    "waist_circumference",
    "dietary_water",
    "dietary_caffeine",
    "mindful_minutes",
    "handwashing",
    "toothbrushing",
    "noise_exposure",
    "headphone_audio_exposure",
    "uv_exposure",
})
SLEEP_DURATION_STATES: dict[str, str] = {
    "inBed": "HKCategoryValueSleepAnalysisInBed",
    "asleep": "HKCategoryValueSleepAnalysisAsleepUnspecified",
    "core": "HKCategoryValueSleepAnalysisAsleepCore",
    "deep": "HKCategoryValueSleepAnalysisAsleepDeep",
    "rem": "HKCategoryValueSleepAnalysisAsleepREM",
    "awake": "HKCategoryValueSleepAnalysisAwake",
}

SLEEP_STATE_VALUES: dict[str, int] = {
    "HKCategoryValueSleepAnalysisAsleepDeep": 0,
    "HKCategoryValueSleepAnalysisAsleepCore": 1,
    "HKCategoryValueSleepAnalysisAsleepREM": 2,
    "HKCategoryValueSleepAnalysisInBed": 3,
    "HKCategoryValueSleepAnalysisAsleepUnspecified": 3,
    "HKCategoryValueSleepAnalysisAwake": 4,
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


def _bpm_from_metric_fields(fields: dict[str, Any]) -> float | None:
    """Pick a sensible BPM number from HeartRate Influx fields (value / Min / Max / …)."""
    if not fields:
        return None
    for key in ("value", "Avg", "Min", "Max", "avg", "min", "max"):
        raw = fields.get(key)
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _time_to_iso(ts: Any) -> str | None:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()  # type: ignore[no-any-return]
    return str(ts)


def _parse_date(date_str: Any) -> int | None:
    """Parse Health Auto Export date string → unix epoch seconds.
    Accepted formats:
      '2024-02-06 14:30:00 -0800'
      '2024-02-06'
      ISO-8601 variants
    """
    if not date_str:
        return None
    date_str = str(date_str).strip()
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
        log.warning("无法解析日期，已跳过该数据点: %r", date_str)
        return None


def _parse_date_as_utc_day(date_str: Any) -> int | None:
    """从日期字符串中提取 YYYY-MM-DD 部分，返回该日期的 UTC 零点 epoch。

    忽略时间和时区偏移，使同一本地日期无论何时导出都映射到相同时间戳。
    这解决了 UTC+8 等时区下早晨导出（本地日期 N，UTC 却是 N-1 日）导致的
    同一天数据落在不同 UTC 日的问题。
    """
    if not date_str:
        return None
    s = str(date_str).strip()
    if len(s) >= 10:
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            pass
    return _parse_date(date_str)


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _duration_to_seconds(value: Any, units: str) -> float | None:
    duration = _to_float(value)
    if duration is None:
        return None

    normalized_units = units.lower()
    if normalized_units in ("h", "hr", "hrs", "hour", "hours"):
        return duration * 3600
    if normalized_units in ("m", "min", "mins", "minute", "minutes"):
        return duration * 60
    return duration


def _snake_to_measurement(name: str) -> str:
    """Convert snake_case metric name to InfluxDB measurement."""
    if name in METRIC_NAME_MAP:
        return METRIC_NAME_MAP[name]
    return "".join(w.capitalize() for w in name.split("_"))


_DAILY_AGGREGATE_MEASUREMENTS: frozenset[str] = frozenset(
    _snake_to_measurement(name) for name in _DAILY_AGGREGATE_METRICS
)


def _sleep_state_from_datapoint(dp: dict[str, Any]) -> str | None:
    raw_state = (
        dp.get("state")
        or dp.get("stage")
        or dp.get("sleepStage")
        or dp.get("value")
        or dp.get("type")
    )
    if raw_state is None:
        return None

    state = str(raw_state)
    if state.startswith("HKCategoryValueSleepAnalysis"):
        return state

    normalized = state.replace(" ", "").replace("_", "").replace("-", "").lower()
    lookup = {
        "inbed": "HKCategoryValueSleepAnalysisInBed",
        "asleep": "HKCategoryValueSleepAnalysisAsleepUnspecified",
        "asleepunspecified": "HKCategoryValueSleepAnalysisAsleepUnspecified",
        "core": "HKCategoryValueSleepAnalysisAsleepCore",
        "asleepcore": "HKCategoryValueSleepAnalysisAsleepCore",
        "deep": "HKCategoryValueSleepAnalysisAsleepDeep",
        "asleepdeep": "HKCategoryValueSleepAnalysisAsleepDeep",
        "rem": "HKCategoryValueSleepAnalysisAsleepREM",
        "asleeprem": "HKCategoryValueSleepAnalysisAsleepREM",
        "awake": "HKCategoryValueSleepAnalysisAwake",
    }
    return lookup.get(normalized)


def _parse_sleep_interval(dp: dict[str, Any]) -> tuple[int, int] | None:
    start = (
        dp.get("start")
        or dp.get("startDate")
        or dp.get("from")
        or dp.get("sleepStart")
    )
    end = (
        dp.get("end")
        or dp.get("endDate")
        or dp.get("to")
        or dp.get("sleepEnd")
    )
    start_ts = _parse_date(start)
    end_ts = _parse_date(end)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        return None
    return start_ts, end_ts


def _append_sleep_stage_points(
    points: list[dict],
    *,
    source: str,
    start_ts: int,
    end_ts: int,
    state: str,
) -> None:
    state_value = SLEEP_STATE_VALUES.get(state)
    if state_value is None:
        return

    for measurement in ("SleepAnalysisTimes", f"SleepAnalysisTimes-{source}"):
        ts = start_ts
        while ts < end_ts:
            points.append({
                "measurement": measurement,
                "time": ts,
                "fields": {"value": state_value},
                "tags": {"device": source} if measurement == "SleepAnalysisTimes" else {},
            })
            ts += 60


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
        date_str = dp.get("date", "")
        if name in _DAILY_AGGREGATE_METRICS:
            # 只取 YYYY-MM-DD 部分映射到 UTC 零点，避免时区偏移导致同一本地日期
            # 在早晨/夜晚导出时落到不同 UTC 日而产生重复数据点
            ts = _parse_date_as_utc_day(date_str)
        else:
            ts = _parse_date(date_str)
        if ts is None:
            continue
        source = dp.get("source", "Health Auto Export")

        if name == "sleep_analysis":
            wrote_sleep_point = False
            duration_keys = [key for key in SLEEP_DURATION_STATES if key in dp]
            if not duration_keys and "totalSleep" in dp:
                duration_keys = ["totalSleep"]

            for key in duration_keys:
                state = SLEEP_DURATION_STATES.get(
                    key, "HKCategoryValueSleepAnalysisAsleepUnspecified"
                )
                seconds = _duration_to_seconds(dp.get(key), units)
                if seconds is None:
                    continue
                points.append({
                    "measurement": "SleepAnalysis",
                    "time": ts,
                    "fields": {"value": seconds},
                    "tags": {"unit": "seconds", "device": source, "state": state},
                })
                wrote_sleep_point = True

            interval = _parse_sleep_interval(dp)
            state = _sleep_state_from_datapoint(dp)
            if interval and state:
                start_ts, end_ts = interval
                _append_sleep_stage_points(
                    points,
                    source=source,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    state=state,
                )
                points.append({
                    "measurement": "SleepAnalysis",
                    "time": start_ts,
                    "fields": {"value": end_ts - start_ts},
                    "tags": {"unit": "seconds", "device": source, "state": state},
                })
                wrote_sleep_point = True

            if not wrote_sleep_point:
                log.warning("无法识别 sleep_analysis 数据点，已跳过: %s", dp)
            continue

        if name == "blood_pressure":
            fields = {}
            if "systolic" in dp:
                systolic = _to_float(dp["systolic"])
                if systolic is not None:
                    fields["systolic"] = systolic
            if "diastolic" in dp:
                diastolic = _to_float(dp["diastolic"])
                if diastolic is not None:
                    fields["diastolic"] = diastolic
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
                    value = _to_float(dp[key])
                    if value is not None:
                        fields[key if key != "qty" else "value"] = value
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
        value = _to_float(value)
        if value is None:
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
    if ts is None:
        return []
    duration = _to_float(w.get("duration", 0))
    if duration is None:
        duration = 0
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
            lat = _to_float(rp.get("lat"))
            lon = _to_float(rp.get("lon"))
            if lat is None or lon is None:
                continue
            rts = _parse_date(rp.get("date", rp.get("timestamp", "")))
            if rts is None:
                continue
            fields: dict[str, float] = {
                "latitude": lat,
                "longitude": lon,
            }
            if "altitude" in rp:
                altitude = _to_float(rp["altitude"])
                if altitude is not None:
                    fields["elevation"] = altitude
            if "speed" in rp:
                speed = _to_float(rp["speed"])
                if speed is not None:
                    fields["speed"] = speed
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
    if API_KEY and request.endpoint not in ("health", "heartbeat_index", "heartbeat_redirect"):
        key = request.headers.get("X-API-Key", "")
        if key != API_KEY:
            return jsonify({"error": "无效的 API Key"}), 401


@app.route("/health", methods=["GET"])
def health():
    """简单健康检查，也可用于 Health Auto Export 连通性测试。"""
    return jsonify({"status": "ok"})


@app.route("/heartbeat")
def heartbeat_redirect():
    return redirect(url_for("heartbeat_index"), code=302)


@app.route("/heartbeat/")
def heartbeat_index():
    """静态页面：心率展示（参考 iBeats 风格）。"""
    return send_from_directory(HEARTBEAT_DIR, "index.html")


@app.route("/api/heartbeat/recent", methods=["GET"])
def heartbeat_recent():
    """从 InfluxDB 读取最近 HeartRate 点，供 /heartbeat/ 页面使用。"""
    try:
        limit = int(request.args.get("limit", "80"))
    except ValueError:
        limit = 80
    limit = max(1, min(limit, 500))

    try:
        client = _get_influx()
        q = f'SELECT * FROM "HeartRate" ORDER BY time DESC LIMIT {limit}'
        rs = client.query(q)
    except Exception as err:
        log.exception("查询 HeartRate 失败。")
        return jsonify({"error": "查询失败", "detail": str(err)}), 502

    readings: list[dict[str, Any]] = []
    for row in rs.get_points("HeartRate"):
        bpm = _bpm_from_metric_fields({k: v for k, v in row.items() if k != "time"})
        if bpm is None:
            continue
        tiso = _time_to_iso(row.get("time"))
        if tiso is None:
            continue
        readings.append({"time": tiso, "bpm": bpm})

    latest = readings[0] if readings else None
    return jsonify({"readings": readings, "latest": latest})


def _delete_existing_day_points(client: InfluxDBClient, points: list[dict]) -> None:
    """写入前删除 InfluxDB 中相同 (measurement, tags, 当天) 的所有旧数据。

    这是去重的核心保障：无论历史数据以何种时间戳写入（精确时间戳、
    UTC 零点或其他归一化形式），DELETE 都会将其全部清除，再由本次导入
    的数据原样覆盖，确保每天每个指标只保留最新一次同步的结果。
    """
    seen: set[tuple] = set()
    for p in points:
        measurement = p.get("measurement")
        ts = p.get("time")
        if not measurement or ts is None:
            continue
        tags: dict = p.get("tags") or {}
        # 每日汇总类指标（步数、步行/跑步距离等）在 Health App 里本质上是“当天总量”，
        # 不应按 source/device 分片累加。若按完整 tags 删除，source 或 unit 变化时会残留
        # 旧点，导致同一天被重复计算。这里对每日汇总直接按 measurement+day 全量覆盖。
        if measurement in _DAILY_AGGREGATE_MEASUREMENTS and tags:
            tags = {}
        day_start = (int(ts) // 86400) * 86400
        key = (measurement, tuple(sorted(tags.items())), day_start)
        if key in seen:
            continue
        seen.add(key)

        day_end = day_start + 86400
        start_str = datetime.utcfromtimestamp(day_start).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = datetime.utcfromtimestamp(day_end).strftime("%Y-%m-%dT%H:%M:%SZ")

        tag_cond = " AND ".join(
            f'"{k}"=\'{v}\'' for k, v in sorted(tags.items())
        )
        if tag_cond:
            q = (
                f'DELETE FROM "{measurement}" WHERE {tag_cond}'
                f" AND time >= '{start_str}' AND time < '{end_str}'"
            )
        else:
            q = f'DELETE FROM "{measurement}" WHERE time >= \'{start_str}\' AND time < \'{end_str}\''

        try:
            client.query(q)
        except Exception as exc:
            log.warning("删除旧数据失败（%s）: %s", measurement, exc)


@app.route("/api/healthautoexport", methods=["POST"])
def ingest():
    """接收 Health Auto Export 发送的 JSON 数据。"""
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"error": "无法解析 JSON"}), 400
    if not isinstance(payload, dict):
        return jsonify({"error": "JSON 根对象必须是 object"}), 400

    data = payload.get("data", payload)
    if not isinstance(data, dict):
        return jsonify({"error": "JSON 格式不正确，缺少 data 对象"}), 400

    metrics_list = data.get("metrics", [])
    workouts_list = data.get("workouts", [])
    if not isinstance(metrics_list, list) or not isinstance(workouts_list, list):
        return jsonify({"error": "metrics 和 workouts 必须是数组"}), 400

    health_points: list[dict] = []
    devices: set[str] = set()
    for m in metrics_list:
        converted = _convert_metric(m)
        health_points.extend(converted)
        for p in converted:
            dev = p.get("tags", {}).get("device")
            if dev:
                devices.add(dev)
    for w in workouts_list:
        converted = _convert_workout(w)
        health_points.extend(converted)
        for p in converted:
            dev = p.get("tags", {}).get("device")
            if dev:
                devices.add(dev)

    if not health_points:
        log.info("收到请求但未解析到有效数据点。")
        return jsonify({"status": "ok", "points": 0})

    # Keep compatibility with existing dashboards that query source devices
    # from measurement "data-sources".
    source_points = [
        {"measurement": "data-sources", "fields": {"value": 1}, "tags": {"device": dev}}
        for dev in devices
    ]

    all_points = health_points + source_points
    try:
        client = _get_influx()
        _delete_existing_day_points(client, health_points)
        client.write_points(all_points, time_precision="s")
    except Exception as err:
        log.exception("写入 InfluxDB 失败。")
        return jsonify({"error": "写入 InfluxDB 失败", "detail": str(err)}), 502

    log.info("写入 %d 个健康数据点（%d 个指标，%d 个运动）",
             len(health_points), len(metrics_list), len(workouts_list))

    return jsonify({"status": "ok", "points": len(health_points)})


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
