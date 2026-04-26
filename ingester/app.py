"""
Ingester module that converts Apple Health export zip file
into influx db datapoints
"""
import os
import re
import time
from lxml import etree
from shutil import unpack_archive
from typing import Any
import subprocess

from formatters import parse_date_as_timestamp, parse_float_with_try, AppleStandHourFormatter, SleepAnalysisFormatter

import gpxpy
from gpxpy.gpx import GPXTrackPoint
from influxdb import InfluxDBClient

ZIP_PATH = "/export.zip"
ROUTES_PATH = "/export/apple_health_export/workout-routes/"
EXPORT_PATH = "/export/apple_health_export"
EXPORT_XML_REGEX = re.compile(r"(export|导出|dati esportati)\.xml",re.IGNORECASE)
RESET_INFLUX = os.getenv("RESET_INFLUX", "false").lower() in ("1", "true", "yes")

points_sources = set()

def format_route_point(
    name: str, point: GPXTrackPoint, next_point=None
) -> (
        dict)[str, Any]:
    """for a given `point`, creates an influxdb point
    and computes speed and distance if `next_point` exists"""
    slug_name = name.replace(" ", "-").replace(":", "-").lower()
    datapoint = {
        "measurement": "workout-routes",
        "tags": {"workout": slug_name},
        "time": point.time,
        "fields": {
            "latitude": point.latitude,
            "longitude": point.longitude,
            "elevation": point.elevation,
        },
    }
    if next_point:
        datapoint["fields"]["speed"] = (
            point.speed_between(next_point) if next_point else 0
        )
        datapoint["fields"]["distance"] = point.distance_3d(next_point)
    return datapoint


def format_record(record: dict[str, Any]) -> dict[str, Any]:
    """format a export health xml record for influx"""
    measurement = (
        record.get("type", "Record")
        .removeprefix("HKQuantityTypeIdentifier")
        .removeprefix("HKCategoryTypeIdentifier")
        .removeprefix("HKDataType")
    )

    if measurement == "AppleStandHour":
        return AppleStandHourFormatter(record)
    if measurement == "SleepAnalysis":
        return SleepAnalysisFormatter(record)

    date = parse_date_as_timestamp(record.get("startDate", "2024-01-01T01:01:01"))
    value = parse_float_with_try(record.get("value", 1))
    unit = record.get("unit", "unit")
    device = record.get("sourceName", "unknown")

    return [{
        "measurement": measurement,
        "time": date,
        "fields": {"value": value},
        "tags": {"unit": unit, "device": device},
    }]


def format_workout(record: dict[str, Any]) -> dict[str, Any]:
    """format a export health xml workout record for influx"""
    measurement = record.get("workoutActivityType", "Workout").removeprefix(
        "HKWorkoutActivityType"
    )
    date = parse_date_as_timestamp(record.get("startDate", "2024-01-01T01:01:01"))
    value = parse_float_with_try(record.get("duration", 0))
    unit = record.get("durationUnit", "unit")
    device = record.get("sourceName", "unknown")

    return {
        "measurement": measurement,
        "time": date,
        "fields": {"value": value},
        "tags": {"unit": unit, "device": device},
    }


def parse_workout_route(client: InfluxDBClient, route_xml_file: str) -> None:
    with open(route_xml_file, "r") as gpx_file:
        gpx = gpxpy.parse(gpx_file)
        for track in gpx.tracks:
            track_points = []
            print("正在打开路线：", track.name)
            for segment in track.segments:
                num_points = len(segment.points)
                for i in range(num_points):
                    track_points.append(
                        format_route_point(
                            track.name,
                            segment.points[i],
                            segment.points[i + 1] if i + 1 < num_points else None,
                        )
                    )
            client.write_points(track_points, time_precision="s")


def process_workout_routes(client: InfluxDBClient) -> None:
    if os.path.exists(ROUTES_PATH) and os.path.isdir(ROUTES_PATH):
        print("正在加载运动路线 GPX …")
        for file in os.listdir(ROUTES_PATH):
            if file.endswith(".gpx"):
                route_file = os.path.join(ROUTES_PATH, file)
                parse_workout_route(client, route_file)
    else:
        print("未找到运动路线目录，跳过 GPX 导入。")


def find_export_xml() -> str | None:
    """Find Apple Health export XML in common localized export archives."""
    xml_files = []
    for root, _, files in os.walk("/export"):
        for file in files:
            if file.lower().endswith(".xml"):
                xml_files.append(os.path.join(root, file))

    for xml_file in xml_files:
        if EXPORT_XML_REGEX.match(os.path.basename(xml_file)):
            return xml_file

    for xml_file in xml_files:
        try:
            with open(xml_file, "rb") as file:
                if b"<HealthData" in file.read(1024 * 1024):
                    return xml_file
        except OSError:
            continue

    if xml_files:
        print("找到 XML 文件但无法识别为 Apple 健康导出：", xml_files)
    return None


def process_health_data(client: InfluxDBClient) -> None:
    export_file = find_export_xml()
    if not export_file:
        print("未找到 export.xml / 导出.xml，跳过健康记录导入。")
        return
    print("导出文件：", export_file)

    print("正在清理可能损坏的 XML 前缀 …")
    p = subprocess.run(["sed", "-i", "/<HealthData/,$!d", export_file], capture_output=True)
    if p.returncode != 0:
        print(p.stdout,p.stderr)

    records = []
    total_count = 0
    context = etree.iterparse(export_file,recover=True)
    for _, elem in context:
        try:
            points_sources.add(elem.get("sourceName", "unknown"))

            if elem.tag == "Record":
                rec = format_record(elem)
                records += rec
            elif elem.tag == "Workout":
                records.append(format_workout(elem))
            elem.clear()
        except Exception as unknown_err:
            print(f"{etree.tostring(elem).decode('UTF-8')}: {unknown_err}")
        # batch push every ~10000
        if len(records) >= 10000:
            total_count += len(records)
            client.write_points(records, time_precision="s")

            del records
            records = []
            print("已写入", total_count, "条记录")

    # push the rest
    client.write_points(records, time_precision="s")
    print("记录总数：", total_count + len(records))

def push_sources(client: InfluxDBClient):
    sources_points = [{
        "measurement": "data-sources",
        "tags": {"device": s},
        "fields":{"value":1}
    }
    for s in points_sources]
    print("正在写入数据来源标签，共", len(sources_points), "个来源。")
    client.write_points(sources_points,time_precision="s")

if __name__ == "__main__":
    print("正在解压导出压缩包 …")
    try:
        unpack_archive(ZIP_PATH, "/export")
    except Exception as unzip_err:
        print("无法打开导出 zip：", unzip_err)
        exit(1)
    print("解压完成。")

    client = InfluxDBClient("influx", 8086, database="health")

    while True:
        try:
            client.ping()
            if RESET_INFLUX:
                print("RESET_INFLUX=true，正在清空 InfluxDB health 数据库。")
                client.drop_database("health")
            client.create_database("health")
            print("InfluxDB 已就绪。")
            break
        except Exception:
            print("等待 InfluxDB 就绪 …")
            time.sleep(1)

    process_workout_routes(client)
    process_health_data(client)
    push_sources(client)
    print("全部完成。请在浏览器中打开 Grafana 查看仪表板。")
