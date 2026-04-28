from datetime import datetime as dt
from datetime import timedelta

from typing import Any, Union


def parse_float_with_try(v: Any) -> Union[float, int]:
    """convert v to float or 0"""
    try:
        return float(v)
    except ValueError:
        try:
            return int(v)
        except Exception:
            return 0


def parse_date_as_timestamp(v: Any) -> int:
    if not v or not isinstance(v, str):
        raise ValueError(f"无效的日期值: {v!r}")
    return int(dt.fromisoformat(v).timestamp())


def AppleStandHourFormatter(record: dict) -> list:
    date = parse_date_as_timestamp(record.get("startDate", "2024-01-01T00:00:00"))
    unit = record.get("unit", "unit")
    device = record.get("sourceName", "unknown")
    value = 1 if record.get("value") == "HKCategoryValueAppleStandHourStood" else 0

    return [{
        "measurement": "AppleStandHour",
        "time": date,
        "fields": {"value": value},
        "tags": {"unit": unit, "device": device},
    }]


sleep_states_lookup = {
    "HKCategoryValueSleepAnalysisAsleepDeep": 0,
    "HKCategoryValueSleepAnalysisAsleepCore": 1,
    "HKCategoryValueSleepAnalysisAsleepREM": 2,
    "HKCategoryValueSleepAnalysisAsleepUnspecified": 3,
    "HKCategoryValueSleepAnalysisInBed": 3,
    "HKCategoryValueSleepAnalysisAwake": 4,
}

sleep_states_short_lookup = {
    "HKCategoryValueSleepAnalysisAsleepDeep": "Deep",
    "HKCategoryValueSleepAnalysisAsleepCore": "Core",
    "HKCategoryValueSleepAnalysisAsleepREM": "REM",
    "HKCategoryValueSleepAnalysisAsleepUnspecified": "Asleep",
    "HKCategoryValueSleepAnalysisInBed": "Asleep",
    "HKCategoryValueSleepAnalysisAwake": "Awake",
}


def SleepAnalysisFormatter(record: dict) -> list:
    start_date = dt.fromisoformat(record.get("startDate"))
    # replace() 返回新对象，截断秒
    start_date = start_date.replace(second=0, microsecond=0)
    end_date = dt.fromisoformat(record.get("endDate"))
    device = record.get("sourceName", "unknown")
    raw_state = record.get("value", "")
    state = sleep_states_lookup.get(raw_state, 5)
    short_state = sleep_states_short_lookup.get(raw_state, "Unspecified")

    minutes_in_bed = []
    cur = start_date
    while cur <= end_date:
        minutes_in_bed.append({
            "measurement": "SleepAnalysisTimes-{}".format(device),
            "time": int(cur.timestamp()),
            "fields": {"value": state},
            "tags": {},
        })
        cur += timedelta(minutes=1)

    start_ts = int(dt.fromisoformat(record.get("startDate")).timestamp())
    end_ts = int(dt.fromisoformat(record.get("endDate")).timestamp())
    minutes_in_bed.append({
        "measurement": "SleepAnalysis",
        "time": start_ts,
        "fields": {
            "value": end_ts - start_ts,
            "start": start_ts,
            "stop": end_ts,
        },
        "tags": {"unit": "seconds", "device": device, "state": short_state},
    })
    return minutes_in_bed
