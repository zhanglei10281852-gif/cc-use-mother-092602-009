"""批次内容解析与逐行校验。

解析只负责把 CSV/JSON 文本变成原始行，校验负责把原始行变成规范化读数；
所有行级问题都以错误条目返回，绝不抛出中断，保证坏行不会阻塞好行。
文档级问题（JSON 无法解析、CSV 缺少表头）才抛出 ValidationError。
"""

from __future__ import annotations

import csv
import io
import json
import math
from datetime import UTC, datetime
from typing import Any

from app.core.clock import to_storage
from app.core.errors import ValidationError

REQUIRED_COLUMNS = [
    "payload",
    "cycle_phase",
    "model_version",
    "recorded_at",
    "temperature",
    "temperature_unit",
    "runtime",
    "runtime_unit",
]
OPTIONAL_COLUMNS = ["anomaly_flag", "revision", "sensor_id"]
ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

# 温度单位别名 -> (比例, 偏移)，摄氏度 = 原值 * 比例 + 偏移
TEMPERATURE_UNITS: dict[str, tuple[float, float]] = {
    "c": (1.0, 0.0),
    "°c": (1.0, 0.0),
    "celsius": (1.0, 0.0),
    "centigrade": (1.0, 0.0),
    "k": (1.0, -273.15),
    "kelvin": (1.0, -273.15),
    "f": (5.0 / 9.0, -160.0 / 9.0),
    "°f": (5.0 / 9.0, -160.0 / 9.0),
    "fahrenheit": (5.0 / 9.0, -160.0 / 9.0),
}

# 运行时长单位别名 -> 秒数倍数
RUNTIME_UNITS: dict[str, float] = {
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "min": 60.0,
    "mins": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hrs": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
}

FLAG_TRUE = {"1", "true", "yes", "y"}
FLAG_FALSE = {"0", "false", "no", "n", ""}

MIN_TEMPERATURE_C = -273.15
MAX_TEMPERATURE_C = 3000.0
MAX_RUNTIME_SECONDS = 2_592_000.0  # 30 天，超出视为字段级错误而非规则异常
MAX_REVISION = 1_000_000

TEXT_LIMITS = {"payload": 120, "cycle_phase": 60, "model_version": 60, "sensor_id": 120}


def parse_batch_content(format: str, content: str) -> tuple[list[tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
    """解析批次内容，返回 (数据行列表, 行形状错误列表)。

    数据行列表元素为 (行号, 原始行字典)；行号从 1 开始且在批次内唯一，
    用于错误报告的稳定排序。
    """

    if format == "csv":
        return _parse_csv(content)
    if format == "json":
        return _parse_json(content)
    raise ValidationError("不支持的批次格式", context={"format": format})


def _parse_csv(content: str) -> tuple[list[tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
    try:
        records = list(csv.reader(io.StringIO(content)))
    except csv.Error as exc:
        raise ValidationError("CSV 内容无法解析", context={"detail": str(exc)}) from exc
    records = [record for record in records if any(cell.strip() for cell in record)]
    if not records:
        raise ValidationError("CSV 内容缺少表头")
    header = [cell.strip() for cell in records[0]]
    missing = [column for column in REQUIRED_COLUMNS if column not in header]
    if missing:
        raise ValidationError("CSV 表头缺少必填列", context={"missing": missing})
    rows: list[tuple[int, dict[str, Any]]] = []
    errors: list[dict[str, Any]] = []
    for row_number, record in enumerate(records[1:], start=1):
        if len(record) != len(header):
            errors.append(
                {
                    "row_number": row_number,
                    "code": "row_shape",
                    "field": "",
                    "message": f"行列数不正确：期望 {len(header)} 列，实际 {len(record)} 列",
                }
            )
            continue
        rows.append((row_number, dict(zip(header, record))))
    return rows, errors


def _parse_json(content: str) -> tuple[list[tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
    try:
        document = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValidationError("JSON 内容无法解析", context={"detail": str(exc)}) from exc
    if isinstance(document, dict):
        items = document.get("readings")
    else:
        items = document
    if not isinstance(items, list):
        raise ValidationError("JSON 批次必须是读数数组，或包含 readings 数组的对象")
    rows: list[tuple[int, dict[str, Any]]] = []
    errors: list[dict[str, Any]] = []
    for row_number, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            errors.append(
                {
                    "row_number": row_number,
                    "code": "invalid_row",
                    "field": "",
                    "message": "读数行必须是 JSON 对象",
                }
            )
            continue
        rows.append((row_number, item))
    return rows, errors


def normalize_row(raw: dict[str, Any], row_number: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """校验并规范化一行读数；失败时返回 (None, 错误条目列表)。"""

    errors: list[dict[str, Any]] = []

    def err(code: str, field: str, message: str) -> None:
        errors.append({"row_number": row_number, "code": code, "field": field, "message": message})

    for field in REQUIRED_COLUMNS:
        value = raw.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            err("missing_field", field, f"缺少必填字段 {field}")
    if errors:
        return None, errors

    normalized: dict[str, Any] = {}
    for field in ("payload", "cycle_phase", "model_version", "sensor_id"):
        value = raw.get(field)
        if value is None:
            text = ""
        elif isinstance(value, str):
            text = value.strip()
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            text = str(value)
        else:
            err("invalid_type", field, f"字段 {field} 必须是字符串")
            continue
        limit = TEXT_LIMITS[field]
        if field != "sensor_id" and not text:
            err("missing_field", field, f"缺少必填字段 {field}")
        elif len(text) > limit:
            err("invalid_value", field, f"字段 {field} 长度不能超过 {limit} 字符")
        else:
            normalized[field] = text

    recorded_at = _parse_timestamp(raw.get("recorded_at"))
    if recorded_at is None:
        err("invalid_timestamp", "recorded_at", "recorded_at 必须是合法的 ISO 8601 时间")
    else:
        normalized["recorded_at"] = recorded_at

    temperature = _parse_number(raw.get("temperature"))
    if temperature is None:
        err("invalid_number", "temperature", "temperature 必须是有限数值")
    temperature_unit = _parse_unit(raw.get("temperature_unit"), TEMPERATURE_UNITS)
    if temperature_unit is None:
        err("invalid_unit", "temperature_unit", f"不支持的温度单位 {raw.get('temperature_unit')!r}")
    if temperature is not None and temperature_unit is not None:
        scale, offset = temperature_unit
        temperature_c = round(temperature * scale + offset, 6)
        if not MIN_TEMPERATURE_C <= temperature_c <= MAX_TEMPERATURE_C:
            err("out_of_bounds", "temperature", f"换算后温度 {temperature_c}°C 超出物理合理范围")
        else:
            normalized["temperature_c"] = temperature_c

    runtime = _parse_number(raw.get("runtime"))
    if runtime is None:
        err("invalid_number", "runtime", "runtime 必须是有限数值")
    runtime_unit = _parse_unit(raw.get("runtime_unit"), RUNTIME_UNITS)
    if runtime_unit is None:
        err("invalid_unit", "runtime_unit", f"不支持的运行时长单位 {raw.get('runtime_unit')!r}")
    if runtime is not None and runtime_unit is not None:
        runtime_seconds = round(runtime * runtime_unit, 6)
        if runtime_seconds < 0:
            err("out_of_bounds", "runtime", "运行时长不能为负")
        elif runtime_seconds > MAX_RUNTIME_SECONDS:
            err("out_of_bounds", "runtime", f"运行时长不能超过 {MAX_RUNTIME_SECONDS} 秒")
        else:
            normalized["runtime_seconds"] = runtime_seconds

    flag = _parse_flag(raw.get("anomaly_flag"))
    if flag is None:
        err("invalid_value", "anomaly_flag", "anomaly_flag 只能是 0/1 或 true/false")
    else:
        normalized["anomaly_flag"] = flag

    revision = _parse_revision(raw.get("revision"))
    if revision is None:
        err("invalid_revision", "revision", "revision 必须是 1 到 1000000 之间的整数")
    else:
        normalized["revision"] = revision

    if errors:
        return None, errors
    normalized["raw_json"] = json.dumps(raw, ensure_ascii=False, sort_keys=True)
    return normalized, []


def _parse_timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return to_storage(parsed)


def _parse_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str) and value.strip():
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def _parse_unit(value: Any, table: dict[str, Any]) -> Any | None:
    if not isinstance(value, str):
        return None
    return table.get(value.strip().lower())


def _parse_flag(value: Any) -> int | None:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int) and value in (0, 1):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in FLAG_TRUE:
            return 1
        if lowered in FLAG_FALSE:
            return 0
    return None


def _parse_revision(value: Any) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return 1
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        revision = value
    elif isinstance(value, float) and value.is_integer():
        revision = int(value)
    elif isinstance(value, str) and value.strip().isdigit():
        revision = int(value.strip())
    else:
        return None
    return revision if 1 <= revision <= MAX_REVISION else None
