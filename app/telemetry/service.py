from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.core.clock import to_storage, utc_now
from app.core.errors import ConflictError, NotFoundError
from app.database import get_connection, transaction
from app.telemetry.schemas import RuleConfig

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_rule_sets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    version TEXT NOT NULL,
    config_json TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(code, version)
);
CREATE TABLE IF NOT EXISTS telemetry_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    format TEXT NOT NULL CHECK(format IN ('csv','json')),
    content_hash TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    total_rows INTEGER NOT NULL DEFAULT 0,
    accepted_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    revised_rows INTEGER NOT NULL DEFAULT 0,
    affected_groups_json TEXT NOT NULL DEFAULT '[]',
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_key TEXT NOT NULL,
    payload_id TEXT NOT NULL,
    cycle_phase TEXT NOT NULL,
    model_version TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    temperature_c REAL NOT NULL,
    run_seconds REAL NOT NULL,
    anomaly_count INTEGER NOT NULL DEFAULT 0,
    flags_json TEXT NOT NULL DEFAULT '[]',
    batch_id INTEGER NOT NULL REFERENCES telemetry_batches(id),
    revision INTEGER NOT NULL DEFAULT 1,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    superseded_by INTEGER,
    content_hash TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    UNIQUE(reading_key, revision)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_readings_group ON telemetry_readings(payload_id, cycle_phase, model_version, is_current);
CREATE INDEX IF NOT EXISTS idx_telemetry_readings_key ON telemetry_readings(reading_key, is_current);
CREATE TABLE IF NOT EXISTS telemetry_row_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES telemetry_batches(id) ON DELETE CASCADE,
    row_index INTEGER NOT NULL,
    reading_key TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL,
    message TEXT NOT NULL,
    raw_excerpt TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_telemetry_row_errors_batch ON telemetry_row_errors(batch_id, row_index, error_code);
CREATE TABLE IF NOT EXISTS telemetry_recomputes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_code TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    trigger_source TEXT NOT NULL DEFAULT 'manual',
    requested_by TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'all' CHECK(scope IN ('all','groups')),
    affected_groups_json TEXT NOT NULL DEFAULT '[]',
    reading_count INTEGER NOT NULL DEFAULT 0,
    input_digest TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry_aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recompute_id INTEGER NOT NULL REFERENCES telemetry_recomputes(id),
    rule_code TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    payload_id TEXT NOT NULL,
    cycle_phase TEXT NOT NULL,
    model_version TEXT NOT NULL,
    reading_count INTEGER NOT NULL,
    peak_temperature_c REAL,
    effective_run_seconds REAL NOT NULL,
    anomaly_total INTEGER NOT NULL,
    flagged_reading_ids_json TEXT NOT NULL DEFAULT '[]',
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    computed_at TEXT NOT NULL,
    UNIQUE(recompute_id, payload_id, cycle_phase, model_version)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_aggregates_group ON telemetry_aggregates(rule_code, rule_version, payload_id, cycle_phase, model_version, is_current);
"""

DEFAULT_RULE_CODE = "thermal-lab"
DEFAULT_RULE_VERSION = "v1"
DEFAULT_RULE_CONFIG: dict[str, Any] = {
    "valid_phases": ["ramp_up", "soak", "ramp_down", "cooldown"],
    "temperature_units": ["C", "F", "K"],
    "duration_units": ["s", "min", "h"],
    "min_temperature_c": -80.0,
    "max_temperature_c": 400.0,
    "max_run_seconds": 172800.0,
    "anomaly_temperature_c": 180.0,
    "max_anomaly_count": 1000,
    "effective_exclude_flags": ["TEMP_ABOVE_THRESHOLD"],
}

_TEXT_FIELDS = (("reading_key", 120), ("payload_id", 80), ("cycle_phase", 60), ("model_version", 60))

_TEMP_ALIASES = {
    "C": "C", "°C": "C", "CELSIUS": "C",
    "F": "F", "°F": "F", "FAHRENHEIT": "F",
    "K": "K", "KELVIN": "K",
}
_TEMP_TO_C = {
    "C": lambda value: value,
    "F": lambda value: (value - 32.0) * 5.0 / 9.0,
    "K": lambda value: value - 273.15,
}
_DURATION_ALIASES = {
    "S": "S", "SEC": "S", "SECS": "S", "SECOND": "S", "SECONDS": "S",
    "MIN": "MIN", "MINS": "MIN", "MINUTE": "MIN", "MINUTES": "MIN",
    "H": "H", "HR": "H", "HRS": "H", "HOUR": "H", "HOURS": "H",
}
_DURATION_TO_SECONDS = {"S": 1.0, "MIN": 60.0, "H": 3600.0}

ERROR_SORT = "row_index, error_code, reading_key"


def _now() -> str:
    return to_storage(utc_now())


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _excerpt(raw: Any) -> str:
    try:
        text = json.dumps(raw, ensure_ascii=False, default=str)
    except TypeError:
        text = str(raw)
    return text[:300]


def _as_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _as_number(value: Any) -> float | None:
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


def _as_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        return int(value.strip())
    return None


@dataclass(frozen=True)
class RowError:
    row_index: int
    reading_key: str
    error_code: str
    message: str
    raw_excerpt: str


def ensure_schema() -> None:
    connection = get_connection()
    connection.executescript(SCHEMA)
    connection.execute(
        "INSERT OR IGNORE INTO telemetry_rule_sets(code,version,config_json,is_active,created_by,created_at) VALUES(?,?,?,1,'system',?)",
        (DEFAULT_RULE_CODE, DEFAULT_RULE_VERSION, json.dumps(DEFAULT_RULE_CONFIG, ensure_ascii=False, sort_keys=True), _now()),
    )


def _parse_csv(content: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in csv.DictReader(io.StringIO(content)):
        row = {key: value for key, value in record.items() if key is not None}
        if None in record.keys() or None in record.values():
            row["__malformed__"] = True
        rows.append(row)
    return rows


def _validate_row(raw: Any, row_index: int, rule: RuleConfig) -> tuple[dict[str, Any] | None, list[RowError]]:
    """校验并归一化单行；返回 (归一化读数, 错误列表)，坏行不抛异常。"""
    excerpt = _excerpt(raw)
    if not isinstance(raw, dict) or raw.get("__malformed__"):
        return None, [RowError(row_index, "", "ROW_NOT_OBJECT", "行必须是完整记录且列数与表头一致", excerpt)]

    errors: list[tuple[str, str]] = []

    def err(code: str, message: str) -> None:
        errors.append((code, message))

    texts: dict[str, str | None] = {}
    for field_name, max_length in _TEXT_FIELDS:
        value = _as_text(raw.get(field_name))
        if not value:
            err("MISSING_FIELD", f"缺少必填字段 {field_name}")
        elif len(value) > max_length:
            err("OUT_OF_RANGE", f"字段 {field_name} 长度超过 {max_length}")
        else:
            texts[field_name] = value
    reading_key = texts.get("reading_key") or ""

    phase = (texts.get("cycle_phase") or "").lower()
    if phase and phase not in {item.lower() for item in rule.valid_phases}:
        err("UNKNOWN_PHASE", f"未知循环阶段 {texts.get('cycle_phase')}，允许值: {sorted(rule.valid_phases)}")

    recorded_at: str | None = None
    recorded_raw = _as_text(raw.get("recorded_at"))
    if not recorded_raw:
        err("MISSING_FIELD", "缺少必填字段 recorded_at")
    else:
        try:
            parsed = datetime.fromisoformat(recorded_raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            recorded_at = to_storage(parsed)
        except ValueError:
            err("INVALID_TIMESTAMP", f"recorded_at 不是合法 ISO 时间: {recorded_raw}")

    temperature = _as_number(raw.get("temperature"))
    if raw.get("temperature") is None or (isinstance(raw.get("temperature"), str) and not raw.get("temperature").strip()):
        err("MISSING_FIELD", "缺少必填字段 temperature")
    elif temperature is None:
        err("INVALID_TYPE", "temperature 必须是有限数值")

    temperature_c: float | None = None
    unit_raw = _as_text(raw.get("temperature_unit"))
    if not unit_raw:
        err("MISSING_FIELD", "缺少必填字段 temperature_unit")
    else:
        unit = _TEMP_ALIASES.get(unit_raw.upper())
        allowed = {item.strip().upper() for item in rule.temperature_units}
        if unit is None or unit not in allowed:
            err("UNKNOWN_UNIT", f"未知温度单位 {unit_raw}，允许值: {sorted(allowed)}")
        elif temperature is not None:
            temperature_c = round(_TEMP_TO_C[unit](temperature), 3)
            if not (rule.min_temperature_c <= temperature_c <= rule.max_temperature_c):
                err("OUT_OF_RANGE", f"温度 {temperature_c}°C 超出物理量程 [{rule.min_temperature_c}, {rule.max_temperature_c}]")

    duration = _as_number(raw.get("run_duration"))
    if raw.get("run_duration") is None or (isinstance(raw.get("run_duration"), str) and not raw.get("run_duration").strip()):
        err("MISSING_FIELD", "缺少必填字段 run_duration")
    elif duration is None:
        err("INVALID_TYPE", "run_duration 必须是有限数值")

    run_seconds: float | None = None
    duration_unit_raw = _as_text(raw.get("duration_unit"))
    if not duration_unit_raw:
        err("MISSING_FIELD", "缺少必填字段 duration_unit")
    else:
        duration_unit = _DURATION_ALIASES.get(duration_unit_raw.upper())
        allowed = {item.strip().upper() for item in rule.duration_units}
        if duration_unit is None or duration_unit not in allowed:
            err("UNKNOWN_UNIT", f"未知时长单位 {duration_unit_raw}，允许值: {sorted(allowed)}")
        elif duration is not None:
            run_seconds = round(duration * _DURATION_TO_SECONDS[duration_unit], 3)
            if run_seconds < 0:
                err("OUT_OF_RANGE", "运行时长不能为负")
            elif run_seconds > rule.max_run_seconds:
                err("OUT_OF_RANGE", f"运行时长 {run_seconds}s 超过上限 {rule.max_run_seconds}s")

    anomaly_count = 0
    count_raw = raw.get("anomaly_count")
    if count_raw is not None and not (isinstance(count_raw, str) and not count_raw.strip()):
        parsed_count = _as_count(count_raw)
        if parsed_count is None:
            err("INVALID_TYPE", "anomaly_count 必须是整数")
        elif parsed_count < 0 or parsed_count > rule.max_anomaly_count:
            err("OUT_OF_RANGE", f"anomaly_count 超出范围 [0, {rule.max_anomaly_count}]")
        else:
            anomaly_count = parsed_count

    if errors:
        return None, [RowError(row_index, reading_key, code, message, excerpt) for code, message in errors]
    normalized = {
        "reading_key": texts["reading_key"],
        "payload_id": texts["payload_id"],
        "cycle_phase": phase,
        "model_version": texts["model_version"],
        "recorded_at": recorded_at,
        "temperature_c": temperature_c,
        "run_seconds": run_seconds,
        "anomaly_count": anomaly_count,
    }
    return normalized, []


def _derive_flags(reading: dict[str, Any], rule: RuleConfig) -> list[str]:
    flags: list[str] = []
    if reading["temperature_c"] > rule.anomaly_temperature_c:
        flags.append("TEMP_ABOVE_THRESHOLD")
    return flags


def _group_key(reading: dict[str, Any]) -> tuple[str, str, str]:
    return (reading["payload_id"], reading["cycle_phase"], reading["model_version"])


def _group_view(group: tuple[str, str, str]) -> dict[str, str]:
    return {"payload_id": group[0], "cycle_phase": group[1], "model_version": group[2]}


class TelemetryService:
    """热循环与辐射测试读数的导入、校验、去重、修订、重算与解释服务。"""

    def __init__(self, connection: sqlite3.Connection | None = None):
        self.connection = connection or get_connection()
        ensure_schema()

    # ---------- 规则版本 ----------

    def _load_rule(self, rule_code: str | None, rule_version: str | None) -> tuple[sqlite3.Row, RuleConfig]:
        code = rule_code or DEFAULT_RULE_CODE
        if rule_version:
            row = self.connection.execute(
                "SELECT * FROM telemetry_rule_sets WHERE code=? AND version=?", (code, rule_version)
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT * FROM telemetry_rule_sets WHERE code=? ORDER BY id DESC LIMIT 1", (code,)
            ).fetchone()
        if row is None:
            raise NotFoundError(f"规则版本不存在: {code}@{rule_version or 'latest'}")
        return row, RuleConfig(**json.loads(row["config_json"]))

    def list_rules(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM telemetry_rule_sets ORDER BY code, id").fetchall()
        return [{**dict(row), "config": json.loads(row["config_json"])} for row in rows]

    def create_rule_set(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = RuleConfig(**payload["config"])
        now = _now()
        with transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT id FROM telemetry_rule_sets WHERE code=? AND version=?", (payload["code"], payload["version"])
            ).fetchone()
            if existing is not None:
                raise ConflictError("规则版本已存在", context={"code": payload["code"], "version": payload["version"]})
            cursor = connection.execute(
                "INSERT INTO telemetry_rule_sets(code,version,config_json,is_active,created_by,created_at) VALUES(?,?,?,1,?,?)",
                (payload["code"], payload["version"], json.dumps(config.model_dump(), ensure_ascii=False, sort_keys=True), payload["created_by"], now),
            )
            row = connection.execute("SELECT * FROM telemetry_rule_sets WHERE id=?", (cursor.lastrowid,)).fetchone()
            return {**dict(row), "config": json.loads(row["config_json"])}

    # ---------- 批次导入 ----------

    def import_batch(self, payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """导入一个批次，返回 (摘要, HTTP 状态码)。重复批次返回已存摘要。"""
        fmt = payload["format"]
        raw_rows: list[Any] = list(payload["rows"]) if fmt == "json" else _parse_csv(payload["content"])
        content_hash = _hash(sorted(_canonical(row) for row in raw_rows))
        rule_row, rule = self._load_rule(payload.get("rule_code"), payload.get("rule_version"))
        now = _now()
        with transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM telemetry_batches WHERE batch_key=?", (payload["batch_key"],)
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] == content_hash:
                    return json.loads(existing["summary_json"]), 200
                raise ConflictError("批次键已被不同内容的批次使用", context={"batch_key": payload["batch_key"]})
            cursor = connection.execute(
                "INSERT INTO telemetry_batches(batch_key,source,format,content_hash,rule_code,rule_version,total_rows,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (payload["batch_key"], payload["source"], fmt, content_hash, rule_row["code"], rule_row["version"], len(raw_rows), now),
            )
            batch_id = cursor.lastrowid
            counts = {"accepted": 0, "rejected": 0, "duplicate": 0, "revised": 0}
            affected: set[tuple[str, str, str]] = set()
            for row_index, raw in enumerate(raw_rows):
                outcome = self._ingest_row(connection, batch_id, row_index, raw, rule, affected, now)
                counts[outcome] += 1
            errors = [
                dict(row)
                for row in connection.execute(
                    f"SELECT row_index,reading_key,error_code,message FROM telemetry_row_errors WHERE batch_id=? ORDER BY {ERROR_SORT}",
                    (batch_id,),
                ).fetchall()
            ]
            groups = [_group_view(group) for group in sorted(affected)]
            summary = {
                "batch_id": batch_id,
                "batch_key": payload["batch_key"],
                "source": payload["source"],
                "format": fmt,
                "status": "imported",
                "rule_code": rule_row["code"],
                "rule_version": rule_row["version"],
                "total_rows": len(raw_rows),
                "accepted_rows": counts["accepted"],
                "rejected_rows": counts["rejected"],
                "duplicate_rows": counts["duplicate"],
                "revised_rows": counts["revised"],
                "affected_groups": groups,
                "errors": errors,
                "created_at": now,
            }
            connection.execute(
                "UPDATE telemetry_batches SET accepted_rows=?,rejected_rows=?,duplicate_rows=?,revised_rows=?,affected_groups_json=?,summary_json=? WHERE id=?",
                (
                    counts["accepted"], counts["rejected"], counts["duplicate"], counts["revised"],
                    json.dumps(groups, ensure_ascii=False), json.dumps(summary, ensure_ascii=False), batch_id,
                ),
            )
            return summary, 201

    def _ingest_row(
        self,
        connection: sqlite3.Connection,
        batch_id: int,
        row_index: int,
        raw: Any,
        rule: RuleConfig,
        affected: set[tuple[str, str, str]],
        now: str,
    ) -> str:
        normalized, errors = _validate_row(raw, row_index, rule)
        if errors:
            for error in errors:
                connection.execute(
                    "INSERT INTO telemetry_row_errors(batch_id,row_index,reading_key,error_code,message,raw_excerpt,created_at) VALUES(?,?,?,?,?,?,?)",
                    (batch_id, error.row_index, error.reading_key, error.error_code, error.message, error.raw_excerpt, now),
                )
            return "rejected"
        assert normalized is not None
        row_hash = _hash(normalized)
        same = connection.execute(
            "SELECT id FROM telemetry_readings WHERE reading_key=? AND content_hash=?",
            (normalized["reading_key"], row_hash),
        ).fetchone()
        if same is not None:
            return "duplicate"
        flags = _derive_flags(normalized, rule)
        current = connection.execute(
            "SELECT * FROM telemetry_readings WHERE reading_key=? AND is_current=1", (normalized["reading_key"],)
        ).fetchone()
        new_id = self._insert_reading(
            connection, batch_id, normalized, flags, revision=(current["revision"] + 1) if current else 1, now=now
        )
        if current is None:
            affected.add(_group_key(normalized))
            return "accepted"
        connection.execute("UPDATE telemetry_readings SET is_current=0, superseded_by=? WHERE id=?", (new_id, current["id"]))
        affected.add(_group_key(normalized))
        affected.add((current["payload_id"], current["cycle_phase"], current["model_version"]))
        return "revised"

    @staticmethod
    def _insert_reading(
        connection: sqlite3.Connection,
        batch_id: int,
        reading: dict[str, Any],
        flags: list[str],
        revision: int,
        now: str,
    ) -> int:
        cursor = connection.execute(
            "INSERT INTO telemetry_readings(reading_key,payload_id,cycle_phase,model_version,recorded_at,temperature_c,run_seconds,anomaly_count,flags_json,batch_id,revision,is_current,content_hash,ingested_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
            (
                reading["reading_key"], reading["payload_id"], reading["cycle_phase"], reading["model_version"],
                reading["recorded_at"], reading["temperature_c"], reading["run_seconds"], reading["anomaly_count"],
                json.dumps(flags, ensure_ascii=False), batch_id, revision, _hash(reading), now,
            ),
        )
        return int(cursor.lastrowid)

    def get_batch(self, batch_key: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT summary_json FROM telemetry_batches WHERE batch_key=?", (batch_key,)).fetchone()
        if row is None:
            raise NotFoundError(f"批次不存在: {batch_key}")
        return json.loads(row["summary_json"])

    # ---------- 原始读数 ----------

    def list_readings(
        self,
        payload_id: str | None = None,
        cycle_phase: str | None = None,
        model_version: str | None = None,
        include_superseded: bool = False,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if not include_superseded:
            clauses.append("is_current=1")
        for column, value in (("payload_id", payload_id), ("cycle_phase", cycle_phase), ("model_version", model_version)):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.connection.execute(
            f"SELECT * FROM telemetry_readings {where} ORDER BY id LIMIT ?", (*params, limit)
        ).fetchall()
        return [self._reading_view(row) for row in rows]

    def reading_history(self, reading_key: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM telemetry_readings WHERE reading_key=? ORDER BY revision", (reading_key,)
        ).fetchall()
        if not rows:
            raise NotFoundError(f"读数不存在: {reading_key}")
        return [self._reading_view(row) for row in rows]

    @staticmethod
    def _reading_view(row: sqlite3.Row) -> dict[str, Any]:
        return {**dict(row), "flags": json.loads(row["flags_json"])}

    # ---------- 按版本重算聚合 ----------

    def recompute(self, payload: dict[str, Any]) -> dict[str, Any]:
        rule_row, rule = self._load_rule(payload.get("rule_code"), payload.get("rule_version"))
        groups_filter = payload.get("groups")
        requested_by = payload.get("requested_by") or "system"
        started = _now()
        with transaction(immediate=True) as connection:
            if groups_filter:
                targets = sorted({(item["payload_id"], item["cycle_phase"], item["model_version"]) for item in groups_filter})
                scope = "groups"
            else:
                targets = [
                    (row["payload_id"], row["cycle_phase"], row["model_version"])
                    for row in connection.execute(
                        "SELECT DISTINCT payload_id, cycle_phase, model_version FROM telemetry_readings WHERE is_current=1"
                        " ORDER BY payload_id, cycle_phase, model_version"
                    ).fetchall()
                ]
                scope = "all"
            cursor = connection.execute(
                "INSERT INTO telemetry_recomputes(rule_code,rule_version,trigger_source,requested_by,scope,started_at,finished_at) VALUES(?,?,?,?,?,?,?)",
                (rule_row["code"], rule_row["version"], payload.get("trigger_source") or "manual", requested_by, scope, started, started),
            )
            recompute_id = int(cursor.lastrowid)
            digest_parts: list[tuple[Any, ...]] = []
            reading_total = 0
            excluded = set(rule.effective_exclude_flags)
            for group in targets:
                rows = connection.execute(
                    "SELECT * FROM telemetry_readings WHERE payload_id=? AND cycle_phase=? AND model_version=? AND is_current=1 ORDER BY id",
                    group,
                ).fetchall()
                digest_parts.extend((row["id"], row["revision"], row["content_hash"]) for row in rows)
                reading_total += len(rows)
                connection.execute(
                    "UPDATE telemetry_aggregates SET is_current=0"
                    " WHERE rule_code=? AND rule_version=? AND payload_id=? AND cycle_phase=? AND model_version=? AND is_current=1",
                    (rule_row["code"], rule_row["version"], *group),
                )
                if not rows:
                    continue
                # 标记按本次重算的规则版本重新推导，保证“按版本重算”语义
                flag_sets = [_derive_flags(row, rule) for row in rows]
                effective = round(
                    sum(row["run_seconds"] for row, flags in zip(rows, flag_sets) if not excluded.intersection(flags)), 3
                )
                flagged_ids = sorted(row["id"] for row, flags in zip(rows, flag_sets) if flags)
                anomaly_total = sum(row["anomaly_count"] for row in rows) + len(flagged_ids)
                peak = round(max(row["temperature_c"] for row in rows), 3)
                connection.execute(
                    "INSERT INTO telemetry_aggregates(recompute_id,rule_code,rule_version,payload_id,cycle_phase,model_version,"
                    "reading_count,peak_temperature_c,effective_run_seconds,anomaly_total,flagged_reading_ids_json,is_current,computed_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?)",
                    (
                        recompute_id, rule_row["code"], rule_row["version"], *group, len(rows), peak, effective,
                        anomaly_total, json.dumps(flagged_ids), _now(),
                    ),
                )
            finished = _now()
            connection.execute(
                "UPDATE telemetry_recomputes SET affected_groups_json=?, reading_count=?, input_digest=?, finished_at=? WHERE id=?",
                (
                    json.dumps([_group_view(group) for group in targets], ensure_ascii=False),
                    reading_total, _hash(sorted(digest_parts)), finished, recompute_id,
                ),
            )
        return self.get_recompute(recompute_id)

    def get_recompute(self, recompute_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM telemetry_recomputes WHERE id=?", (recompute_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"重算记录不存在: {recompute_id}")
        return {**dict(row), "affected_groups": json.loads(row["affected_groups_json"])}

    def list_recomputes(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM telemetry_recomputes ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "affected_groups": json.loads(row["affected_groups_json"])} for row in rows]

    # ---------- 聚合查询 ----------

    def list_aggregates(
        self,
        payload_id: str | None = None,
        cycle_phase: str | None = None,
        model_version: str | None = None,
        rule_code: str | None = None,
        rule_version: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["is_current=1"]
        params: list[Any] = []
        for column, value in (
            ("payload_id", payload_id), ("cycle_phase", cycle_phase), ("model_version", model_version),
            ("rule_code", rule_code), ("rule_version", rule_version),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        rows = self.connection.execute(
            f"SELECT * FROM telemetry_aggregates WHERE {' AND '.join(clauses)}"
            " ORDER BY payload_id, cycle_phase, model_version, rule_code, rule_version",
            params,
        ).fetchall()
        return [self._aggregate_view(row) for row in rows]

    def aggregate_history(
        self,
        payload_id: str,
        cycle_phase: str,
        model_version: str,
        rule_code: str | None = None,
        rule_version: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["payload_id=?", "cycle_phase=?", "model_version=?"]
        params: list[Any] = [payload_id, cycle_phase, model_version]
        for column, value in (("rule_code", rule_code), ("rule_version", rule_version)):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        rows = self.connection.execute(
            f"SELECT * FROM telemetry_aggregates WHERE {' AND '.join(clauses)} ORDER BY id", params
        ).fetchall()
        if not rows:
            raise NotFoundError("该分组没有聚合历史")
        return [self._aggregate_view(row) for row in rows]

    @staticmethod
    def _aggregate_view(row: sqlite3.Row) -> dict[str, Any]:
        return {**dict(row), "flagged_reading_ids": json.loads(row["flagged_reading_ids_json"])}

    # ---------- 异常解释 ----------

    def explain_anomalies(
        self,
        payload_id: str,
        cycle_phase: str,
        model_version: str,
        rule_code: str | None = None,
        rule_version: str | None = None,
    ) -> dict[str, Any]:
        clauses = ["is_current=1", "payload_id=?", "cycle_phase=?", "model_version=?"]
        params: list[Any] = [payload_id, cycle_phase, model_version]
        for column, value in (("rule_code", rule_code), ("rule_version", rule_version)):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        aggregate = self.connection.execute(
            f"SELECT * FROM telemetry_aggregates WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT 1", params
        ).fetchone()
        if aggregate is None:
            raise NotFoundError("该分组当前没有聚合结果，请先执行重算")
        _, rule = self._load_rule(aggregate["rule_code"], aggregate["rule_version"])
        readings = self.connection.execute(
            "SELECT * FROM telemetry_readings WHERE payload_id=? AND cycle_phase=? AND model_version=? AND is_current=1 ORDER BY id",
            (payload_id, cycle_phase, model_version),
        ).fetchall()
        excluded = set(rule.effective_exclude_flags)
        contributions: list[dict[str, Any]] = []
        for row in readings:
            flags = _derive_flags(row, rule)
            reasons: list[str] = []
            if "TEMP_ABOVE_THRESHOLD" in flags:
                reasons.append(f"峰值温度 {row['temperature_c']}°C 超过规则阈值 {rule.anomaly_temperature_c}°C")
            if row["anomaly_count"] > 0:
                reasons.append(f"仪器上报异常 {row['anomaly_count']} 次")
            if not reasons:
                continue
            contributions.append(
                {
                    "reading_key": row["reading_key"],
                    "revision": row["revision"],
                    "recorded_at": row["recorded_at"],
                    "temperature_c": row["temperature_c"],
                    "run_seconds": row["run_seconds"],
                    "anomaly_count": row["anomaly_count"],
                    "flags": flags,
                    "reasons": reasons,
                    "counts_toward_effective_duration": not excluded.intersection(flags),
                }
            )
        contributions.sort(key=lambda item: (item["reading_key"], item["revision"]))
        return {
            "group": {"payload_id": payload_id, "cycle_phase": cycle_phase, "model_version": model_version},
            "rule_code": aggregate["rule_code"],
            "rule_version": aggregate["rule_version"],
            "recompute_id": aggregate["recompute_id"],
            "anomaly_total": aggregate["anomaly_total"],
            "effective_run_seconds": aggregate["effective_run_seconds"],
            "contributions": contributions,
        }
