"""实验室热循环与辐射测试读数数据管线。

数据流：批次导入（CSV/JSON，逐行校验与单位换算）→ 读数去重与迟到修订 →
按规则版本重算聚合 → 异常解释查询。原始读数只插入不更新，聚合结果写在
独立的运行表中，重算永远不会覆盖原始读数。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.lab.parsing import normalize_row, parse_batch_content
from app.lab.rules import DEFAULT_RULE_VERSION, RULE_SETS, aggregate_group

SCHEMA = """
CREATE TABLE IF NOT EXISTS lab_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    format TEXT NOT NULL CHECK(format IN ('csv','json')),
    content_digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('accepted','accepted_with_errors','rejected')),
    total_rows INTEGER NOT NULL DEFAULT 0,
    accepted_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    superseded_rows INTEGER NOT NULL DEFAULT 0,
    error_report_json TEXT NOT NULL DEFAULT '[]',
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(source, batch_key)
);
CREATE TABLE IF NOT EXISTS lab_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES lab_batches(id) ON DELETE RESTRICT,
    row_number INTEGER NOT NULL,
    reading_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    cycle_phase TEXT NOT NULL,
    model_version TEXT NOT NULL,
    sensor_id TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL,
    temperature_c REAL NOT NULL,
    runtime_seconds REAL NOT NULL,
    anomaly_flag INTEGER NOT NULL DEFAULT 0 CHECK(anomaly_flag IN (0,1)),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(reading_key, revision)
);
CREATE INDEX IF NOT EXISTS idx_lab_readings_key ON lab_readings(reading_key, revision);
CREATE INDEX IF NOT EXISTS idx_lab_readings_group ON lab_readings(payload, cycle_phase, model_version);
CREATE TABLE IF NOT EXISTS lab_aggregation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version TEXT NOT NULL,
    origin TEXT NOT NULL CHECK(origin IN ('import','manual')),
    actor TEXT NOT NULL DEFAULT 'system',
    batch_id INTEGER REFERENCES lab_batches(id) ON DELETE SET NULL,
    group_count INTEGER NOT NULL,
    readings_count INTEGER NOT NULL,
    affected_groups_json TEXT NOT NULL DEFAULT '[]',
    changes_json TEXT NOT NULL DEFAULT '[]',
    input_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lab_runs_digest ON lab_aggregation_runs(rule_version, input_digest);
CREATE TABLE IF NOT EXISTS lab_aggregates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES lab_aggregation_runs(id) ON DELETE CASCADE,
    rule_version TEXT NOT NULL,
    payload TEXT NOT NULL,
    cycle_phase TEXT NOT NULL,
    model_version TEXT NOT NULL,
    reading_count INTEGER NOT NULL,
    peak_temperature_c REAL,
    effective_runtime_seconds REAL NOT NULL,
    anomaly_count INTEGER NOT NULL,
    first_recorded_at TEXT,
    last_recorded_at TEXT,
    UNIQUE(run_id, payload, cycle_phase, model_version)
);
CREATE INDEX IF NOT EXISTS idx_lab_aggregates_run ON lab_aggregates(run_id, payload, cycle_phase, model_version);
CREATE TABLE IF NOT EXISTS lab_anomaly_explanations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES lab_aggregation_runs(id) ON DELETE CASCADE,
    reading_id INTEGER NOT NULL REFERENCES lab_readings(id) ON DELETE RESTRICT,
    payload TEXT NOT NULL,
    cycle_phase TEXT NOT NULL,
    model_version TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    detail TEXT NOT NULL,
    observed_value REAL,
    threshold_value REAL,
    UNIQUE(run_id, reading_id, rule_code)
);
CREATE INDEX IF NOT EXISTS idx_lab_explanations_run ON lab_anomaly_explanations(run_id, payload, cycle_phase, model_version);
CREATE TABLE IF NOT EXISTS lab_aggregation_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    current_run_id INTEGER REFERENCES lab_aggregation_runs(id) ON DELETE SET NULL
);
"""

CURRENT_READINGS_SQL = """
SELECT r.* FROM lab_readings r
JOIN (
    SELECT reading_key, MAX(revision) AS max_revision FROM lab_readings GROUP BY reading_key
) latest ON latest.reading_key = r.reading_key AND latest.max_revision = r.revision
"""

METRIC_FIELDS = (
    "reading_count",
    "peak_temperature_c",
    "effective_runtime_seconds",
    "anomaly_count",
    "first_recorded_at",
    "last_recorded_at",
)


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def ensure_schema() -> None:
    get_connection().executescript(SCHEMA)


def _group_key(payload: str, cycle_phase: str, model_version: str) -> dict[str, str]:
    return {"payload": payload, "cycle_phase": cycle_phase, "model_version": model_version}


def _sort_key(group: dict[str, str]) -> tuple[str, str, str]:
    return (group["payload"], group["cycle_phase"], group["model_version"])


class LabPipelineService:
    """批次导入、去重、迟到修订与按版本重算聚合的事务服务。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        ensure_schema()

    # ------------------------------------------------------------------ 导入

    def import_batch(self, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """导入一个批次，返回 (摘要, 是否新建)。

        相同 (source, batch_key) 且内容一致的重复批次直接返回首次导入保存的
        摘要；内容不一致则报 409。坏行只记入错误报告，不阻塞好行。
        """

        source = payload["source"].strip()
        batch_key = payload["batch_key"].strip()
        fmt = payload["format"]
        content = payload["content"]
        rows, shape_errors = parse_batch_content(fmt, content)
        if not rows and not shape_errors:
            raise ValidationError("批次不包含任何数据行")
        content_digest = digest({"source": source, "batch_key": batch_key, "format": fmt, "content": content})
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM lab_batches WHERE source=? AND batch_key=?", (source, batch_key)
            ).fetchone()
            if existing is not None:
                if existing["content_digest"] != content_digest:
                    raise ConflictError("同一来源批次键对应了不同的批次内容")
                return json.loads(existing["summary_json"]), False

            batch_id = self._insert_batch(connection, source, batch_key, fmt, content_digest, now)
            errors: list[dict[str, Any]] = list(shape_errors)
            accepted = duplicates = superseded = 0
            rejected = len(shape_errors)
            for row_number, raw in rows:
                normalized, row_errors = normalize_row(raw, row_number)
                if row_errors:
                    errors.extend(row_errors)
                    rejected += 1
                    continue
                outcome, row_error = self._store_reading(connection, batch_id, row_number, normalized, now)
                if row_error is not None:
                    errors.append(row_error)
                    rejected += 1
                elif outcome == "duplicate":
                    duplicates += 1
                else:
                    accepted += 1
                    if outcome == "supersede":
                        superseded += 1
            errors.sort(key=lambda item: (item["row_number"], item["code"]))

            rule_version = self._current_rule_version(connection)
            run, reused = self._recompute(connection, rule_version, origin="import", batch_id=batch_id, actor="import")
            status = self._batch_status(accepted, rejected)
            summary = {
                "batch_id": batch_id,
                "source": source,
                "batch_key": batch_key,
                "format": fmt,
                "status": status,
                "total_rows": accepted + duplicates + rejected,
                "accepted_rows": accepted,
                "duplicate_rows": duplicates,
                "rejected_rows": rejected,
                "superseded_rows": superseded,
                "errors": errors,
                "aggregation_run": self._run_summary(run, reused),
            }
            connection.execute(
                "UPDATE lab_batches SET status=?,total_rows=?,accepted_rows=?,duplicate_rows=?,rejected_rows=?,superseded_rows=?,error_report_json=?,summary_json=? WHERE id=?",
                (
                    status,
                    summary["total_rows"],
                    accepted,
                    duplicates,
                    rejected,
                    superseded,
                    json.dumps(errors, ensure_ascii=False),
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                    batch_id,
                ),
            )
            return summary, True

    def _insert_batch(self, connection: sqlite3.Connection, source: str, batch_key: str, fmt: str, content_digest: str, now: str) -> int:
        cursor = connection.execute(
            "INSERT INTO lab_batches(source,batch_key,format,content_digest,status,created_at) VALUES(?,?,?,?,'accepted',?)",
            (source, batch_key, fmt, content_digest, now),
        )
        return int(cursor.lastrowid)

    def _store_reading(
        self, connection: sqlite3.Connection, batch_id: int, row_number: int, normalized: dict[str, Any], now: str
    ) -> tuple[str, dict[str, Any] | None]:
        """写入一条规范化读数，返回 (结果, 行错误)。

        结果为 accepted / duplicate / supersede 之一；同一自然键更高修订号
        的读数成为新的当前读数（迟到修订），旧行保留且永不修改。
        """

        reading_key = digest(
            [
                normalized["payload"],
                normalized["cycle_phase"],
                normalized["model_version"],
                normalized["recorded_at"],
                normalized["sensor_id"],
            ]
        )
        latest = connection.execute(
            "SELECT * FROM lab_readings WHERE reading_key=? ORDER BY revision DESC LIMIT 1", (reading_key,)
        ).fetchone()
        revision = normalized["revision"]
        if latest is not None:
            if revision < latest["revision"]:
                return "rejected", {
                    "row_number": row_number,
                    "code": "stale_revision",
                    "field": "revision",
                    "message": f"读数修订号 {revision} 低于已接收的修订号 {latest['revision']}",
                }
            if revision == latest["revision"]:
                same_content = (
                    float(latest["temperature_c"]) == normalized["temperature_c"]
                    and float(latest["runtime_seconds"]) == normalized["runtime_seconds"]
                    and int(latest["anomaly_flag"]) == normalized["anomaly_flag"]
                )
                if same_content:
                    return "duplicate", None
                return "rejected", {
                    "row_number": row_number,
                    "code": "conflicting_revision",
                    "field": "revision",
                    "message": f"修订号 {revision} 已存在但内容不一致",
                }
        connection.execute(
            "INSERT INTO lab_readings(batch_id,row_number,reading_key,payload,cycle_phase,model_version,sensor_id,recorded_at,temperature_c,runtime_seconds,anomaly_flag,revision,raw_json,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                row_number,
                reading_key,
                normalized["payload"],
                normalized["cycle_phase"],
                normalized["model_version"],
                normalized["sensor_id"],
                normalized["recorded_at"],
                normalized["temperature_c"],
                normalized["runtime_seconds"],
                normalized["anomaly_flag"],
                revision,
                normalized["raw_json"],
                now,
            ),
        )
        return ("supersede" if latest is not None else "accepted"), None

    @staticmethod
    def _batch_status(accepted: int, rejected: int) -> str:
        if rejected and not accepted:
            return "rejected"
        if rejected:
            return "accepted_with_errors"
        return "accepted"

    # ------------------------------------------------------------------ 重算

    def recompute(self, rule_version: str | None, actor: str) -> dict[str, Any]:
        """按指定（或当前）规则版本全量重算聚合。

        相同规则版本 + 相同输入读数集合的重算是幂等的：直接复用已有运行。
        """

        with transaction(immediate=True) as connection:
            version = rule_version or self._current_rule_version(connection)
            run, reused = self._recompute(connection, version, origin="manual", batch_id=None, actor=actor)
            return {"run": self._run_view(connection, run["id"]), "reused": reused}

    def _recompute(
        self, connection: sqlite3.Connection, rule_version: str, *, origin: str, batch_id: int | None, actor: str
    ) -> tuple[dict[str, Any], bool]:
        rules = RULE_SETS.get(rule_version)
        if rules is None:
            raise ValidationError("未知的聚合规则版本", context={"rule_version": rule_version, "available": sorted(RULE_SETS)})
        readings = [dict(row) for row in connection.execute(CURRENT_READINGS_SQL + " ORDER BY r.reading_key").fetchall()]
        input_digest = digest(
            [[r["reading_key"], r["revision"], r["temperature_c"], r["runtime_seconds"], r["anomaly_flag"]] for r in readings]
        )
        existing = connection.execute(
            "SELECT * FROM lab_aggregation_runs WHERE rule_version=? AND input_digest=? ORDER BY id LIMIT 1",
            (rule_version, input_digest),
        ).fetchone()
        if existing is not None:
            self._set_current_run(connection, int(existing["id"]))
            return dict(existing), True

        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for reading in readings:
            key = (reading["payload"], reading["cycle_phase"], reading["model_version"])
            groups.setdefault(key, []).append(reading)
        new_metrics: dict[tuple[str, str, str], dict[str, Any]] = {}
        explanations: list[dict[str, Any]] = []
        for key in sorted(groups):
            ordered = sorted(groups[key], key=lambda item: (item["recorded_at"], item["id"]))
            metrics, group_explanations = aggregate_group(ordered, rules)
            new_metrics[key] = metrics
            explanations.extend(group_explanations)
        explanations.sort(
            key=lambda item: (item["payload"], item["cycle_phase"], item["model_version"], item["recorded_at"], item["reading_id"], item["rule_code"])
        )

        previous = self._current_run_metrics(connection)
        affected = [
            key
            for key in sorted(set(new_metrics) | set(previous))
            if new_metrics.get(key) != previous.get(key)
        ]
        changes = [
            {
                "group": _group_key(*key),
                "before": previous.get(key),
                "after": new_metrics.get(key),
            }
            for key in affected
        ]
        now = to_storage(self.clock.now())
        cursor = connection.execute(
            "INSERT INTO lab_aggregation_runs(rule_version,origin,actor,batch_id,group_count,readings_count,affected_groups_json,changes_json,input_digest,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                rule_version,
                origin,
                actor,
                batch_id,
                len(new_metrics),
                len(readings),
                json.dumps([_group_key(*key) for key in affected], ensure_ascii=False),
                json.dumps(changes, ensure_ascii=False),
                input_digest,
                now,
            ),
        )
        run_id = int(cursor.lastrowid)
        for key in sorted(new_metrics):
            metrics = new_metrics[key]
            connection.execute(
                "INSERT INTO lab_aggregates(run_id,rule_version,payload,cycle_phase,model_version,reading_count,peak_temperature_c,effective_runtime_seconds,anomaly_count,first_recorded_at,last_recorded_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    rule_version,
                    key[0],
                    key[1],
                    key[2],
                    metrics["reading_count"],
                    metrics["peak_temperature_c"],
                    metrics["effective_runtime_seconds"],
                    metrics["anomaly_count"],
                    metrics["first_recorded_at"],
                    metrics["last_recorded_at"],
                ),
            )
        for explanation in explanations:
            connection.execute(
                "INSERT INTO lab_anomaly_explanations(run_id,reading_id,payload,cycle_phase,model_version,recorded_at,rule_code,detail,observed_value,threshold_value)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    explanation["reading_id"],
                    explanation["payload"],
                    explanation["cycle_phase"],
                    explanation["model_version"],
                    explanation["recorded_at"],
                    explanation["rule_code"],
                    explanation["detail"],
                    explanation["observed_value"],
                    explanation["threshold_value"],
                ),
            )
        self._set_current_run(connection, run_id)
        return dict(connection.execute("SELECT * FROM lab_aggregation_runs WHERE id=?", (run_id,)).fetchone()), False

    def _current_rule_version(self, connection: sqlite3.Connection) -> str:
        row = connection.execute(
            "SELECT r.rule_version FROM lab_aggregation_state s JOIN lab_aggregation_runs r ON r.id=s.current_run_id WHERE s.id=1"
        ).fetchone()
        return str(row["rule_version"]) if row else DEFAULT_RULE_VERSION

    def _current_run_id(self, connection: sqlite3.Connection) -> int | None:
        row = connection.execute("SELECT current_run_id FROM lab_aggregation_state WHERE id=1").fetchone()
        return int(row["current_run_id"]) if row and row["current_run_id"] is not None else None

    def _current_run_metrics(self, connection: sqlite3.Connection) -> dict[tuple[str, str, str], dict[str, Any]]:
        run_id = self._current_run_id(connection)
        if run_id is None:
            return {}
        rows = connection.execute("SELECT * FROM lab_aggregates WHERE run_id=?", (run_id,)).fetchall()
        return {
            (row["payload"], row["cycle_phase"], row["model_version"]): {field: row[field] for field in METRIC_FIELDS}
            for row in rows
        }

    @staticmethod
    def _set_current_run(connection: sqlite3.Connection, run_id: int) -> None:
        connection.execute(
            "INSERT INTO lab_aggregation_state(id,current_run_id) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET current_run_id=excluded.current_run_id",
            (run_id,),
        )

    @staticmethod
    def _run_summary(run: dict[str, Any], reused: bool) -> dict[str, Any]:
        return {
            "id": run["id"],
            "rule_version": run["rule_version"],
            "origin": run["origin"],
            "reused": reused,
            "group_count": run["group_count"],
            "readings_count": run["readings_count"],
            "affected_groups": json.loads(run["affected_groups_json"]),
        }

    def _run_view(self, connection: sqlite3.Connection, run_id: int) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM lab_aggregation_runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise NotFoundError("聚合运行不存在")
        result = dict(row)
        result["affected_groups"] = json.loads(result.pop("affected_groups_json"))
        result["changes"] = json.loads(result.pop("changes_json"))
        return result

    # ------------------------------------------------------------------ 查询

    def list_batches(self, *, source: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses = ""
        params: list[Any] = []
        if source:
            clauses = " WHERE source=?"
            params.append(source)
        params.append(max(1, min(limit, 500)))
        rows = self.connection.execute(
            "SELECT id,source,batch_key,format,status,total_rows,accepted_rows,duplicate_rows,rejected_rows,superseded_rows,created_at"
            " FROM lab_batches" + clauses + " ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM lab_batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        result = dict(row)
        result["errors"] = json.loads(result.pop("error_report_json"))
        result.pop("summary_json")
        result.pop("content_digest")
        return result

    def list_readings(
        self,
        *,
        payload: str | None = None,
        cycle_phase: str | None = None,
        model_version: str | None = None,
        current_only: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (("payload", payload), ("cycle_phase", cycle_phase), ("model_version", model_version)):
            if value:
                clauses.append(f"r.{column}=?")
                params.append(value)
        if current_only:
            clauses.append("r.revision=(SELECT MAX(revision) FROM lab_readings x WHERE x.reading_key=r.reading_key)")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 1000)))
        rows = self.connection.execute(
            "SELECT r.*, CASE WHEN r.revision=(SELECT MAX(revision) FROM lab_readings x WHERE x.reading_key=r.reading_key)"
            " THEN 1 ELSE 0 END AS current FROM lab_readings r" + where + " ORDER BY r.recorded_at,r.id LIMIT ?",
            params,
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["current"] = bool(item["current"])
            result.append(item)
        return result

    def current_aggregates(
        self, *, payload: str | None = None, cycle_phase: str | None = None, model_version: str | None = None
    ) -> dict[str, Any]:
        run_id = self._current_run_id(self.connection)
        if run_id is None:
            return {"run_id": None, "rule_version": None, "items": []}
        run = self.connection.execute("SELECT rule_version FROM lab_aggregation_runs WHERE id=?", (run_id,)).fetchone()
        clauses: list[str] = ["run_id=?"]
        params: list[Any] = [run_id]
        for column, value in (("payload", payload), ("cycle_phase", cycle_phase), ("model_version", model_version)):
            if value:
                clauses.append(f"{column}=?")
                params.append(value)
        rows = self.connection.execute(
            "SELECT payload,cycle_phase,model_version,reading_count,peak_temperature_c,effective_runtime_seconds,anomaly_count,first_recorded_at,last_recorded_at"
            " FROM lab_aggregates WHERE " + " AND ".join(clauses) + " ORDER BY payload,cycle_phase,model_version",
            params,
        ).fetchall()
        return {"run_id": run_id, "rule_version": run["rule_version"], "items": [dict(row) for row in rows]}

    def list_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM lab_aggregation_runs ORDER BY id DESC LIMIT ?", (max(1, min(limit, 200)),)
        ).fetchall()
        current_id = self._current_run_id(self.connection)
        items = []
        for row in rows:
            item = dict(row)
            item["affected_groups"] = json.loads(item.pop("affected_groups_json"))
            item.pop("changes_json")
            item["current"] = item["id"] == current_id
            items.append(item)
        return items

    def get_run(self, run_id: int) -> dict[str, Any]:
        run = self._run_view(self.connection, run_id)
        rows = self.connection.execute(
            "SELECT payload,cycle_phase,model_version,reading_count,peak_temperature_c,effective_runtime_seconds,anomaly_count,first_recorded_at,last_recorded_at"
            " FROM lab_aggregates WHERE run_id=? ORDER BY payload,cycle_phase,model_version",
            (run_id,),
        ).fetchall()
        run["items"] = [dict(row) for row in rows]
        run["current"] = run_id == self._current_run_id(self.connection)
        return run

    def explain_anomalies(
        self,
        *,
        run_id: int | None = None,
        payload: str | None = None,
        cycle_phase: str | None = None,
        model_version: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        effective_run_id = run_id if run_id is not None else self._current_run_id(self.connection)
        if effective_run_id is None:
            return {"run_id": None, "rule_version": None, "items": []}
        run = self.connection.execute("SELECT rule_version FROM lab_aggregation_runs WHERE id=?", (effective_run_id,)).fetchone()
        if run is None:
            raise NotFoundError("聚合运行不存在")
        clauses = ["e.run_id=?"]
        params: list[Any] = [effective_run_id]
        for column, value in (("payload", payload), ("cycle_phase", cycle_phase), ("model_version", model_version)):
            if value:
                clauses.append(f"e.{column}=?")
                params.append(value)
        params.append(max(1, min(limit, 2000)))
        rows = self.connection.execute(
            "SELECT e.reading_id,e.payload,e.cycle_phase,e.model_version,e.recorded_at,e.rule_code,e.detail,e.observed_value,e.threshold_value,"
            " b.source,b.batch_key FROM lab_anomaly_explanations e"
            " JOIN lab_readings r ON r.id=e.reading_id JOIN lab_batches b ON b.id=r.batch_id"
            " WHERE " + " AND ".join(clauses)
            + " ORDER BY e.payload,e.cycle_phase,e.model_version,e.recorded_at,e.reading_id,e.rule_code LIMIT ?",
            params,
        ).fetchall()
        return {"run_id": effective_run_id, "rule_version": run["rule_version"], "items": [dict(row) for row in rows]}

    def list_rules(self) -> list[dict[str, Any]]:
        current = self._current_rule_version(self.connection)
        return [
            {
                "version": rules.version,
                "description": rules.description,
                "temp_min_c": rules.temp_min_c,
                "temp_max_c": rules.temp_max_c,
                "runtime_cap_seconds": rules.runtime_cap_seconds,
                "peak_excludes_flagged": rules.peak_excludes_flagged,
                "current": rules.version == current,
            }
            for rules in (RULE_SETS[version] for version in sorted(RULE_SETS))
        ]
