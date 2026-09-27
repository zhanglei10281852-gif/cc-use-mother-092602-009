from __future__ import annotations

BASE_ROW = {
    "reading_key": "R-1",
    "payload_id": "P-1",
    "cycle_phase": "soak",
    "model_version": "m1",
    "recorded_at": "2026-09-27T08:00:00Z",
    "temperature": 120.5,
    "temperature_unit": "C",
    "run_duration": 600,
    "duration_unit": "s",
    "anomaly_count": 0,
}

GROUP = {"payload_id": "P-1", "cycle_phase": "soak", "model_version": "m1"}


def import_batch(client, batch_key, rows, **extra):
    payload = {"batch_key": batch_key, "source": "lab-a", "format": "json", "rows": rows}
    payload.update(extra)
    return client.post("/api/telemetry/imports", json=payload)


def test_json_import_and_readings(client):
    response = import_batch(client, "B-001", [BASE_ROW, {**BASE_ROW, "reading_key": "R-2", "temperature": 99.9}])
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["total_rows"] == 2
    assert summary["accepted_rows"] == 2
    assert summary["rejected_rows"] == 0
    assert summary["rule_code"] == "thermal-lab"
    assert summary["rule_version"] == "v1"
    assert summary["affected_groups"] == [GROUP]

    readings = client.get("/api/telemetry/readings").json()["items"]
    assert len(readings) == 2
    assert {item["reading_key"] for item in readings} == {"R-1", "R-2"}
    assert all(item["is_current"] == 1 and item["revision"] == 1 for item in readings)


def test_csv_import_with_unit_conversion(client):
    content = (
        "reading_key,payload_id,cycle_phase,model_version,recorded_at,temperature,temperature_unit,run_duration,duration_unit,anomaly_count\n"
        "R-1,P-1,soak,m1,2026-09-27T08:00:00+00:00,212,F,1.5,min,1\n"
        "R-2,P-1,ramp_up,m1,2026-09-27T09:00:00+00:00,373.15,K,0.5,h,0\n"
    )
    response = client.post(
        "/api/telemetry/imports",
        json={"batch_key": "B-CSV-1", "source": "lab-b", "format": "csv", "content": content},
    )
    assert response.status_code == 201, response.text
    assert response.json()["accepted_rows"] == 2

    readings = {item["reading_key"]: item for item in client.get("/api/telemetry/readings").json()["items"]}
    assert readings["R-1"]["temperature_c"] == 100.0
    assert readings["R-1"]["run_seconds"] == 90.0
    assert readings["R-1"]["anomaly_count"] == 1
    assert readings["R-2"]["temperature_c"] == 100.0
    assert readings["R-2"]["run_seconds"] == 1800.0


def test_bad_rows_do_not_block_good_rows(client):
    rows = [
        BASE_ROW,
        {**BASE_ROW, "reading_key": "R-bad-unit", "temperature_unit": "rankine"},
        {**BASE_ROW, "reading_key": "R-missing", "temperature": None},
        {**BASE_ROW, "reading_key": "R-range", "temperature": 9999},
        {**BASE_ROW, "reading_key": "R-phase", "cycle_phase": "explode"},
        "not-a-dict",
    ]
    response = import_batch(client, "B-002", rows)
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["accepted_rows"] == 1
    assert summary["rejected_rows"] == 5

    errors = summary["errors"]
    assert [(e["row_index"], e["error_code"]) for e in errors] == sorted((e["row_index"], e["error_code"]) for e in errors)
    by_key = {e["reading_key"]: e["error_code"] for e in errors}
    assert by_key["R-bad-unit"] == "UNKNOWN_UNIT"
    assert by_key["R-missing"] == "MISSING_FIELD"
    assert by_key["R-range"] == "OUT_OF_RANGE"
    assert by_key["R-phase"] == "UNKNOWN_PHASE"
    assert any(e["error_code"] == "ROW_NOT_OBJECT" for e in errors)

    fetched = client.get("/api/telemetry/imports/B-002").json()
    assert fetched == summary
    assert len(client.get("/api/telemetry/readings").json()["items"]) == 1


def test_error_report_stable_ordering(client):
    rows = [
        {**BASE_ROW, "reading_key": "R-late", "cycle_phase": "nope"},
        BASE_ROW,
        {**BASE_ROW, "reading_key": "R-multi", "temperature_unit": "bad", "duration_unit": "bad"},
        {**BASE_ROW, "reading_key": "R-early", "temperature": "hot"},
    ]
    summary = import_batch(client, "B-003", rows).json()
    keys = [(e["row_index"], e["error_code"], e["reading_key"]) for e in summary["errors"]]
    assert keys == sorted(keys)
    assert [e["row_index"] for e in summary["errors"]] == [0, 2, 2, 3]


def test_duplicate_batch_returns_same_summary(client):
    first = import_batch(client, "B-004", [BASE_ROW])
    assert first.status_code == 201
    replay = import_batch(client, "B-004", [BASE_ROW])
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert len(client.get("/api/telemetry/readings").json()["items"]) == 1


def test_batch_key_conflict_on_different_content(client):
    assert import_batch(client, "B-005", [BASE_ROW]).status_code == 201
    conflict = import_batch(client, "B-005", [{**BASE_ROW, "temperature": 50}])
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "conflict"


def test_dedup_and_late_revision(client):
    assert import_batch(client, "B-100", [BASE_ROW]).status_code == 201

    duplicate = import_batch(client, "B-101", [BASE_ROW]).json()
    assert duplicate["duplicate_rows"] == 1
    assert duplicate["accepted_rows"] == 0
    assert duplicate["affected_groups"] == []

    revised = import_batch(client, "B-102", [{**BASE_ROW, "temperature": 131.0, "anomaly_count": 2}]).json()
    assert revised["revised_rows"] == 1
    assert revised["affected_groups"] == [GROUP]

    history = client.get("/api/telemetry/readings/R-1/history").json()["items"]
    assert [item["revision"] for item in history] == [1, 2]
    assert history[0]["is_current"] == 0
    assert history[0]["superseded_by"] == history[1]["id"]
    assert history[0]["temperature_c"] == 120.5  # 原始读数保留，不被覆盖
    assert history[1]["is_current"] == 1
    assert history[1]["temperature_c"] == 131.0

    current = client.get("/api/telemetry/readings").json()["items"]
    assert len(current) == 1 and current[0]["revision"] == 2
    everything = client.get("/api/telemetry/readings?include_superseded=true").json()["items"]
    assert len(everything) == 2


def test_recompute_aggregates_by_rule_version(client):
    rows = [
        {**BASE_ROW, "reading_key": "R-1", "temperature": 195.0, "run_duration": 600, "anomaly_count": 2},
        {**BASE_ROW, "reading_key": "R-2", "temperature": 100.0, "run_duration": 600, "anomaly_count": 0},
    ]
    assert import_batch(client, "B-200", rows).status_code == 201

    recompute = client.post("/api/telemetry/recomputes", json={"requested_by": "tester"})
    assert recompute.status_code == 201, recompute.text
    record = recompute.json()
    assert record["rule_code"] == "thermal-lab"
    assert record["rule_version"] == "v1"
    assert record["affected_groups"] == [GROUP]
    assert record["reading_count"] == 2
    assert record["input_digest"]

    aggregates = client.get("/api/telemetry/aggregates").json()["items"]
    assert len(aggregates) == 1
    aggregate = aggregates[0]
    assert aggregate["rule_version"] == "v1"
    assert aggregate["recompute_id"] == record["id"]
    assert aggregate["reading_count"] == 2
    assert aggregate["peak_temperature_c"] == 195.0
    # 默认规则：超阈值读数被标记且不计入有效运行时长
    assert aggregate["anomaly_total"] == 3  # 仪器 2 次 + 规则标记 1 次
    assert aggregate["effective_run_seconds"] == 600.0

    # 原始读数不被聚合覆盖
    readings = client.get("/api/telemetry/readings").json()["items"]
    assert {item["reading_key"]: item["temperature_c"] for item in readings} == {"R-1": 195.0, "R-2": 100.0}

    # 注册新规则版本并按版本重算
    created = client.post(
        "/api/telemetry/rules",
        json={
            "code": "thermal-lab",
            "version": "v2",
            "config": {"anomaly_temperature_c": 300.0, "effective_exclude_flags": []},
            "created_by": "tester",
        },
    )
    assert created.status_code == 201, created.text

    recompute_v2 = client.post("/api/telemetry/recomputes", json={"rule_version": "v2", "requested_by": "tester"})
    assert recompute_v2.status_code == 201
    assert recompute_v2.json()["rule_version"] == "v2"

    by_version = {
        item["rule_version"]: item
        for item in client.get("/api/telemetry/aggregates", params=GROUP).json()["items"]
    }
    assert by_version["v1"]["anomaly_total"] == 3
    assert by_version["v2"]["anomaly_total"] == 2  # 阈值提高后无规则标记
    assert by_version["v2"]["effective_run_seconds"] == 1200.0

    detail = client.get(f"/api/telemetry/recomputes/{record['id']}").json()
    assert detail["rule_version"] == "v1"
    runs = client.get("/api/telemetry/recomputes").json()["items"]
    assert [run["id"] for run in runs] == sorted((run["id"] for run in runs), reverse=True)


def test_aggregate_history_shows_late_revision_impact(client):
    assert import_batch(client, "B-300", [BASE_ROW]).status_code == 201
    client.post("/api/telemetry/recomputes", json={})
    first = client.get("/api/telemetry/aggregates", params=GROUP).json()["items"][0]
    assert first["peak_temperature_c"] == 120.5

    late = import_batch(client, "B-301", [{**BASE_ROW, "temperature": 150.0}])
    assert late.json()["revised_rows"] == 1
    assert late.json()["affected_groups"] == [GROUP]  # 迟到数据的影响范围

    client.post("/api/telemetry/recomputes", json={})
    history = client.get("/api/telemetry/aggregates/history", params=GROUP).json()["items"]
    assert len(history) == 2
    assert history[0]["is_current"] == 0
    assert history[0]["peak_temperature_c"] == 120.5
    assert history[1]["is_current"] == 1
    assert history[1]["peak_temperature_c"] == 150.0
    assert history[1]["recompute_id"] > history[0]["recompute_id"]


def test_anomaly_explain(client):
    rows = [
        {**BASE_ROW, "reading_key": "R-1", "temperature": 195.0, "anomaly_count": 2},
        {**BASE_ROW, "reading_key": "R-2", "temperature": 100.0},
    ]
    assert import_batch(client, "B-400", rows).status_code == 201
    client.post("/api/telemetry/recomputes", json={})

    explain = client.get("/api/telemetry/anomalies/explain", params=GROUP)
    assert explain.status_code == 200, explain.text
    body = explain.json()
    assert body["rule_version"] == "v1"
    assert body["anomaly_total"] == 3
    assert len(body["contributions"]) == 1
    contribution = body["contributions"][0]
    assert contribution["reading_key"] == "R-1"
    assert contribution["flags"] == ["TEMP_ABOVE_THRESHOLD"]
    assert any("180" in reason for reason in contribution["reasons"])
    assert any("2 次" in reason for reason in contribution["reasons"])
    assert contribution["counts_toward_effective_duration"] is False

    missing = client.get("/api/telemetry/anomalies/explain", params={"payload_id": "P-x", "cycle_phase": "soak", "model_version": "m1"})
    assert missing.status_code == 404


def test_recompute_scoped_to_groups(client):
    rows = [
        BASE_ROW,
        {**BASE_ROW, "reading_key": "R-2", "payload_id": "P-2"},
    ]
    assert import_batch(client, "B-500", rows).status_code == 201
    response = client.post("/api/telemetry/recomputes", json={"groups": [GROUP], "requested_by": "tester"})
    assert response.status_code == 201
    record = response.json()
    assert record["scope"] == "groups"
    assert record["affected_groups"] == [GROUP]
    assert record["reading_count"] == 1
    aggregates = client.get("/api/telemetry/aggregates").json()["items"]
    assert len(aggregates) == 1
    assert aggregates[0]["payload_id"] == "P-1"


def test_unknown_rule_version_rejected(client):
    assert import_batch(client, "B-600", [BASE_ROW]).status_code == 201
    response = client.post("/api/telemetry/recomputes", json={"rule_version": "v99"})
    assert response.status_code == 404
    duplicate_rule = client.post("/api/telemetry/rules", json={"code": "thermal-lab", "version": "v1", "config": {}})
    assert duplicate_rule.status_code == 409
