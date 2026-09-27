from __future__ import annotations

import json

CSV_HEADER = "payload,cycle_phase,model_version,recorded_at,temperature,temperature_unit,runtime,runtime_unit,anomaly_flag,revision,sensor_id"


def csv_content(rows: list[str]) -> str:
    return CSV_HEADER + "\n" + "\n".join(rows)


def post_import(client, content: str, *, source: str = "chamber-1", batch_key: str, fmt: str = "csv"):
    return client.post(
        "/api/lab/imports",
        json={"source": source, "batch_key": batch_key, "format": fmt, "content": content},
    )


def test_csv_import_normalizes_units_and_aggregates(client):
    rows = [
        "SAT-1,ramp_up,mv-a,2026-09-26T10:00:00Z,25,C,1800,s,0,1,TC-1",
        "SAT-1,ramp_up,mv-a,2026-09-26T10:05:00Z,77,F,30,min,0,1,TC-2",
        "SAT-1,ramp_up,mv-a,2026-09-26T10:10:00Z,298.15,K,0.5,h,0,1,TC-3",
    ]
    response = post_import(client, csv_content(rows), batch_key="2026-09-26-am")
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["status"] == "accepted"
    assert summary["accepted_rows"] == 3
    assert summary["aggregation_run"]["rule_version"] == "v2"
    assert summary["aggregation_run"]["reused"] is False

    aggregates = client.get("/api/lab/aggregates", params={"payload": "SAT-1"}).json()
    assert aggregates["rule_version"] == "v2"
    (item,) = aggregates["items"]
    assert item["reading_count"] == 3
    assert item["peak_temperature_c"] == 25.0
    assert item["effective_runtime_seconds"] == 5400.0
    assert item["anomaly_count"] == 0
    assert item["first_recorded_at"] == "2026-09-26T10:00:00+00:00"

    readings = client.get("/api/lab/readings", params={"payload": "SAT-1"}).json()["items"]
    assert [row["temperature_c"] for row in readings] == [25.0, 25.0, 25.0]
    assert [row["runtime_seconds"] for row in readings] == [1800.0, 1800.0, 1800.0]
    assert all(row["current"] for row in readings)


def test_bad_rows_do_not_block_good_rows_and_errors_are_stably_sorted(client):
    rows = [
        "SAT-2,high_soak,mv-a,2026-09-26T10:00:00Z,150,C,600,s,0,1,TC-1",
        "SAT-2,high_soak,mv-a,2026-09-26T10:05:00Z,150,X,600,s,0,1,TC-1",
        "SAT-2,high_soak,mv-a,2026-09-26T10:10:00Z,150,C,,s,0,1,TC-1",
        "SAT-2,high_soak,mv-a,not-a-time,150,C,600,s,0,1,TC-1",
        "SAT-2,high_soak,mv-a,2026-09-26T10:15:00Z,160,C,600,s,0,1,TC-1",
    ]
    response = post_import(client, csv_content(rows), batch_key="2026-09-26-with-errors")
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["status"] == "accepted_with_errors"
    assert summary["accepted_rows"] == 2
    assert summary["rejected_rows"] == 3
    assert summary["total_rows"] == 5
    errors = summary["errors"]
    assert [entry["row_number"] for entry in errors] == [2, 3, 4]
    assert [entry["code"] for entry in errors] == ["invalid_unit", "missing_field", "invalid_timestamp"]

    detail = client.get(f"/api/lab/batches/{summary['batch_id']}").json()
    assert detail["errors"] == errors

    (item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-2"}).json()["items"]
    assert item["reading_count"] == 2
    assert item["peak_temperature_c"] == 160.0


def test_duplicate_batch_returns_same_summary_and_conflicting_content_rejected(client):
    content = csv_content(
        [
            "SAT-5,ramp_down,mv-a,2026-09-26T10:00:00Z,25,C,60,s,0,1,TC-1",
            "SAT-5,ramp_down,mv-a,2026-09-26T10:05:00Z,26,C,60,s,0,1,TC-2",
        ]
    )
    first = post_import(client, content, batch_key="dup-1")
    second = post_import(client, content, batch_key="dup-1")
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json() == second.json()

    batches = client.get("/api/lab/batches").json()["items"]
    assert len(batches) == 1
    readings = client.get("/api/lab/readings", params={"payload": "SAT-5"}).json()["items"]
    assert len(readings) == 2

    changed = post_import(client, content + "\nSAT-5,ramp_down,mv-a,2026-09-26T10:10:00Z,27,C,60,s,0,1,TC-3", batch_key="dup-1")
    assert changed.status_code == 409


def test_late_revision_supersedes_and_raw_readings_are_preserved(client):
    first = post_import(
        client,
        csv_content(["SAT-3,low_soak,mv-b,2026-09-26T10:00:00Z,100,C,600,s,0,1,TC-1"]),
        batch_key="rev-1",
    )
    assert first.status_code == 201
    second = post_import(
        client,
        csv_content(["SAT-3,low_soak,mv-b,2026-09-26T10:00:00Z,120,C,600,s,0,2,TC-1"]),
        batch_key="rev-2",
    )
    assert second.status_code == 201, second.text
    summary = second.json()
    assert summary["accepted_rows"] == 1
    assert summary["superseded_rows"] == 1
    assert summary["aggregation_run"]["affected_groups"] == [
        {"payload": "SAT-3", "cycle_phase": "low_soak", "model_version": "mv-b"}
    ]

    readings = client.get("/api/lab/readings", params={"payload": "SAT-3"}).json()["items"]
    assert len(readings) == 2
    old, new = readings
    assert old["revision"] == 1 and old["current"] is False
    assert old["temperature_c"] == 100.0  # 原始读数未被修订或聚合覆盖
    assert new["revision"] == 2 and new["current"] is True
    assert new["temperature_c"] == 120.0

    (item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-3"}).json()["items"]
    assert item["reading_count"] == 1
    assert item["peak_temperature_c"] == 120.0

    run = client.get(f"/api/lab/aggregations/runs/{summary['aggregation_run']['id']}").json()
    assert run["changes"][0]["group"] == {"payload": "SAT-3", "cycle_phase": "low_soak", "model_version": "mv-b"}
    assert run["changes"][0]["before"]["peak_temperature_c"] == 100.0
    assert run["changes"][0]["after"]["peak_temperature_c"] == 120.0


def test_stale_and_conflicting_revisions_are_rejected(client):
    post_import(
        client,
        csv_content(["SAT-6,annealing,mv-b,2026-09-26T10:00:00Z,100,C,600,s,0,2,TC-1"]),
        batch_key="base-rev-2",
    )
    response = post_import(
        client,
        csv_content(
            [
                "SAT-6,annealing,mv-b,2026-09-26T10:00:00Z,99,C,600,s,0,1,TC-1",
                "SAT-6,annealing,mv-b,2026-09-26T10:00:00Z,999,C,600,s,0,2,TC-1",
            ]
        ),
        batch_key="stale-and-conflict",
    )
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["status"] == "rejected"
    assert summary["accepted_rows"] == 0
    assert summary["rejected_rows"] == 2
    assert [entry["code"] for entry in summary["errors"]] == ["stale_revision", "conflicting_revision"]

    (item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-6"}).json()["items"]
    assert item["peak_temperature_c"] == 100.0


def test_recompute_by_rule_version_is_versioned_and_idempotent(client):
    rows = [
        "SAT-4,irradiation,mv-c,2026-09-26T10:00:00Z,200,C,100,s,0,1,TC-1",
        "SAT-4,irradiation,mv-c,2026-09-26T10:05:00Z,150,C,5000,s,1,1,TC-2",
    ]
    imported = post_import(client, csv_content(rows), batch_key="rules-base")
    assert imported.status_code == 201
    v2_run_id = imported.json()["aggregation_run"]["id"]

    (v2_item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-4"}).json()["items"]
    assert v2_item["reading_count"] == 2
    assert v2_item["anomaly_count"] == 2
    assert v2_item["effective_runtime_seconds"] == 3600.0
    assert v2_item["peak_temperature_c"] is None

    recomputed = client.post("/api/lab/aggregations/recompute", json={"rule_version": "v1", "actor": "researcher-1"})
    assert recomputed.status_code == 200, recomputed.text
    run = recomputed.json()["run"]
    assert recomputed.json()["reused"] is False
    assert run["rule_version"] == "v1"
    assert run["origin"] == "manual"
    assert run["affected_groups"] == [{"payload": "SAT-4", "cycle_phase": "irradiation", "model_version": "mv-c"}]

    (v1_item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-4"}).json()["items"]
    assert v1_item["anomaly_count"] == 1
    assert v1_item["effective_runtime_seconds"] == 5100.0
    assert v1_item["peak_temperature_c"] == 200.0

    again = client.post("/api/lab/aggregations/recompute", json={"rule_version": "v1", "actor": "researcher-1"})
    assert again.json()["reused"] is True
    assert again.json()["run"]["id"] == run["id"]

    back = client.post("/api/lab/aggregations/recompute", json={"rule_version": "v2", "actor": "researcher-1"})
    assert back.json()["reused"] is True
    assert back.json()["run"]["id"] == v2_run_id
    (item,) = client.get("/api/lab/aggregates", params={"payload": "SAT-4"}).json()["items"]
    assert item["effective_runtime_seconds"] == 3600.0

    runs = client.get("/api/lab/aggregations/runs").json()["items"]
    assert [item["rule_version"] for item in runs] == ["v1", "v2"]
    current = [item for item in runs if item["current"]]
    assert len(current) == 1 and current[0]["rule_version"] == "v2"

    unknown = client.post("/api/lab/aggregations/recompute", json={"rule_version": "v9", "actor": "researcher-1"})
    assert unknown.status_code == 422


def test_anomaly_explanations_follow_current_rule_version(client):
    rows = [
        "SAT-7,irradiation,mv-c,2026-09-26T10:00:00Z,200,C,100,s,0,1,TC-1",
        "SAT-7,irradiation,mv-c,2026-09-26T10:05:00Z,150,C,5000,s,1,1,TC-2",
    ]
    post_import(client, csv_content(rows), batch_key="anomaly-base")

    explained = client.get("/api/lab/anomalies", params={"payload": "SAT-7"}).json()
    assert explained["rule_version"] == "v2"
    items = explained["items"]
    assert [item["rule_code"] for item in items] == ["temperature_out_of_range", "runtime_exceeds_cap", "source_flagged"]
    assert items[0]["observed_value"] == 200.0
    assert items[0]["threshold_value"] == 175.0
    assert items[1]["observed_value"] == 5000.0
    assert items[1]["threshold_value"] == 3600.0
    assert all(item["source"] == "chamber-1" and item["batch_key"] == "anomaly-base" for item in items)

    client.post("/api/lab/aggregations/recompute", json={"rule_version": "v1", "actor": "researcher-1"})
    explained_v1 = client.get("/api/lab/anomalies", params={"payload": "SAT-7"}).json()
    assert explained_v1["rule_version"] == "v1"
    assert [item["rule_code"] for item in explained_v1["items"]] == ["source_flagged"]


def test_json_import_handles_duplicates_and_bad_rows(client):
    document = {
        "readings": [
            {
                "payload": "SAT-8",
                "cycle_phase": "ramp_up",
                "model_version": "mv-d",
                "recorded_at": "2026-09-26T10:00:00+08:00",
                "temperature": 25,
                "temperature_unit": "C",
                "runtime": 2,
                "runtime_unit": "min",
                "anomaly_flag": False,
            },
            {
                "payload": "SAT-8",
                "cycle_phase": "ramp_up",
                "model_version": "mv-d",
                "recorded_at": "2026-09-26T02:00:00Z",
                "temperature": 25,
                "temperature_unit": "C",
                "runtime": 120,
                "runtime_unit": "s",
            },
            "not-an-object",
            {
                "payload": "SAT-8",
                "cycle_phase": "ramp_up",
                "model_version": "mv-d",
                "recorded_at": "2026-09-26T03:00:00Z",
                "temperature": 25,
                "temperature_unit": "BTU",
                "runtime": 60,
                "runtime_unit": "s",
            },
        ]
    }
    response = post_import(client, json.dumps(document), batch_key="json-1", fmt="json")
    assert response.status_code == 201, response.text
    summary = response.json()
    assert summary["status"] == "accepted_with_errors"
    assert summary["accepted_rows"] == 1
    assert summary["duplicate_rows"] == 1
    assert summary["rejected_rows"] == 2
    assert [(entry["row_number"], entry["code"]) for entry in summary["errors"]] == [(3, "invalid_row"), (4, "invalid_unit")]

    readings = client.get("/api/lab/readings", params={"payload": "SAT-8"}).json()["items"]
    assert len(readings) == 1
    assert readings[0]["recorded_at"] == "2026-09-26T02:00:00+00:00"
    assert readings[0]["runtime_seconds"] == 120.0


def test_raw_readings_survive_recompute_and_supersede(client):
    post_import(
        client,
        csv_content(["SAT-9,high_soak,mv-e,2026-09-26T10:00:00Z,100,C,600,s,0,1,TC-1"]),
        batch_key="raw-1",
    )
    before = client.get("/api/lab/readings", params={"payload": "SAT-9"}).json()["items"]
    client.post("/api/lab/aggregations/recompute", json={"rule_version": "v1", "actor": "researcher-1"})
    client.post("/api/lab/aggregations/recompute", json={"rule_version": "v2", "actor": "researcher-1"})
    post_import(
        client,
        csv_content(["SAT-9,high_soak,mv-e,2026-09-26T10:00:00Z,101,C,600,s,0,2,TC-1"]),
        batch_key="raw-2",
    )
    client.post("/api/lab/aggregations/recompute", json={"rule_version": "v1", "actor": "researcher-1"})
    after = client.get("/api/lab/readings", params={"payload": "SAT-9"}).json()["items"]
    assert len(after) == 2
    original = next(row for row in after if row["id"] == before[0]["id"])
    for field in ("temperature_c", "runtime_seconds", "anomaly_flag", "revision", "raw_json", "recorded_at"):
        assert original[field] == before[0][field]


def test_batch_level_validation_and_rules_listing(client):
    bad_json = post_import(client, "{not json", batch_key="bad-json", fmt="json")
    assert bad_json.status_code == 422

    missing_header = post_import(client, "payload,recorded_at\nSAT-1,2026-09-26T10:00:00Z", batch_key="bad-csv")
    assert missing_header.status_code == 422

    empty = post_import(client, csv_content([]), batch_key="empty-csv")
    assert empty.status_code == 422

    rules = client.get("/api/lab/rules").json()["items"]
    assert [item["version"] for item in rules] == ["v1", "v2"]
    assert rules[0]["current"] is False
    assert rules[1]["current"] is True
    assert rules[1]["temp_max_c"] == 175.0
