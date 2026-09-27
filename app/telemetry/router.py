from __future__ import annotations

from fastapi import APIRouter, Query, Response

from app.telemetry.schemas import ImportRequest, RecomputeRequest, RuleSetCreate
from app.telemetry.service import TelemetryService

router = APIRouter(prefix="/api/telemetry", tags=["热测试数据管线"])


def service() -> TelemetryService:
    return TelemetryService()


@router.get("/rules")
def list_rules():
    return {"items": service().list_rules()}


@router.post("/rules", status_code=201)
def create_rule(payload: RuleSetCreate):
    return service().create_rule_set(payload.model_dump())


@router.post("/imports")
def import_batch(payload: ImportRequest, response: Response):
    summary, status_code = service().import_batch(payload.model_dump())
    response.status_code = status_code
    return summary


@router.get("/imports/{batch_key}")
def get_import(batch_key: str):
    return service().get_batch(batch_key)


@router.get("/readings")
def list_readings(
    payload_id: str | None = None,
    cycle_phase: str | None = None,
    model_version: str | None = None,
    include_superseded: bool = False,
    limit: int = Query(default=500, ge=1, le=2000),
):
    return {"items": service().list_readings(payload_id, cycle_phase, model_version, include_superseded, limit)}


@router.get("/readings/{reading_key}/history")
def reading_history(reading_key: str):
    return {"items": service().reading_history(reading_key)}


@router.post("/recomputes", status_code=201)
def recompute(payload: RecomputeRequest):
    return service().recompute(payload.model_dump())


@router.get("/recomputes")
def list_recomputes(limit: int = Query(default=50, ge=1, le=200)):
    return {"items": service().list_recomputes(limit)}


@router.get("/recomputes/{recompute_id}")
def get_recompute(recompute_id: int):
    return service().get_recompute(recompute_id)


@router.get("/aggregates")
def list_aggregates(
    payload_id: str | None = None,
    cycle_phase: str | None = None,
    model_version: str | None = None,
    rule_code: str | None = None,
    rule_version: str | None = None,
):
    return {"items": service().list_aggregates(payload_id, cycle_phase, model_version, rule_code, rule_version)}


@router.get("/aggregates/history")
def aggregate_history(
    payload_id: str = Query(..., min_length=1),
    cycle_phase: str = Query(..., min_length=1),
    model_version: str = Query(..., min_length=1),
    rule_code: str | None = None,
    rule_version: str | None = None,
):
    return {"items": service().aggregate_history(payload_id, cycle_phase, model_version, rule_code, rule_version)}


@router.get("/anomalies/explain")
def explain_anomalies(
    payload_id: str = Query(..., min_length=1),
    cycle_phase: str = Query(..., min_length=1),
    model_version: str = Query(..., min_length=1),
    rule_code: str | None = None,
    rule_version: str | None = None,
):
    return service().explain_anomalies(payload_id, cycle_phase, model_version, rule_code, rule_version)
