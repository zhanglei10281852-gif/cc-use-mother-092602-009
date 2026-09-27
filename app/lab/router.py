from __future__ import annotations

from fastapi import APIRouter, Query, Response

from app.lab.schemas import ImportRequest, RecomputeRequest
from app.lab.service import LabPipelineService

router = APIRouter(prefix="/api/lab", tags=["实验室测试数据管线"])


def service() -> LabPipelineService:
    return LabPipelineService()


@router.post("/imports", status_code=201)
def import_batch(payload: ImportRequest, response: Response):
    summary, created = service().import_batch(payload.model_dump())
    if not created:
        response.status_code = 200
    return summary


@router.get("/batches")
def list_batches(source: str | None = None, limit: int = Query(default=100, ge=1, le=500)):
    return {"items": service().list_batches(source=source, limit=limit)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int):
    return service().get_batch(batch_id)


@router.get("/readings")
def list_readings(
    payload: str | None = None,
    cycle_phase: str | None = None,
    model_version: str | None = None,
    current_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
):
    return {
        "items": service().list_readings(
            payload=payload, cycle_phase=cycle_phase, model_version=model_version, current_only=current_only, limit=limit
        )
    }


@router.get("/aggregates")
def current_aggregates(payload: str | None = None, cycle_phase: str | None = None, model_version: str | None = None):
    return service().current_aggregates(payload=payload, cycle_phase=cycle_phase, model_version=model_version)


@router.get("/aggregations/runs")
def list_runs(limit: int = Query(default=50, ge=1, le=200)):
    return {"items": service().list_runs(limit=limit)}


@router.get("/aggregations/runs/{run_id}")
def get_run(run_id: int):
    return service().get_run(run_id)


@router.post("/aggregations/recompute")
def recompute(payload: RecomputeRequest):
    return service().recompute(payload.rule_version, payload.actor)


@router.get("/anomalies")
def explain_anomalies(
    run_id: int | None = None,
    payload: str | None = None,
    cycle_phase: str | None = None,
    model_version: str | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
):
    return service().explain_anomalies(
        run_id=run_id, payload=payload, cycle_phase=cycle_phase, model_version=model_version, limit=limit
    )


@router.get("/rules")
def list_rules():
    return {"items": service().list_rules()}
