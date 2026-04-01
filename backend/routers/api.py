"""Main API router: funnel data, agents, targets, sync."""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
from backend.routers.auth import verify_token
from backend.database import (
    get_all_agents, upsert_agent, query_funnel_range, get_day_count,
    get_target, upsert_target, get_daily_raw,
    get_all_requisitions, set_requisition_enabled, get_enabled_requisition_ids,
)
from backend.sync import run_sync

router = APIRouter(prefix="/api", tags=["data"])


# ===== Models =====

class AgentUpdate(BaseModel):
    name: str
    contact: str = ""
    tier: int = 2
    herp_source: str = ""


class RequisitionToggle(BaseModel):
    id: str
    enabled: int  # 0 or 1


class TargetUpdate(BaseModel):
    year_month: str  # "2026-04"
    rec: int = 0
    i1: int = 0
    i2: int = 0
    i3: int = 0
    offer: int = 0
    accept: int = 0


# ===== Endpoints =====

@router.get("/agents")
def list_agents(_=Depends(verify_token)):
    return get_all_agents()


@router.post("/agents")
def create_or_update_agent(agent: AgentUpdate, _=Depends(verify_token)):
    return upsert_agent(
        name=agent.name,
        contact=agent.contact,
        tier=agent.tier,
        herp_source=agent.herp_source,
    )


@router.get("/funnel")
def get_funnel(
    date_from: str = Query(..., alias="from", description="Start date YYYY-MM-DD"),
    date_to: str = Query(..., alias="to", description="End date YYYY-MM-DD"),
    _=Depends(verify_token),
):
    """
    Return aggregated funnel data for the given date range.
    Response matches the frontend's expected structure.
    """
    # Filter by enabled requisitions (if any are enabled)
    req_ids = get_enabled_requisition_ids() or None
    rows = query_funnel_range(date_from, date_to, req_ids)
    day_count = get_day_count(date_from, date_to, req_ids)

    # Build agents dict keyed by agent_id
    agents_data = {}
    for r in rows:
        agents_data[str(r["agent_id"])] = {
            "rec": r["rec"],
            "i1": r["i1"],
            "i2": r["i2"],
            "i3": r["i3"],
            "offer": r["offer"],
            "accept": r["accept"],
        }

    # Get target for the month of date_from
    ym = date_from[:7]  # "2026-04"
    target = get_target(ym)
    target_data = None
    if target:
        target_data = {
            "rec": target["rec"],
            "i1": target["i1"],
            "i2": target["i2"],
            "i3": target["i3"],
            "offer": target["offer"],
            "accept": target["accept"],
        }

    return {
        "agents": agents_data,
        "dayCount": day_count,
        "targets": target_data,
        "dateRange": {"from": date_from, "to": date_to},
    }


@router.get("/targets/{year_month}")
def get_monthly_target(year_month: str, _=Depends(verify_token)):
    target = get_target(year_month)
    if not target:
        return {"year_month": year_month, "rec": 0, "i1": 0, "i2": 0, "i3": 0, "offer": 0, "accept": 0}
    return target


@router.post("/targets")
def set_target(t: TargetUpdate, _=Depends(verify_token)):
    upsert_target(
        year_month=t.year_month,
        rec=t.rec, i1=t.i1, i2=t.i2,
        i3=t.i3, offer=t.offer, accept=t.accept,
    )
    return {"status": "ok", "year_month": t.year_month}


@router.get("/requisitions")
def list_requisitions(_=Depends(verify_token)):
    return get_all_requisitions()


@router.post("/requisitions/toggle")
def toggle_requisition(body: RequisitionToggle, _=Depends(verify_token)):
    set_requisition_enabled(body.id, body.enabled)
    return {"status": "ok", "id": body.id, "enabled": body.enabled}


@router.post("/sync")
async def trigger_sync(_=Depends(verify_token)):
    """Manually trigger HERP data sync."""
    result = await run_sync()
    return result


@router.get("/raw")
def get_raw_data(
    date_from: str = Query(..., alias="from"),
    date_to: str = Query(..., alias="to"),
    _=Depends(verify_token),
):
    """Raw daily data for debug/export."""
    return get_daily_raw(date_from, date_to)
