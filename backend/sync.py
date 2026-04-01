"""Orchestrator: fetch from HERP API -> map to agents -> upsert into DB."""
import logging
from backend.herp_client import fetch_all_candidacies, fetch_all_requisitions, aggregate_by_agent_date_req
from backend.database import (
    upsert_agent, upsert_daily_funnel, get_all_agents,
    upsert_requisition,
)

logger = logging.getLogger(__name__)


async def run_sync() -> dict:
    """
    Full sync: fetch all candidacies from HERP, aggregate by agent/date,
    and upsert into the database.

    Returns a summary of what was synced.
    """
    logger.info("Starting HERP sync...")

    # 1. Sync requisitions from HERP
    reqs = await fetch_all_requisitions()
    for r in reqs:
        upsert_requisition(
            req_id=r["id"],
            name=r.get("name", ""),
            is_archived=1 if r.get("isArchived") else 0,
        )
    logger.info(f"Synced {len(reqs)} requisitions")

    # 2. Fetch candidacies from HERP API
    candidacies = await fetch_all_candidacies()
    if not candidacies:
        return {"status": "no_data", "candidacies": 0, "agents": 0, "days": 0}

    # 3. Aggregate by (agent_name, date, requisition_id) — store ALL data, filter at query time
    aggregated = aggregate_by_agent_date_req(candidacies)
    logger.info(f"Aggregated into {len(aggregated)} agent-date-req combinations")

    # 4. Ensure all agents exist in DB
    agent_names = set(agent for (agent, _, _) in aggregated.keys())
    agent_map = {}  # name -> id

    existing = {a["name"]: a for a in get_all_agents()}

    for name in agent_names:
        if name in existing:
            agent_map[name] = existing[name]["id"]
        else:
            agent = upsert_agent(name=name, contact="", tier=2)
            agent_map[name] = agent["id"]
            logger.info(f"New agent registered: {name} (id={agent['id']})")

    # 5. Upsert daily funnel data (per requisition)
    days_set = set()
    for (agent_name, date, req_id), funnel in aggregated.items():
        agent_id = agent_map.get(agent_name)
        if not agent_id:
            continue
        upsert_daily_funnel(
            agent_id=agent_id,
            date=date,
            requisition_id=req_id,
            rec=funnel["rec"],
            i1=funnel["i1"],
            i2=funnel["i2"],
            i3=funnel["i3"],
            offer=funnel["offer"],
            accept=funnel["accept"],
        )
        days_set.add(date)

    summary = {
        "status": "ok",
        "candidacies": len(candidacies),
        "agents": len(agent_names),
        "days": len(days_set),
        "date_range": f"{min(days_set)} ~ {max(days_set)}" if days_set else "",
    }
    logger.info(f"Sync complete: {summary}")
    return summary
