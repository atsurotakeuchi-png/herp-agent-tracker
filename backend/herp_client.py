"""HERP Hire API client - fetch candidacies and map to agent funnel data."""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from collections import defaultdict
from typing import Optional
import httpx
from backend.config import HERP_API_TOKEN, HERP_BASE_URL

logger = logging.getLogger(__name__)

# ===== Stage mapping: HERP step name -> our funnel key =====
# HERP uses English step names
STAGE_MAP = {
    "entry": "rec",
    "casualInterview": "i1",
    "firstInterview": "i1",
    "secondInterview": "i2",
    "thirdInterview": "i2",
    "finalInterview": "i3",
    "offer": "offer",
    "offerAccepted": "accept",
    "joined": "accept",
}

STAGE_ORDER = ["rec", "i1", "i2", "i3", "offer", "accept"]


def detect_stage(candidacy: dict) -> str:
    """Detect the highest funnel stage a candidacy has reached."""
    step = candidacy.get("step", "")
    if step in STAGE_MAP:
        return STAGE_MAP[step]

    # Fallback: check status
    status = candidacy.get("status", "")
    if status == "terminated":
        # Still count as at least rec
        return "rec"

    return "rec"


def get_agent_name(candidacy: dict) -> Optional[str]:
    """Extract recruitment agent company name. Returns None if not agent channel."""
    channel = candidacy.get("channel", {})
    if channel.get("type") != "agent":
        return None
    agent = channel.get("agent", {})
    return agent.get("company", "").strip() or None


def get_application_date(candidacy: dict) -> Optional[str]:
    """Extract application date as YYYY-MM-DD string."""
    val = candidacy.get("appliedAt", "")
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


async def fetch_all_candidacies() -> list:
    """Fetch all candidacies with pagination using hasNextPage."""
    headers = {"Authorization": f"Bearer {HERP_API_TOKEN}"}
    base = HERP_BASE_URL
    all_items = []
    page = 1
    per_page = 100

    async with httpx.AsyncClient() as client:
        while True:
            params = {"page": page, "per_page": per_page}
            try:
                resp = await client.get(
                    f"{base}/candidacies",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
            except httpx.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                break

            if resp.status_code == 429:
                logger.warning("Rate limited, waiting 60s...")
                await asyncio.sleep(60)
                continue

            if resp.status_code != 200:
                logger.error(f"API error {resp.status_code}: {resp.text[:200]}")
                break

            data = resp.json()
            items = data.get("candidacies", [])
            if not items:
                break

            all_items.extend(items)
            logger.info(f"  Page {page}: {len(items)} candidacies (total: {len(all_items)})")

            if not data.get("hasNextPage", False):
                break

            page += 1

    logger.info(f"Fetched {len(all_items)} total candidacies")
    return all_items


async def fetch_all_requisitions() -> list:
    """Fetch all requisitions (job postings) from HERP."""
    headers = {"Authorization": f"Bearer {HERP_API_TOKEN}"}
    base = HERP_BASE_URL
    all_items = []
    page = 1

    async with httpx.AsyncClient() as client:
        while True:
            params = {"page": page, "per_page": 100}
            try:
                resp = await client.get(
                    f"{base}/requisitions",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
            except httpx.HTTPError as e:
                logger.error(f"HTTP error fetching requisitions: {e}")
                break

            if resp.status_code != 200:
                logger.error(f"Requisitions API error {resp.status_code}")
                break

            data = resp.json()
            items = data.get("requisitions", [])
            if not items:
                break

            all_items.extend(items)
            if not data.get("hasNextPage", False):
                break
            page += 1

    logger.info(f"Fetched {len(all_items)} requisitions")
    return all_items


def aggregate_by_agent_date_req(candidacies: list) -> dict:
    """
    Process raw candidacies into:
    { (agent_name, date, requisition_id): { rec, i1, i2, i3, offer, accept } }

    Only includes agent-channel candidacies.
    Each candidacy increments ALL stages up to its current stage.
    Keyed by (agent, date, requisition_id) so we can filter by requisition at query time.
    """
    result = defaultdict(lambda: {"rec": 0, "i1": 0, "i2": 0, "i3": 0, "offer": 0, "accept": 0})

    skipped = 0
    for c in candidacies:
        agent = get_agent_name(c)
        if not agent:
            skipped += 1
            continue

        date = get_application_date(c)
        if not date:
            continue

        req_id = c.get("requisitionId", "") or ""
        stage = detect_stage(c)
        stage_idx = STAGE_ORDER.index(stage) if stage in STAGE_ORDER else 0

        key = (agent, date, req_id)
        for i in range(stage_idx + 1):
            result[key][STAGE_ORDER[i]] += 1

    logger.info(f"Aggregated {len(result)} agent-date-req combos ({skipped} non-agent candidacies skipped)")
    return dict(result)
