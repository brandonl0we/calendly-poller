#!/usr/bin/env python3
"""
Calendly Capacity Poller
========================
Pulls real available-slot counts from Calendly for every active AMER AE rep,
recalculates three-signal capacity scores, and writes the results back to
Airtable so the round-robin routing engine always has fresh data.

Runs every 30 minutes Mon–Fri via GitHub Actions.

Secrets required (set in repo Settings → Secrets → Actions):
  CALENDLY_API_TOKEN   — Calendly personal access token
  AIRTABLE_API_TOKEN   — Airtable personal access token
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

CALENDLY_TOKEN = os.environ["CALENDLY_API_TOKEN"]
AIRTABLE_TOKEN = os.environ["AIRTABLE_API_TOKEN"]

AIRTABLE_BASE  = "app8hrGmXlXqqACc5"
AIRTABLE_TABLE = "tblSw0so9hmBrwgJX"

# Partial lowercase strings to match inbound event type names
INBOUND_EVENT_NAMES = [
    "activecampaign conversation",
    "activecampaign discussion",
]

# Capacity routing thresholds
TIER_HIGH   = 70
TIER_NORMAL = 30
TIER_LOW    = 10

# Airtable field IDs (do not rename — matches live schema)
F_NAME        = "fldxcjeG1myRMPlSN"
F_EMAIL       = "fldRilR8ALqSVefoj"
F_MAX_DAILY   = "fldhAZJBEeipU0SPG"
F_MAX_WEEKLY  = "fldqWeowrDDW3LhHU"
F_SLOT_WEIGHT = "fld4f57bMUXRMx2tn"
F_LOAD_WEIGHT = "fldVGOfaXeClqWBPG"
F_PIPE_WEIGHT = "fldNWaxZEqeGv73Zd"
F_ACTIVE      = "fldonN4GCMmXVdmeE"
F_AVAIL_SLOTS = "fldXBZkyIVblI0oa5"
F_BOOKED_WK   = "fldD1PzdV2Q5ZPy4L"
F_OPEN_PIPE   = "flde5glHWiirnyRai"
F_SCORE       = "fldqho14Dqv38iZi6"
F_STATUS      = "fldU20rnhGZZn6FeH"
F_SYNCED_AT   = "fldpDhu2Im2MYsIMU"

CT = ZoneInfo("America/Chicago")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Calendly API helpers ───────────────────────────────────────────────────────

def calendly_get(path: str, params: dict = None) -> dict:
    """GET a Calendly v2 endpoint, raise on non-2xx."""
    resp = requests.get(
        f"https://api.calendly.com{path}",
        headers={"Authorization": f"Bearer {CALENDLY_TOKEN}"},
        params=params or {},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def get_org_uri() -> str:
    """Return the authenticated user's organization URI."""
    return calendly_get("/users/me")["resource"]["current_organization"]


def get_user_uri(org_uri: str, email: str):
    """Look up a rep's Calendly user URI by their email address."""
    data = calendly_get(
        "/organization_memberships",
        {"organization": org_uri, "email": email},
    )
    members = data.get("collection", [])
    if not members:
        return None
    return members[0]["user"]["uri"]


def get_inbound_event_type_uris(user_uri: str) -> list[str]:
    """
    Return URIs of active event types whose names match any INBOUND_EVENT_NAMES
    substring (case-insensitive). Both 'ActiveCampaign Conversation' and
    'ActiveCampaign Discussion' qualify.
    """
    data = calendly_get("/event_types", {"user": user_uri, "active": "true"})
    matches = []
    for et in data.get("collection", []):
        name_lower = et.get("name", "").lower()
        if any(inbound in name_lower for inbound in INBOUND_EVENT_NAMES):
            matches.append(et["uri"])
            log.info("    Event type matched: %s", et["name"])
    return matches


def count_available_slots(event_type_uris: list[str], business_days: int = 5) -> int:
    """
    Count unique available booking slots across all inbound event types over
    the next `business_days` weekdays.

    Deduplicates by start_time so that the same calendar opening counted in
    both 'Conversation' and 'Discussion' event types isn't double-counted.
    """
    now  = datetime.now(tz=timezone.utc)
    # Add a 2-day buffer to cover weekends when looking ahead
    end  = now + timedelta(days=business_days + 2)

    seen_times: set[str] = set()

    for et_uri in event_type_uris:
        try:
            data = calendly_get(
                "/event_type_available_times",
                {
                    "event_type": et_uri,
                    "start_time": now.isoformat(),
                    "end_time":   end.isoformat(),
                },
            )
            for slot in data.get("collection", []):
                start_str = slot.get("start_time", "")
                if not start_str:
                    continue
                slot_dt = datetime.fromisoformat(
                    start_str.replace("Z", "+00:00")
                ).astimezone(CT)
                # Skip weekends
                if slot_dt.weekday() >= 5:
                    continue
                seen_times.add(start_str)
        except requests.HTTPError as exc:
            log.warning("    Slot fetch failed for event type URI — %s", exc)

    # Only count slots within the actual business_days window
    # (the buffer is just to ensure we don't cut off Friday)
    bday_count = 0
    bdays_seen: set = set()
    for start_str in sorted(seen_times):
        slot_dt = datetime.fromisoformat(
            start_str.replace("Z", "+00:00")
        ).astimezone(CT)
        bdays_seen.add(slot_dt.date())
        if len(bdays_seen) <= business_days:
            bday_count += 1

    return bday_count


# ── Airtable helpers ───────────────────────────────────────────────────────────

def airtable_get_active_reps() -> list[dict]:
    """Fetch all Airtable rep records where Active in Rotation is checked."""
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    records, offset = [], None

    while True:
        params: dict = {"filterByFormula": f"{{{F_ACTIVE}}}=1", "returnFieldsByFieldId": "true"}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return records


def airtable_update_record(record_id: str, fields: dict) -> None:
    """PATCH a single Airtable record."""
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type":  "application/json",
    }
    url  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}/{record_id}"
    resp = requests.patch(url, headers=headers, json={"fields": fields}, timeout=20)
    resp.raise_for_status()


# ── Scoring ────────────────────────────────────────────────────────────────────

def compute_capacity_score(
    avail_slots: int,
    max_daily:   int,
    booked_wk:   int,
    max_wk:      int,
    open_pipe:   int,
    slot_w:      float,
    load_w:      float,
    pipe_w:      float,
) -> tuple[float, float, float, float]:
    """
    Returns (capacity_score, slot_score, load_score, pipe_score).

    slot_score  — fraction of max weekly slot capacity currently open
    load_score  — fraction of max weekly meetings not yet booked
    pipe_score  — 100 when no CRM data; replace with real value when available
    """
    capacity_slots = max_daily * 5  # 5 business days
    slot_score = min(100.0, (avail_slots / capacity_slots) * 100) if capacity_slots else 0.0
    load_score = min(100.0, max(0.0, (1 - booked_wk / max_wk) * 100)) if max_wk else 0.0
    pipe_score = 100.0  # TODO: swap for live CRM open-deal count when integrated

    score = round(slot_score * slot_w + load_score * load_w + pipe_score * pipe_w, 1)
    return score, slot_score, load_score, pipe_score


def routing_status(score: float) -> str:
    if score >= TIER_HIGH:
        return "High Capacity"
    if score >= TIER_NORMAL:
        return "Normal Capacity"
    if score >= TIER_LOW:
        return "Low Capacity"
    return "Unavailable"


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("════ Calendly Capacity Sync ════")
    log.info("Fetching Calendly org URI ...")
    org_uri = get_org_uri()
    log.info("Org URI: %s", org_uri)

    log.info("Fetching active reps from Airtable ...")
    reps = airtable_get_active_reps()
    log.info("Active reps: %d", len(reps))

    now_iso = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    warnings: list[str] = []

    for rec in reps:
        f    = rec["fields"]
        name = f.get(F_NAME, "Unknown")
        email = f.get(F_EMAIL, "")

        log.info("── %s (%s)", name, email)

        if not email:
            msg = f"{name}: no email — skipped"
            log.warning("  %s", msg)
            warnings.append(msg)
            continue

        # 1. Resolve Calendly user URI
        user_uri = get_user_uri(org_uri, email)
        if not user_uri:
            msg = f"{name}: not found in Calendly org"
            log.warning("  %s", msg)
            warnings.append(msg)
            continue

        # 2. Find inbound event type URIs
        et_uris = get_inbound_event_type_uris(user_uri)
        if not et_uris:
            msg = f"{name}: no matching inbound event types"
            log.warning("  %s", msg)
            warnings.append(msg)
            continue

        # 3. Count unique available slots over next 5 business days
        avail_slots = count_available_slots(et_uris, business_days=5)
        log.info("  Available slots (5d): %d", avail_slots)

        # 4. Calculate score
        max_daily  = int(f.get(F_MAX_DAILY, 2))
        max_wk     = int(f.get(F_MAX_WEEKLY, 10))
        booked_wk  = int(f.get(F_BOOKED_WK, 0))
        open_pipe  = int(f.get(F_OPEN_PIPE, 0))
        slot_w     = float(f.get(F_SLOT_WEIGHT, 0.40))
        load_w     = float(f.get(F_LOAD_WEIGHT, 0.35))
        pipe_w     = float(f.get(F_PIPE_WEIGHT, 0.25))

        score, slot_s, load_s, pipe_s = compute_capacity_score(
            avail_slots, max_daily, booked_wk, max_wk, open_pipe,
            slot_w, load_w, pipe_w,
        )
        status = routing_status(score)
        log.info(
            "  SlotScore=%.1f  LoadScore=%.1f  PipeScore=%.1f  → Score=%.1f  [%s]",
            slot_s, load_s, pipe_s, score, status,
        )

        # 5. Write back to Airtable
        airtable_update_record(rec["id"], {
            F_AVAIL_SLOTS: avail_slots,
            F_SCORE:       score,
            F_STATUS:      status,
            F_SYNCED_AT:   now_iso,
        })
        log.info("  ✓ Airtable updated")

    # Summary
    log.info("════ Sync complete — %d reps processed, %d warning(s) ════",
             len(reps), len(warnings))
    if warnings:
        for w in warnings:
            log.warning("  • %s", w)
        sys.exit(1)  # non-zero exit flags the Action as failed for visibility


if __name__ == "__main__":
    main()
