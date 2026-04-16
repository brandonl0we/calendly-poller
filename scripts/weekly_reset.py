#!/usr/bin/env python3
"""
Weekly Booked-This-Week Reset
==============================
Runs every Monday at 6:00 AM CT via GitHub Actions.
Zeroes out the "Booked This Week" field for every active rep so the
load score reflects the current week's bookings only.

After reset, the capacity sync runs automatically (also scheduled Mon 6:30 AM CT)
to recompute scores with fresh weekly data.

Secrets required:
  AIRTABLE_API_TOKEN   — Airtable personal access token
"""

import os
import logging

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

AIRTABLE_TOKEN = os.environ["AIRTABLE_API_TOKEN"]
AIRTABLE_BASE  = "app8hrGmXlXqqACc5"
AIRTABLE_TABLE = "tblSw0so9hmBrwgJX"

F_NAME      = "fldxcjeG1myRMPlSN"
F_ACTIVE    = "fldonN4GCMmXVdmeE"
F_BOOKED_WK = "fldD1PzdV2Q5ZPy4L"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Airtable helpers ───────────────────────────────────────────────────────────

def airtable_get_active_reps() -> list[dict]:
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    records, offset = [], None
    while True:
        params: dict = {"filterByFormula": f"{{{F_ACTIVE}}}=1"}
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
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type":  "application/json",
    }
    url  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}/{record_id}"
    resp = requests.patch(url, headers=headers, json={"fields": fields}, timeout=20)
    resp.raise_for_status()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("════ Weekly Reset: Booked This Week → 0 ════")
    reps = airtable_get_active_reps()
    log.info("Active reps to reset: %d", len(reps))

    for rec in reps:
        name = rec["fields"].get(F_NAME, rec["id"])
        airtable_update_record(rec["id"], {F_BOOKED_WK: 0})
        log.info("  ✓ Reset: %s", name)

    log.info("════ Reset complete ════")


if __name__ == "__main__":
    main()
