#!/usr/bin/env python3
"""
calibrate_capacity.py
=====================
Uses L90 closed-won deal data from the AC CRM to compute per-segment
capacity benchmarks and (optionally) write updated Max Daily / Max Weekly
targets back to Airtable.

Also prints recommended PIPELINE_ARR_CEILING values per role to paste
into calendly_sync.py.

Methodology
-----------
1. Fetch all active reps from Airtable (name, email, region, role).
2. Build AC CRM email → user_id map.
3. For each rep, pull won deals and filter to the last 90 days client-side.
4. Group by (Region, Role); rank by ARR; identify top-25% performers.
5. From top-25% deal COUNTS, derive a weekly volume target:
       max_weekly = round(top25_avg_deals / WEEKS_IN_QUARTER)
       max_daily  = max(1, ceil(max_weekly / 5))
6. PIPELINE_ARR_CEILING suggestion = top-25% avg quarterly ARR × ARR_MULTIPLE
   (open pipeline is typically 1–2× quarterly close rate).

Usage
-----
  python scripts/calibrate_capacity.py           # dry run — print only
  python scripts/calibrate_capacity.py --write   # write to Airtable
"""

import math
import os
import sys
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

WEEKS_IN_QUARTER = 13
ARR_MULTIPLE     = 1.5   # open pipeline ≈ 1.5× quarterly close rate
MIN_MAX_WEEKLY   = 3     # floor — every rep gets at least 3 leads/week
MIN_MAX_DAILY    = 1     # floor — at least 1/day

AIRTABLE_BASE  = "app8hrGmXlXqqACc5"
AIRTABLE_TABLE = "tblSw0so9hmBrwgJX"
AC_API_BASE    = "https://ac.api-us1.com"

F_NAME       = "fldxcjeG1myRMPlSN"
F_EMAIL      = "fldRilR8ALqSVefoj"
F_ACTIVE     = "fldonN4GCMmXVdmeE"
F_REGION     = "fld7TfFY09TvACf6i"
F_REP_TYPE   = "fldM8blAk6JTqKQZX"
F_MAX_DAILY  = "fldhAZJBEeipU0SPG"
F_MAX_WEEKLY = "fldqWeowrDDW3LhHU"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_env() -> None:
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def airtable_get_active_reps(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    records, offset = [], None
    while True:
        params: dict = {
            "filterByFormula": f"{{{F_ACTIVE}}}=1",
            "returnFieldsByFieldId": "true",
        }
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


def airtable_update(token: str, record_id: str, fields: dict) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    url  = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}/{record_id}"
    resp = requests.patch(url, headers=headers, json={"fields": fields}, timeout=20)
    resp.raise_for_status()


def build_ac_user_map(api_key: str) -> dict[str, str]:
    headers = {"Api-Token": api_key}
    users, offset = [], 0
    while True:
        r = requests.get(f"{AC_API_BASE}/api/3/users", headers=headers,
                         params={"limit": 100, "offset": offset}, timeout=20)
        r.raise_for_status()
        d = r.json()
        batch = d.get("users", [])
        users.extend(batch)
        total = int(d.get("meta", {}).get("total", 0))
        offset += len(batch)
        if not batch or offset >= total:
            break
    return {u["email"].lower(): u["id"] for u in users}


def get_l90_won_deals(api_key: str, user_id: str, cutoff: datetime) -> tuple[int, float]:
    """Return (deal_count, total_arr) for won deals closed in the last 90 days."""
    headers = {"Api-Token": api_key}
    deals_all, off = [], 0
    while True:
        r = requests.get(
            f"{AC_API_BASE}/api/3/deals", headers=headers,
            params={"filters[owner]": user_id, "filters[status]": 1,
                    "limit": 100, "offset": off},
            timeout=30,
        )
        r.raise_for_status()
        d = r.json()
        batch = d.get("deals", [])
        deals_all.extend(batch)
        total = int(d.get("meta", {}).get("total", 0))
        off += len(batch)
        if not batch or off >= total:
            break

    l90 = []
    for deal in deals_all:
        mdate_str = deal.get("mdate", "")
        if not mdate_str:
            continue
        try:
            mdate = datetime.fromisoformat(mdate_str.replace("Z", "+00:00"))
            if mdate >= cutoff:
                l90.append(deal)
        except ValueError:
            continue

    arr = sum(int(d.get("value", 0)) for d in l90) / 100.0
    return len(l90), arr


# ── Main ──────────────────────────────────────────────────────────────────────

def main(write: bool = False) -> None:
    load_env()
    airtable_token = os.environ["AIRTABLE_API_TOKEN"]
    ac_api_key     = os.environ["AC_API_KEY"]
    cutoff         = datetime.now(tz=timezone.utc) - timedelta(days=90)

    print("Fetching active reps from Airtable ...")
    records = airtable_get_active_reps(airtable_token)
    print(f"  {len(records)} active reps")

    print("Building AC CRM user map ...")
    ac_map = build_ac_user_map(ac_api_key)
    print(f"  {len(ac_map)} AC users loaded")

    # Collect L90 data per rep
    rep_data = []
    for rec in records:
        f     = rec["fields"]
        name  = f.get(F_NAME, "?")
        email = f.get(F_EMAIL, "").lower().strip()
        region = f.get(F_REGION, "Unknown")
        role   = f.get(F_REP_TYPE, "AE")
        cur_daily  = int(f.get(F_MAX_DAILY, 2))
        cur_weekly = int(f.get(F_MAX_WEEKLY, 10))

        uid = ac_map.get(email)
        if uid:
            deals, arr = get_l90_won_deals(ac_api_key, uid, cutoff)
        else:
            deals, arr = 0, 0.0
            print(f"  WARN: {name} not found in AC CRM")

        rep_data.append({
            "id":         rec["id"],
            "name":       name,
            "email":      email,
            "region":     region,
            "role":       role,
            "deals":      deals,
            "arr":        arr,
            "cur_daily":  cur_daily,
            "cur_weekly": cur_weekly,
        })
        print(f"  {name:<42} {region:<6} {role:<4}  deals={deals:3}  ARR=${arr:>10,.0f}")

    # Segment analysis
    segments: dict = defaultdict(list)
    for r in rep_data:
        segments[(r["region"], r["role"])].append(r)

    # Per-segment: compute top-25% benchmarks
    seg_benchmarks: dict = {}
    for key, group in segments.items():
        sorted_g = sorted(group, key=lambda x: x["arr"], reverse=True)
        n        = len(sorted_g)
        top_n    = max(1, math.ceil(n * 0.25))
        top25    = sorted_g[:top_n]
        top25_avg_deals = sum(r["deals"] for r in top25) / top_n
        top25_avg_arr   = sum(r["arr"]   for r in top25) / top_n

        # Capacity targets derived from top-25% deal volume
        max_weekly = max(MIN_MAX_WEEKLY, round(top25_avg_deals / WEEKS_IN_QUARTER))
        max_daily  = max(MIN_MAX_DAILY,  math.ceil(max_weekly / 5))

        seg_benchmarks[key] = {
            "top25_avg_deals": top25_avg_deals,
            "top25_avg_arr":   top25_avg_arr,
            "max_weekly":      max_weekly,
            "max_daily":       max_daily,
            "arr_ceiling":     round(top25_avg_arr * ARR_MULTIPLE / 50_000) * 50_000,
        }

    # Print segment summary
    print()
    print("═" * 72)
    print("SEGMENT BENCHMARKS")
    print("═" * 72)
    for (region, role), b in sorted(seg_benchmarks.items()):
        print(f"\n{region} — {role}")
        print(f"  Top-25% avg deals/qtr : {b['top25_avg_deals']:.1f}")
        print(f"  Top-25% avg ARR/qtr   : ${b['top25_avg_arr']:>10,.0f}")
        print(f"  → Max Weekly          : {b['max_weekly']}")
        print(f"  → Max Daily           : {b['max_daily']}")
        print(f"  → ARR ceiling suggest : ${b['arr_ceiling']:>10,.0f}")

    # Derive role-level ARR ceilings (max across segments for that role)
    role_ceilings: dict = defaultdict(list)
    for (region, role), b in seg_benchmarks.items():
        role_ceilings[role].append(b["arr_ceiling"])
    print()
    print("═" * 72)
    print("SUGGESTED PIPELINE_ARR_CEILING for calendly_sync.py")
    print("═" * 72)
    for role, ceilings in sorted(role_ceilings.items()):
        suggested = round(max(ceilings) / 50_000) * 50_000
        print(f"  {role:<4}: ${suggested:>12,.0f}")

    # Print per-rep proposed changes
    print()
    print("═" * 72)
    print("PER-REP CAPACITY TARGETS  (cur → proposed)")
    print("═" * 72)
    changes = []
    for r in sorted(rep_data, key=lambda x: (x["region"], x["role"], x["name"])):
        key = (r["region"], r["role"])
        b   = seg_benchmarks[key]
        new_daily  = b["max_daily"]
        new_weekly = b["max_weekly"]
        changed = (new_daily != r["cur_daily"]) or (new_weekly != r["cur_weekly"])
        marker = " ←" if changed else ""
        print(
            f"  {r['name']:<42} {r['region']:<8} {r['role']:<4}"
            f"  daily {r['cur_daily']}→{new_daily}  weekly {r['cur_weekly']}→{new_weekly}{marker}"
        )
        if changed:
            changes.append((r["id"], new_daily, new_weekly))

    print(f"\n{len(changes)} rep(s) would change.")

    if not write:
        print("\nDry run — pass --write to apply changes to Airtable.")
        return

    print("\nWriting to Airtable ...")
    for record_id, new_daily, new_weekly in changes:
        airtable_update(airtable_token, record_id, {
            F_MAX_DAILY:  new_daily,
            F_MAX_WEEKLY: new_weekly,
        })
        print(f"  ✓ {record_id}")
    print("Done.")


if __name__ == "__main__":
    main(write="--write" in sys.argv)
