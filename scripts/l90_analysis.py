#!/usr/bin/env python3
"""
l90_analysis.py
===============
Pulls last-90-day closed-won deal data from the AC CRM for all active reps
and ranks them by ARR within each (Region, Role) segment, highlighting the
top-25% performers used to calibrate capacity targets.

Usage:
  python scripts/l90_analysis.py
"""

import math
import os
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path


AIRTABLE_BASE  = "app8hrGmXlXqqACc5"
AIRTABLE_TABLE = "tblSw0so9hmBrwgJX"
AC_API_BASE    = "https://ac.api-us1.com"

F_NAME     = "fldxcjeG1myRMPlSN"
F_EMAIL    = "fldRilR8ALqSVefoj"
F_ACTIVE   = "fldonN4GCMmXVdmeE"
F_REGION   = "fld7TfFY09TvACf6i"
F_REP_TYPE = "fldM8blAk6JTqKQZX"


def load_env() -> None:
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def main() -> None:
    load_env()
    airtable_token = os.environ["AIRTABLE_API_TOKEN"]
    ac_api_key     = os.environ["AC_API_KEY"]
    cutoff         = datetime.now(tz=timezone.utc) - timedelta(days=90)

    # Fetch active reps
    headers_at = {"Authorization": f"Bearer {airtable_token}"}
    url        = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    records, offset = [], None
    while True:
        params: dict = {
            "filterByFormula": f"{{{F_ACTIVE}}}=1",
            "returnFieldsByFieldId": "true",
        }
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers_at, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    reps = [
        {
            "name":   r["fields"].get(F_NAME, "?"),
            "email":  r["fields"].get(F_EMAIL, "").lower().strip(),
            "region": r["fields"].get(F_REGION, "Unknown"),
            "role":   r["fields"].get(F_REP_TYPE, "Unknown"),
        }
        for r in records
    ]
    print(f"Active reps: {len(reps)}")

    # Build AC user map
    headers_ac = {"Api-Token": ac_api_key}
    users, off = [], 0
    while True:
        r = requests.get(f"{AC_API_BASE}/api/3/users", headers=headers_ac,
                         params={"limit": 100, "offset": off}, timeout=20)
        r.raise_for_status()
        d = r.json()
        batch = d.get("users", [])
        users.extend(batch)
        total = int(d.get("meta", {}).get("total", 0))
        off += len(batch)
        if not batch or off >= total:
            break
    ac_map = {u["email"].lower(): u["id"] for u in users}
    print(f"AC users loaded: {len(ac_map)}")

    # Per-rep L90 data
    results = []
    for rep in reps:
        uid = ac_map.get(rep["email"])
        if not uid:
            print(f"  WARN: {rep['name']} not in AC CRM")
            results.append({**rep, "deals": 0, "arr": 0.0})
            continue

        deals_all, off = [], 0
        while True:
            r = requests.get(
                f"{AC_API_BASE}/api/3/deals", headers=headers_ac,
                params={"filters[owner]": uid, "filters[status]": 1,
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
        print(f"  {rep['name']:<42} won deals L90: {len(l90):4}  ARR: ${arr:>12,.0f}")
        results.append({**rep, "deals": len(l90), "arr": arr})

    # Segment ranking
    segments: dict = defaultdict(list)
    for r in results:
        segments[(r["region"], r["role"])].append(r)

    print()
    print("═" * 72)
    print("L90 CLOSED ARR BY REGION / ROLE — TOP 25% PERFORMERS")
    print("═" * 72)

    for key in sorted(segments.keys()):
        region, role = key
        group  = sorted(segments[key], key=lambda x: x["arr"], reverse=True)
        n      = len(group)
        top_n  = max(1, math.ceil(n * 0.25))
        seg_avg = sum(r["arr"] for r in group) / n if n else 0
        top_avg = sum(r["arr"] for r in group[:top_n]) / top_n if top_n else 0

        print(f"\n{region} — {role}  ({n} reps)")
        print(f"  Segment avg ARR: ${seg_avg:>10,.0f}   Top 25% avg: ${top_avg:>10,.0f}")
        print(f"  {'Name':<38} {'ARR Closed':>12}  {'Deals':>6}")
        print(f"  {'─'*38} {'─'*12}  {'─'*6}")
        for i, r in enumerate(group):
            star = " ★" if i < top_n else ""
            print(f"  {r['name']:<38} ${r['arr']:>11,.0f}  {r['deals']:>6}{star}")


if __name__ == "__main__":
    main()
