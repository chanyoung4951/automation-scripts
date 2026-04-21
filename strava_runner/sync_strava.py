from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PARQUET_DIR = DATA_DIR / "parquet" / "activities"
STATE_FILE = DATA_DIR / "state" / "strava_sync_state.json"

TOKEN_URL = "https://www.strava.com/oauth/token"
ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"


def _ensure_dirs() -> None:
    (DATA_DIR / "state").mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def _read_state() -> dict:
    if not STATE_FILE.exists():
        return {"last_sync_unix": 0}
    return json.loads(STATE_FILE.read_text())


def _write_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _refresh_access_token() -> str:
    payload = {
        "client_id": os.environ["STRAVA_CLIENT_ID"],
        "client_secret": os.environ["STRAVA_CLIENT_SECRET"],
        "refresh_token": os.environ["STRAVA_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }
    response = requests.post(TOKEN_URL, data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]


def _fetch_activities(access_token: str, after_ts: int, page_size: int = 200) -> list[dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    page = 1
    out: list[dict] = []

    while True:
        params = {"after": after_ts, "per_page": page_size, "page": page}
        r = requests.get(ACTIVITIES_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        out.extend(rows)
        if len(rows) < page_size:
            break
        page += 1
        time.sleep(0.2)

    return out


def _normalize(activities: list[dict]) -> pd.DataFrame:
    if not activities:
        return pd.DataFrame()

    rows = []
    for a in activities:
        start_local = a.get("start_date_local")
        dt = datetime.fromisoformat(start_local.replace("Z", "+00:00")) if start_local else None
        rows.append(
            {
                "activity_id": a.get("id"),
                "name": a.get("name"),
                "sport_type": a.get("sport_type"),
                "distance_m": a.get("distance"),
                "moving_time_s": a.get("moving_time"),
                "elapsed_time_s": a.get("elapsed_time"),
                "total_elevation_gain_m": a.get("total_elevation_gain"),
                "average_speed_mps": a.get("average_speed"),
                "max_speed_mps": a.get("max_speed"),
                "average_heartrate": a.get("average_heartrate"),
                "max_heartrate": a.get("max_heartrate"),
                "start_date": a.get("start_date"),
                "start_date_local": start_local,
                "year": dt.year if dt else None,
                "month": dt.month if dt else None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["activity_id"], keep="last")


def _write_partitioned_parquet(df: pd.DataFrame) -> None:
    for (year, month), chunk in df.groupby(["year", "month"], dropna=True):
        if pd.isna(year) or pd.isna(month):
            continue
        out_dir = PARQUET_DIR / f"year={int(year)}" / f"month={int(month):02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{uuid.uuid4().hex[:8]}.parquet"
        chunk.to_parquet(out_path, index=False)


def main() -> None:
    load_dotenv()
    _ensure_dirs()

    required = ["STRAVA_CLIENT_ID", "STRAVA_CLIENT_SECRET", "STRAVA_REFRESH_TOKEN"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

    state = _read_state()
    last_sync = int(state.get("last_sync_unix", 0))

    access_token = os.environ.get("STRAVA_ACCESS_TOKEN") or _refresh_access_token()
    page_size = int(os.environ.get("STRAVA_PAGE_SIZE", "200"))

    activities = _fetch_activities(access_token=access_token, after_ts=last_sync, page_size=page_size)
    df = _normalize(activities)

    if not df.empty:
        _write_partitioned_parquet(df)

    state["last_sync_unix"] = int(datetime.now(timezone.utc).timestamp())
    state["last_sync_iso"] = datetime.now(timezone.utc).isoformat()
    state["rows_last_run"] = int(len(df))
    _write_state(state)

    print(f"Synced {len(df)} activities")


if __name__ == "__main__":
    main()
