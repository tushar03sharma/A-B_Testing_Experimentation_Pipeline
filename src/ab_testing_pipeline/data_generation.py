from __future__ import annotations

from datetime import datetime, timedelta
from random import Random
from uuid import uuid4

import pandas as pd

from .config import Settings, ensure_directories


COUNTRIES = ["IN", "US", "DE", "GB", "CA"]
DEVICES = ["mobile", "desktop", "tablet"]


def _pick_signup_date(rng: Random) -> datetime:
    start = datetime(2026, 1, 1, 9, 0, 0)
    return start + timedelta(days=rng.randint(0, 27), hours=rng.randint(0, 23))


def _base_conversion_rate(country: str, device_type: str) -> float:
    country_bias = {"IN": 0.085, "US": 0.11, "DE": 0.1, "GB": 0.105, "CA": 0.102}
    device_bias = {"mobile": -0.015, "desktop": 0.012, "tablet": -0.005}
    return country_bias[country] + device_bias[device_type]


def generate_data(
    settings: Settings | None = None,
    num_users: int = 5000,
    seed: int = 7,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = settings or Settings()
    ensure_directories()
    rng = Random(seed)

    users: list[dict[str, object]] = []
    events: list[dict[str, object]] = []

    for user_number in range(1, num_users + 1):
        user_id = f"user_{user_number:05d}"
        country = COUNTRIES[rng.randrange(len(COUNTRIES))]
        device_type = DEVICES[rng.randrange(len(DEVICES))]
        signup_ts = _pick_signup_date(rng)
        variant = "treatment" if rng.random() >= 0.5 else "control"
        session_id = f"session_{uuid4().hex[:12]}"

        users.append(
            {
                "user_id": user_id,
                "signup_ts": signup_ts.isoformat(sep=" "),
                "country": country,
                "device_type": device_type,
                "assigned_variant": variant,
            }
        )

        event_rows = [
            ("signup_completed", signup_ts, None),
            ("app_open", signup_ts + timedelta(minutes=rng.randint(1, 30)), None),
            ("page_view", signup_ts + timedelta(minutes=rng.randint(5, 60)), None),
        ]

        exposed = rng.random() >= 0.08
        exposure_ts = signup_ts + timedelta(hours=rng.randint(1, 72))
        if exposed:
            event_rows.append(("experiment_exposure", exposure_ts, None))
            event_rows.append(
                ("checkout_started", exposure_ts + timedelta(minutes=rng.randint(1, 45)), None)
            )

        conversion_rate = _base_conversion_rate(country, device_type)
        if variant == "treatment":
            conversion_rate += 0.03
        if signup_ts.weekday() in (4, 5):
            conversion_rate += 0.01
        converted = exposed and rng.random() < max(0.01, min(conversion_rate, 0.35))

        if converted:
            purchase_ts = exposure_ts + timedelta(hours=rng.randint(1, settings.lookback_days * 24 - 1))
            revenue = round(rng.uniform(28, 180), 2)
            event_rows.append(("checkout_completed", purchase_ts, revenue))

        for event_name, event_ts, revenue in event_rows:
            events.append(
                {
                    "event_id": uuid4().hex,
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_name": event_name,
                    "event_ts": event_ts.isoformat(sep=" "),
                    "experiment_key": settings.experiment_key if event_name == "experiment_exposure" else None,
                    "variant": variant if event_name == "experiment_exposure" else None,
                    "country": country,
                    "device_type": device_type,
                    "revenue": revenue,
                }
            )

    users_df = pd.DataFrame(users).sort_values("user_id").reset_index(drop=True)
    events_df = pd.DataFrame(events).sort_values(["event_ts", "user_id"]).reset_index(drop=True)

    users_df.to_csv(settings.users_path, index=False)
    events_df.to_json(settings.raw_events_path, orient="records", lines=True)
    return users_df, events_df
