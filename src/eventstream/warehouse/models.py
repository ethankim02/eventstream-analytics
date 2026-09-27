"""Ordered SQL model manifest.

Each model is a `.sql` file containing a single `CREATE OR REPLACE TABLE`
statement. Order matters: later models depend on earlier ones. Keeping this
list explicit (rather than inferring dependency order from file contents)
keeps the build deterministic and easy for a reviewer to audit.
"""

from __future__ import annotations

STAGING_MODELS = [
    "staging/stg_transfers.sql",
    "staging/stg_wallet_events.sql",
]

INTERMEDIATE_MODELS = [
    "intermediate/int_wallet_first_seen.sql",
    "intermediate/int_wallet_daily_activity.sql",
    "intermediate/int_wallet_activity_weeks.sql",
    "intermediate/int_wallet_lifecycle.sql",
    "intermediate/int_wallet_funnel_flags.sql",
    "intermediate/int_wallet_activation.sql",
]

MART_MODELS = [
    "marts/mart_daily_metrics.sql",
    "marts/mart_weekly_metrics.sql",
    "marts/mart_wallet_segments.sql",
    "marts/mart_retention_cohorts.sql",
    "marts/mart_lifecycle_funnel.sql",
    "marts/mart_concentration.sql",
    "marts/mart_concentration_by_week.sql",
    "marts/mart_anomalies.sql",
]

ALL_MODELS = STAGING_MODELS + INTERMEDIATE_MODELS + MART_MODELS

# Same pipeline with `stg_transfers` built without the global dedup window. Only valid for a
# source already proven unique on (transaction_hash, log_index) — see `stg_transfers_disjoint.sql`.
DISJOINT_MODELS = [
    "staging/stg_transfers_disjoint.sql" if m == "staging/stg_transfers.sql" else m
    for m in ALL_MODELS
]
