"""Validate Yambda data marts and cross-check them against the raw source."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_DIR / "data" / "processed" / "multi_event_clean.parquet"
DEFAULT_MARTS_DIR = PROJECT_DIR / "data" / "marts"
MAX_MART_SIZE = 50 * 1024 * 1024  # Increased to 50MB because content mart can be large

EXPECTED_COLUMNS = {
    "mart_daily_metrics.parquet": {
        "time_period", "dau", "active_items", "total_events", "listens",
        "likes", "unlikes", "dislikes", "undislikes", "completed_listens",
        "short_listens", "organic_listens", "played_hours",
        "avg_played_ratio_pct", "completion_rate", "short_listen_rate",
        "organic_listen_ratio",
    },
    "mart_event_daily.parquet": {
        "time_period", "event_type", "is_organic", "events", "users", "items"
    },
    "mart_user_segments.parquet": {"time_period", "uid", "segment"},
    "mart_content_health.parquet": {
        "item_id", "listeners", "listens", "total_likes", "dislikes",
        "completed_listens", "short_listens", "organic_listens",
        "algo_listens", "organic_listening", "algo_listening",
        "algo_completed_listens", "organic_completed_listens",
        "algo_short_listens", "organic_short_listens",
        "played_hours", "avg_played_ratio_pct",
        "track_length_seconds", "completion_rate", "short_listen_rate",
        "organic_ratio", "content_tier",
    },
    "mart_user_general.parquet": {
        "time_period", "uid", "organic_listening", "algo_listening",
        "organic_likes", "organic_unlikes", "organic_dislikes",
        "organic_undislikes", "algo_likes", "algo_unlikes",
        "algo_dislikes", "algo_undislikes", "algo_completed_listens",
        "organic_completed_listens", "algo_short_listens",
        "organic_short_listens", "algo_unique_items", "organic_unique_items",
    },
}
CORE_SOURCE_COLUMNS = (
    "uid", "item_id", "time_period", "is_organic", "event_type",
    "played_ratio_pct", "track_length_seconds", "played_seconds_capped",
    "is_listen", "sequence_eligible", "is_bot_session",
    "is_suspected_bot_user",
)


def _collect(plan: pl.LazyFrame) -> pl.DataFrame:
    return plan.collect(engine="streaming")


def _scalar(frame: pl.DataFrame, column: str) -> int | float:
    value = frame.item(0, column)
    if value is None:
        raise AssertionError(f"Validation aggregate {column!r} is null")
    return value


def _check_files(marts_dir: Path) -> None:
    for name, expected in EXPECTED_COLUMNS.items():
        path = marts_dir / name
        if not path.is_file():
            raise FileNotFoundError(f"Missing mart: {path}")
        if path.stat().st_size > MAX_MART_SIZE:
            raise AssertionError(f"{name} exceeds max size: {path.stat().st_size} bytes")
        schema = pl.scan_parquet(path).collect_schema()
        if set(schema.names()) != expected:
            raise AssertionError(
                f"Unexpected columns in {name}: {schema.names()} (expected {sorted(expected)})"
            )


def validate_marts(
    input_path: Path,
    marts_dir: Path,
    verbose: bool = False,
) -> dict:
    input_path = input_path.expanduser().resolve()
    marts_dir = marts_dir.expanduser().resolve()
    _check_files(marts_dir)

    source = pl.scan_parquet(input_path)
    source_schema = source.collect_schema()
    missing_core = set(CORE_SOURCE_COLUMNS).difference(source_schema.names())
    if missing_core:
        raise AssertionError(f"Source is missing core columns: {sorted(missing_core)}")
    
    # Pre-filter source exactly as build_marts_v2 does.
    event_types = ["listen", "like", "unlike", "dislike", "undislike"]
    filtered_source = source.filter(
        pl.col("uid").is_not_null()
        & pl.col("item_id").is_not_null()
        & pl.col("time_period").is_not_null()
        & pl.col("is_organic").is_in([0, 1])
        & pl.col("event_type").cast(pl.String).is_in(event_types)
    )

    daily = pl.scan_parquet(marts_dir / "mart_daily_metrics.parquet")
    event_daily = pl.scan_parquet(marts_dir / "mart_event_daily.parquet")
    users = pl.scan_parquet(marts_dir / "mart_user_segments.parquet")
    content = pl.scan_parquet(marts_dir / "mart_content_health.parquet")
    user_general = pl.scan_parquet(marts_dir / "mart_user_general.parquet")

    source_stats = _collect(
        filtered_source.select(
            pl.len().alias("interactions"),
            pl.col("uid").n_unique().alias("users"),
            pl.col("item_id").n_unique().alias("items"),
        )
    )

    daily_stats = _collect(
        daily.select(
            pl.len().alias("periods"),
            pl.col("time_period").n_unique().alias("unique_periods"),
            pl.col("dau").min().alias("min_dau"),
            pl.col("total_events").sum().alias("interactions"),
        )
    )
    event_daily_stats = _collect(
        event_daily.select(
            pl.len().alias("rows"),
            pl.struct("time_period", "event_type", "is_organic")
            .n_unique()
            .alias("unique_keys"),
            pl.col("events").sum().alias("interactions"),
        )
    )
    user_stats = _collect(
        users.select(
            pl.col("uid").n_unique().alias("unique_users"),
            pl.len().alias("rows"),
            pl.struct("uid", "time_period").n_unique().alias("unique_keys"),
        )
    )
    general_stats = _collect(
        user_general.select(
            pl.len().alias("rows"),
            pl.struct("uid", "time_period").n_unique().alias("unique_keys"),
        )
    )
    source_user_periods = _collect(
        filtered_source.select("uid", "time_period")
        .unique()
        .select(pl.len().alias("rows"))
    )
    missing_segment_keys = _collect(
        user_general.select("uid", "time_period")
        .join(users.select("uid", "time_period"), on=["uid", "time_period"], how="anti")
        .select(pl.len().alias("rows"))
    )
    
    # content_health is grouped by ALL items that appeared in ANY event
    content_stats = _collect(
        content.select(
            pl.len().alias("items"),
            pl.col("item_id").n_unique().alias("unique_items"),
        )
    )

    interactions = int(_scalar(source_stats, "interactions"))
    expected_users = int(_scalar(source_stats, "users"))
    expected_items = int(_scalar(source_stats, "items"))
    
    checks = {
        "daily_nonzero_dau": _scalar(daily_stats, "min_dau") > 0,
        "daily_unique_periods": _scalar(daily_stats, "periods")
        == _scalar(daily_stats, "unique_periods"),
        "daily_interactions_match": _scalar(daily_stats, "interactions")
        == interactions,
        "event_daily_keys_unique": _scalar(event_daily_stats, "rows")
        == _scalar(event_daily_stats, "unique_keys"),
        "event_daily_interactions_match": _scalar(
            event_daily_stats, "interactions"
        )
        == interactions,
        "users_match": _scalar(user_stats, "unique_users") == expected_users,
        "segment_keys_unique": _scalar(user_stats, "rows")
        == _scalar(user_stats, "unique_keys"),
        "user_general_keys_unique": _scalar(general_stats, "rows")
        == _scalar(general_stats, "unique_keys"),
        "user_general_grain_matches": _scalar(general_stats, "rows")
        == _scalar(source_user_periods, "rows"),
        "all_user_general_keys_have_segment": _scalar(missing_segment_keys, "rows") == 0,
        "items_match": _scalar(content_stats, "items")
        == _scalar(content_stats, "unique_items")
        == expected_items,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise AssertionError(f"Failed mart checks: {', '.join(failed)}")

    eligible = (
        pl.col("is_listen")
        & pl.col("sequence_eligible")
        & ~pl.col("is_bot_session")
        & ~pl.col("is_suspected_bot_user")
    )
    completed = eligible & (pl.col("played_ratio_pct").clip(0, 100) >= 80)
    short = eligible & (
        (pl.col("played_seconds_capped") <= 30)
        | (pl.col("played_ratio_pct").clip(0, 100) < 10)
    )
    expected_listening = _collect(
        filtered_source.select(
            eligible.sum().alias("listens"),
            (eligible & (pl.col("is_organic") == 0)).sum().alias("algo_listening"),
            (eligible & (pl.col("is_organic") == 1)).sum().alias("organic_listening"),
            (completed & (pl.col("is_organic") == 0)).sum().alias("algo_completed_listens"),
            (completed & (pl.col("is_organic") == 1)).sum().alias("organic_completed_listens"),
            (short & (pl.col("is_organic") == 0)).sum().alias("algo_short_listens"),
            (short & (pl.col("is_organic") == 1)).sum().alias("organic_short_listens"),
        )
    ).row(0, named=True)
    expected_unique = _collect(
        filtered_source.group_by("uid", "time_period")
        .agg(
            pl.col("item_id")
            .filter(eligible & (pl.col("is_organic") == 0))
            .n_unique()
            .alias("algo_unique_items"),
            pl.col("item_id")
            .filter(eligible & (pl.col("is_organic") == 1))
            .n_unique()
            .alias("organic_unique_items"),
        )
        .select(
            pl.col("algo_unique_items").sum(),
            pl.col("organic_unique_items").sum(),
        )
    ).row(0, named=True)
    user_listening = _collect(
        user_general.select(
            pl.col("algo_listening").sum(),
            pl.col("organic_listening").sum(),
            pl.col("algo_completed_listens").sum(),
            pl.col("organic_completed_listens").sum(),
            pl.col("algo_short_listens").sum(),
            pl.col("organic_short_listens").sum(),
            pl.col("algo_unique_items").sum(),
            pl.col("organic_unique_items").sum(),
            (
                (pl.col("algo_completed_listens") > pl.col("algo_listening"))
                | (pl.col("algo_short_listens") > pl.col("algo_listening"))
                | (pl.col("algo_unique_items") > pl.col("algo_listening"))
                | (pl.col("organic_completed_listens") > pl.col("organic_listening"))
                | (pl.col("organic_short_listens") > pl.col("organic_listening"))
                | (pl.col("organic_unique_items") > pl.col("organic_listening"))
            )
            .sum()
            .alias("bound_violations"),
        )
    ).row(0, named=True)
    content_listening = _collect(
        content.select(
            pl.col("listens").sum(),
            pl.col("algo_listening").sum(),
            pl.col("organic_listening").sum(),
            pl.col("algo_completed_listens").sum(),
            pl.col("organic_completed_listens").sum(),
            pl.col("algo_short_listens").sum(),
            pl.col("organic_short_listens").sum(),
            (pl.col("completed_listens") - pl.col("algo_completed_listens") - pl.col("organic_completed_listens")).abs().max().alias("completed_split_error"),
            (pl.col("short_listens") - pl.col("algo_short_listens") - pl.col("organic_short_listens")).abs().max().alias("short_split_error"),
            (pl.col("algo_listens") - pl.col("algo_listening")).abs().max().alias("algo_alias_error"),
            (pl.col("organic_listens") - pl.col("organic_listening")).abs().max().alias("organic_alias_error"),
        )
    ).row(0, named=True)
    for metric in (
        "algo_listening", "organic_listening", "algo_completed_listens",
        "organic_completed_listens", "algo_short_listens", "organic_short_listens",
    ):
        if user_listening[metric] != expected_listening[metric]:
            raise AssertionError(f"Incorrect {metric} total in mart_user_general")
        if content_listening[metric] != expected_listening[metric]:
            raise AssertionError(f"Incorrect {metric} total in mart_content_health")
    for metric in ("algo_unique_items", "organic_unique_items"):
        if user_listening[metric] != expected_unique[metric]:
            raise AssertionError(f"Incorrect {metric} total in mart_user_general")
    if user_listening["bound_violations"] != 0:
        raise AssertionError("User listening metrics exceed their denominators")
    if content_listening["listens"] != expected_listening["listens"]:
        raise AssertionError("Incorrect eligible listen total in mart_content_health")
    for metric in (
        "completed_split_error", "short_split_error", "algo_alias_error",
        "organic_alias_error",
    ):
        if content_listening[metric] != 0:
            raise AssertionError(f"Content split invariant failed: {metric}")

    for mart_name, plan, rate_columns in (
        (
            "mart_daily_metrics",
            daily,
            ("completion_rate", "short_listen_rate", "organic_listen_ratio"),
        ),
        (
            "mart_content_health",
            content,
            ("completion_rate", "short_listen_rate", "organic_ratio"),
        ),
    ):
        violations = _collect(
            plan.select(
                pl.any_horizontal(
                    *(
                        pl.col(column).is_not_null()
                        & ~pl.col(column).is_between(0, 1, closed="both")
                        for column in rate_columns
                    )
                )
                .sum()
                .alias("rows")
            )
        ).item()
        if violations:
            raise AssertionError(f"Rates outside [0, 1] in {mart_name}")

    segment_counts = _collect(users.group_by("segment").agg(pl.len().alias("count")))
    invalid_segments = set(segment_counts["segment"].to_list()).difference(
        {"Cold", "Explorer", "Passive", "Mixed"}
    )
    if invalid_segments:
        raise AssertionError(f"Unexpected user segments: {sorted(invalid_segments)}")
    total_segment_rows = sum(row["count"] for row in segment_counts.to_dicts())
    segment_shares = {row["segment"]: row["count"] / total_segment_rows for row in segment_counts.to_dicts()}
    if not math.isclose(sum(segment_shares.values()), 1.0, abs_tol=1e-12):
        raise AssertionError("User segment shares do not sum to 100%")

    

    tier_counts = _collect(content.group_by("content_tier").agg(pl.len().alias("count")))
    invalid_tiers = set(tier_counts["content_tier"].to_list()).difference(
        {"Head", "Torso", "Tail"}
    )
    if invalid_tiers:
        raise AssertionError(f"Unexpected content tiers: {sorted(invalid_tiers)}")
    actual_tier_counts = {
        row["content_tier"]: row["count"] for row in tier_counts.to_dicts()
    }
    expected_head = max(1, math.ceil(expected_items * 0.01))
    expected_torso_end = max(1, math.ceil(expected_items * 0.20))
    expected_tier_counts = {
        "Head": expected_head,
        "Torso": expected_torso_end - expected_head,
        "Tail": expected_items - expected_torso_end,
    }
    if actual_tier_counts != expected_tier_counts:
        raise AssertionError(
            f"Incorrect tier sizes: {actual_tier_counts}; expected {expected_tier_counts}"
        )

    result = {
        "source_interactions": interactions,
        "users": expected_users,
        "items": expected_items,
        "periods": int(_scalar(daily_stats, "periods")),
        "segment_shares": segment_shares,
        "tier_counts": actual_tier_counts,
        "file_sizes_bytes": {
            name: (marts_dir / name).stat().st_size for name in EXPECTED_COLUMNS
        },
    }
    if verbose:
        print("Validation passed")
        print(f"  all events: {interactions:,}")
        print(f"  users: {expected_users:,}; items: {expected_items:,}")
        print(f"  periods: {result['periods']:,}")
        print(
            "  segment shares: "
            + ", ".join(
                f"{name}={share:.2%}" for name, share in sorted(segment_shares.items())
            )
        )
        print(
            "  mart sizes: "
            + ", ".join(
                f"{name}={size / 1024:.1f} KiB"
                for name, size in result["file_sizes_bytes"].items()
            )
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT, help="Clean source parquet."
    )
    parser.add_argument(
        "--marts-dir", type=Path, default=DEFAULT_MARTS_DIR, help="Mart directory."
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    validate_marts(
        arguments.input,
        arguments.marts_dir,
        verbose=True,
    )
