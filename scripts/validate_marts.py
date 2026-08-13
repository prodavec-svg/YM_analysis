"""Validate Yambda data marts and cross-check them against the raw source."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
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
    "mart_user_segments.parquet": {
        "uid", "total_events", "unique_items", "listens", "total_likes",
        "dislikes", "completed_listens", "short_listens", "organic_listens",
        "algo_listens", "organic_likes", "algo_likes", "played_hours",
        "avg_played_ratio_pct", "completion_rate", "organic_listen_ratio",
        "segment",
    },
    "mart_content_health.parquet": {
        "item_id", "listeners", "listens", "total_likes", "dislikes",
        "completed_listens", "short_listens", "organic_listens",
        "algo_listens", "played_hours", "avg_played_ratio_pct",
        "track_length_seconds", "completion_rate", "short_listen_rate",
        "organic_ratio", "content_tier",
    },
}
CORE_SOURCE_COLUMNS = ("uid", "item_id", "timestamp", "is_organic", "event_type", "played_ratio_pct", "track_length_seconds")


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


def validate_marts(input_path: Path, marts_dir: Path, verbose: bool = False) -> dict:
    input_path = input_path.expanduser().resolve()
    marts_dir = marts_dir.expanduser().resolve()
    _check_files(marts_dir)

    source = pl.scan_parquet(input_path)
    source_schema = source.collect_schema()
    missing_core = set(CORE_SOURCE_COLUMNS).difference(source_schema.names())
    if missing_core:
        raise AssertionError(f"Source is missing core columns: {sorted(missing_core)}")
    
    # Pre-filter source exactly as build_marts_v2 does
    EVENT_TYPES = ["listen", "like", "unlike", "dislike", "undislike"]
    filtered_source = source.filter(
        pl.col("uid").is_not_null()
        & pl.col("item_id").is_not_null()
        & pl.col("timestamp").is_not_null()
        & pl.col("is_organic").is_in([0, 1])
        & pl.col("event_type").cast(pl.String).is_in(EVENT_TYPES)
    )

    daily = pl.scan_parquet(marts_dir / "mart_daily_metrics.parquet")
    event_daily = pl.scan_parquet(marts_dir / "mart_event_daily.parquet")
    users = pl.scan_parquet(marts_dir / "mart_user_segments.parquet")
    content = pl.scan_parquet(marts_dir / "mart_content_health.parquet")

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
    user_stats = _collect(
        users.select(
            pl.len().alias("users"),
            pl.col("uid").n_unique().alias("unique_users"),
            pl.col("total_events").sum().alias("interactions"),
        )
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
        "users_match": _scalar(user_stats, "users")
        == _scalar(user_stats, "unique_users")
        == expected_users,
        "user_interactions_match": _scalar(user_stats, "interactions")
        == interactions,
        "items_match": _scalar(content_stats, "items")
        == _scalar(content_stats, "unique_items")
        == expected_items,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise AssertionError(f"Failed mart checks: {', '.join(failed)}")

    segment_counts = _collect(users.group_by("segment").agg(pl.len().alias("count")))
    invalid_segments = set(segment_counts["segment"].to_list()).difference(
        {"Cold", "Explorer", "Passive", "Mixed"}
    )
    if invalid_segments:
        raise AssertionError(f"Unexpected user segments: {sorted(invalid_segments)}")
    segment_shares = {
        row["segment"]: row["count"] / expected_users
        for row in segment_counts.to_dicts()
    }
    if not math.isclose(sum(segment_shares.values()), 1.0, abs_tol=1e-12):
        raise AssertionError("User segment shares do not sum to 100%")

    expected_segment = (
        pl.when(pl.col("total_likes") < 5)
        .then(pl.lit("Cold"))
        .when(pl.col("organic_likes") / pl.col("total_likes") > 0.7)
        .then(pl.lit("Explorer"))
        .when(pl.col("algo_likes") / pl.col("total_likes") > 0.7)
        .then(pl.lit("Passive"))
        .otherwise(pl.lit("Mixed"))
    )
    invalid_user_rows = _scalar(
        _collect(users.select((pl.col("segment") != expected_segment).sum().alias("n"))),
        "n",
    )
    if invalid_user_rows:
        raise AssertionError(f"Incorrect segment on {invalid_user_rows} user rows")

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
    parser.add_argument("--input", type=Path, required=True, help="Source parquet.")
    parser.add_argument(
        "--marts-dir", type=Path, default=DEFAULT_MARTS_DIR, help="Mart directory."
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    validate_marts(arguments.input, arguments.marts_dir, verbose=True)
