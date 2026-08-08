"""Validate Yambda data marts and cross-check them against the raw source."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_MARTS_DIR = PROJECT_DIR / "data" / "marts"
MAX_MART_SIZE = 10 * 1024 * 1024
EXPECTED_COLUMNS = {
    "mart_daily_metrics.parquet": {
        "time_period",
        "dau",
        "total_interactions",
        "organic_ratio",
    },
    "mart_user_segments.parquet": {
        "uid",
        "total_likes",
        "organic_likes",
        "algo_likes",
        "segment",
    },
    "mart_content_health.parquet": {
        "item_id",
        "total_likes",
        "organic_ratio",
        "content_tier",
    },
}
CORE_SOURCE_COLUMNS = ("uid", "item_id", "timestamp", "is_organic")


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
            raise AssertionError(f"{name} exceeds 10 MiB: {path.stat().st_size} bytes")
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
    likes_source = (
        source.filter(pl.col("event_type").cast(pl.String) == "like")
        if "event_type" in source_schema
        else source
    )
    daily = pl.scan_parquet(marts_dir / "mart_daily_metrics.parquet")
    users = pl.scan_parquet(marts_dir / "mart_user_segments.parquet")
    content = pl.scan_parquet(marts_dir / "mart_content_health.parquet")

    source_stats = _collect(
        source.select(
            pl.len().alias("interactions"),
            pl.col("is_organic").sum().alias("organic"),
            pl.sum_horizontal(
                *(pl.col(name).null_count() for name in CORE_SOURCE_COLUMNS)
            ).alias("nulls"),
            pl.col("is_organic").min().alias("organic_min"),
            pl.col("is_organic").max().alias("organic_max"),
        )
    )
    if _scalar(source_stats, "nulls") != 0:
        raise AssertionError("Source contains nulls in required columns")
    if (
        _scalar(source_stats, "organic_min") != 0
        or _scalar(source_stats, "organic_max") != 1
    ):
        raise AssertionError("is_organic must contain binary values 0 and 1")

    like_stats = _collect(
        likes_source.select(
            pl.len().alias("interactions"),
            pl.col("uid").n_unique().alias("users"),
            pl.col("item_id").n_unique().alias("items"),
            pl.col("is_organic").sum().alias("organic"),
        )
    )

    daily_stats = _collect(
        daily.select(
            pl.len().alias("periods"),
            pl.col("time_period").n_unique().alias("unique_periods"),
            pl.col("dau").min().alias("min_dau"),
            pl.col("total_interactions").sum().alias("interactions"),
            pl.col("organic_ratio").min().alias("ratio_min"),
            pl.col("organic_ratio").max().alias("ratio_max"),
            (pl.col("organic_ratio") * pl.col("total_interactions"))
            .sum()
            .alias("organic"),
            pl.sum_horizontal(pl.all().null_count()).alias("nulls"),
        )
    )
    user_stats = _collect(
        users.select(
            pl.len().alias("users"),
            pl.col("uid").n_unique().alias("unique_users"),
            pl.col("total_likes").sum().alias("interactions"),
            pl.col("organic_likes").sum().alias("organic"),
            pl.col("algo_likes").sum().alias("algorithmic"),
            pl.sum_horizontal(pl.all().null_count()).alias("nulls"),
        )
    )
    content_stats = _collect(
        content.select(
            pl.len().alias("items"),
            pl.col("item_id").n_unique().alias("unique_items"),
            pl.col("total_likes").sum().alias("interactions"),
            pl.col("organic_ratio").min().alias("ratio_min"),
            pl.col("organic_ratio").max().alias("ratio_max"),
            (pl.col("organic_ratio") * pl.col("total_likes"))
            .sum()
            .alias("organic"),
            pl.sum_horizontal(pl.all().null_count()).alias("nulls"),
        )
    )

    interactions = int(_scalar(source_stats, "interactions"))
    expected_organic = int(_scalar(source_stats, "organic"))
    expected_like_interactions = int(_scalar(like_stats, "interactions"))
    expected_users = int(_scalar(like_stats, "users"))
    expected_items = int(_scalar(like_stats, "items"))
    expected_like_organic = int(_scalar(like_stats, "organic"))
    checks = {
        "daily_nonzero_dau": _scalar(daily_stats, "min_dau") > 0,
        "daily_unique_periods": _scalar(daily_stats, "periods")
        == _scalar(daily_stats, "unique_periods"),
        "daily_interactions_match": _scalar(daily_stats, "interactions")
        == interactions,
        "daily_ratios_valid": 0 <= _scalar(daily_stats, "ratio_min")
        <= _scalar(daily_stats, "ratio_max")
        <= 1,
        "daily_organic_matches": math.isclose(
            _scalar(daily_stats, "organic"), expected_organic, abs_tol=1e-6
        ),
        "users_match": _scalar(user_stats, "users")
        == _scalar(user_stats, "unique_users")
        == expected_users,
        "user_interactions_match": _scalar(user_stats, "interactions")
        == expected_like_interactions,
        "user_like_split_matches": _scalar(user_stats, "organic")
        + _scalar(user_stats, "algorithmic")
        == expected_like_interactions,
        "user_organic_matches": _scalar(user_stats, "organic")
        == expected_like_organic,
        "items_match": _scalar(content_stats, "items")
        == _scalar(content_stats, "unique_items")
        == expected_items,
        "content_interactions_match": _scalar(content_stats, "interactions")
        == expected_like_interactions,
        "content_ratios_valid": 0 <= _scalar(content_stats, "ratio_min")
        <= _scalar(content_stats, "ratio_max")
        <= 1,
        "content_organic_matches": math.isclose(
            _scalar(content_stats, "organic"), expected_like_organic, abs_tol=1e-6
        ),
        "no_mart_nulls": sum(
            int(_scalar(stats, "nulls"))
            for stats in (daily_stats, user_stats, content_stats)
        )
        == 0,
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
        "like_interactions": expected_like_interactions,
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
        print(f"  all events: {interactions:,}; likes: {expected_like_interactions:,}")
        print(f"  users with likes: {expected_users:,}; liked items: {expected_items:,}")
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
