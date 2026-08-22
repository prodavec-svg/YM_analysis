"""Build Yambda marts from the cleaned multi-event layer.

The listening-quality metrics use one shared eligibility rule:
listen event, sequence eligible, non-bot session and non-suspected-bot user.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_DIR / "data" / "processed" / "multi_event_clean.parquet"
DEFAULT_MARTS_DIR = PROJECT_DIR / "data" / "marts"

EVENT_TYPES = ["listen", "like", "unlike", "dislike", "undislike"]
REQUIRED_COLUMNS = {
    "uid",
    "item_id",
    "time_period",
    "is_organic",
    "event_type",
    "played_ratio_pct",
    "track_length_seconds",
    "played_seconds_capped",
    "is_listen",
    "sequence_eligible",
    "is_bot_session",
    "is_suspected_bot_user",
}

# ``played_ratio_pct`` is stored on the 0..100 percentage scale in the source.
COMPLETED_RATIO_PCT = 80.0
SHORT_RATIO_PCT = 10.0
SHORT_SECONDS = 30.0


def write_mart(plan: pl.LazyFrame, destination: Path) -> None:
    """Write parquet atomically so an interrupted run keeps the old mart."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    plan.sink_parquet(temporary, compression="zstd", statistics=True, mkdir=True)
    temporary.replace(destination)
    print(f"Wrote: {destination}")


def scan_source(path: Path) -> pl.LazyFrame:
    source = pl.scan_parquet(path)
    schema = source.collect_schema()
    missing = REQUIRED_COLUMNS.difference(schema.names())
    if missing:
        raise ValueError(f"Source is missing required columns: {sorted(missing)}")

    return (
        source.select(sorted(REQUIRED_COLUMNS))
        .filter(
            pl.col("uid").is_not_null()
            & pl.col("item_id").is_not_null()
            & pl.col("time_period").is_not_null()
            & pl.col("is_organic").is_in([0, 1])
            & pl.col("event_type").cast(pl.String).is_in(EVENT_TYPES)
        )
        .with_columns(
            pl.col("event_type").cast(pl.String),
            pl.col("time_period").cast(pl.UInt32),
            pl.col("played_ratio_pct").clip(0, 100).cast(pl.Float64).alias(
                "played_ratio_capped_pct"
            ),
            pl.col("played_seconds_capped").cast(pl.Float64),
        )
    )


def eligible_listen() -> pl.Expr:
    return (
        pl.col("is_listen")
        & pl.col("sequence_eligible")
        & ~pl.col("is_bot_session")
        & ~pl.col("is_suspected_bot_user")
    )


def completed_listen() -> pl.Expr:
    return eligible_listen() & (
        pl.col("played_ratio_capped_pct") >= COMPLETED_RATIO_PCT
    )


def short_listen() -> pl.Expr:
    return eligible_listen() & (
        (pl.col("played_seconds_capped") <= SHORT_SECONDS)
        | (pl.col("played_ratio_capped_pct") < SHORT_RATIO_PCT)
    )


def mart_daily_metrics(source: pl.LazyFrame) -> pl.LazyFrame:
    return (
        source.group_by("time_period")
        .agg(
            pl.col("uid").n_unique().alias("dau"),
            pl.col("item_id").n_unique().alias("active_items"),
            pl.len().alias("total_events"),
            eligible_listen().sum().alias("listens"),
            (pl.col("event_type") == "like").sum().alias("likes"),
            (pl.col("event_type") == "unlike").sum().alias("unlikes"),
            (pl.col("event_type") == "dislike").sum().alias("dislikes"),
            (pl.col("event_type") == "undislike").sum().alias("undislikes"),
            completed_listen().sum().alias("completed_listens"),
            short_listen().sum().alias("short_listens"),
            (eligible_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_listens"),
            pl.col("played_seconds_capped")
            .filter(eligible_listen())
            .sum()
            .truediv(3600)
            .alias("played_hours"),
            pl.col("played_ratio_capped_pct")
            .filter(eligible_listen())
            .mean()
            .alias("avg_played_ratio_pct"),
        )
        .with_columns(
            pl.when(pl.col("listens") > 0)
            .then(pl.col("completed_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("completion_rate"),
            pl.when(pl.col("listens") > 0)
            .then(pl.col("short_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("short_listen_rate"),
            pl.when(pl.col("listens") > 0)
            .then(pl.col("organic_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("organic_listen_ratio"),
        )
        .sort("time_period")
    )


def mart_event_daily(source: pl.LazyFrame) -> pl.LazyFrame:
    return (
        source.group_by(["time_period", "event_type", "is_organic"])
        .agg(
            pl.len().alias("events"),
            pl.col("uid").n_unique().alias("users"),
            pl.col("item_id").n_unique().alias("items"),
        )
        .sort(["time_period", "event_type", "is_organic"])
    )


def mart_user_segments(source: pl.LazyFrame) -> pl.LazyFrame:
    daily_stats = source.group_by(["uid", "time_period"]).agg(
        (pl.col("event_type") == "like").sum().alias("daily_total_likes"),
        ((pl.col("event_type") == "like") & (pl.col("is_organic") == 1))
        .sum()
        .alias("daily_organic_likes"),
        ((pl.col("event_type") == "like") & (pl.col("is_organic") == 0))
        .sum()
        .alias("daily_algo_likes"),
    )
    global_max = source.select(pl.col("time_period").max().alias("max_period"))
    user_spans = (
        source.group_by("uid")
        .agg(pl.col("time_period").min().alias("min_period"))
        .join(global_max, how="cross")
    )
    user_grid = (
        user_spans.with_columns(
            pl.int_ranges(pl.col("min_period"), pl.col("max_period") + 1).alias(
                "time_period"
            )
        )
        .explode("time_period", empty_as_null=True)
        .select(pl.col("uid"), pl.col("time_period").cast(pl.UInt32))
    )
    return (
        user_grid.join(daily_stats, on=["uid", "time_period"], how="left")
        .with_columns(
            pl.col("daily_total_likes").fill_null(0),
            pl.col("daily_organic_likes").fill_null(0),
            pl.col("daily_algo_likes").fill_null(0),
        )
        .sort(["uid", "time_period"])
        .with_columns(
            pl.col("daily_total_likes").cum_sum().over("uid").alias("total_likes"),
            pl.col("daily_organic_likes")
            .cum_sum()
            .over("uid")
            .alias("organic_likes"),
            pl.col("daily_algo_likes").cum_sum().over("uid").alias("algo_likes"),
        )
        .with_columns(
            pl.when(pl.col("total_likes") < 5)
            .then(pl.lit("Cold"))
            .when(pl.col("organic_likes") / pl.col("total_likes") > 0.7)
            .then(pl.lit("Explorer"))
            .when(pl.col("algo_likes") / pl.col("total_likes") > 0.7)
            .then(pl.lit("Passive"))
            .otherwise(pl.lit("Mixed"))
            .alias("segment")
        )
        .select("time_period", "uid", "segment")
        .sort(["time_period", "uid"])
    )


def mart_content_health(source: pl.LazyFrame) -> pl.LazyFrame:
    items = (
        source.group_by("item_id")
        .agg(
            pl.col("uid").filter(eligible_listen()).n_unique().alias("listeners"),
            eligible_listen().sum().alias("listens"),
            (pl.col("event_type") == "like").sum().alias("total_likes"),
            (pl.col("event_type") == "dislike").sum().alias("dislikes"),
            completed_listen().sum().alias("completed_listens"),
            short_listen().sum().alias("short_listens"),
            (eligible_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_listens"),
            (eligible_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_listens"),
            (completed_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_completed_listens"),
            (completed_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_completed_listens"),
            (short_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_short_listens"),
            (short_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_short_listens"),
            pl.col("played_seconds_capped")
            .filter(eligible_listen())
            .sum()
            .truediv(3600)
            .alias("played_hours"),
            pl.col("played_ratio_capped_pct")
            .filter(eligible_listen())
            .mean()
            .alias("avg_played_ratio_pct"),
            pl.col("track_length_seconds")
            .filter(eligible_listen())
            .max()
            .alias("track_length_seconds"),
        )
        .with_columns(
            # Compatibility aliases used by the DataLens user mart formulas.
            pl.col("organic_listens").alias("organic_listening"),
            pl.col("algo_listens").alias("algo_listening"),
            pl.when(pl.col("listens") > 0)
            .then(pl.col("completed_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("completion_rate"),
            pl.when(pl.col("listens") > 0)
            .then(pl.col("short_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("short_listen_rate"),
            pl.when(pl.col("listens") > 0)
            .then(pl.col("organic_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("organic_ratio"),
        )
        .sort(["total_likes", "item_id"], descending=[True, False])
        .with_row_index("_rank", offset=1)
        .with_columns(
            pl.max_horizontal(
                pl.lit(1, dtype=pl.UInt32),
                (pl.len() * 0.01).ceil().cast(pl.UInt32),
            ).alias("_head_end"),
            pl.max_horizontal(
                pl.lit(1, dtype=pl.UInt32),
                (pl.len() * 0.20).ceil().cast(pl.UInt32),
            ).alias("_torso_end"),
        )
        .with_columns(
            pl.when(pl.col("_rank") <= pl.col("_head_end"))
            .then(pl.lit("Head"))
            .when(pl.col("_rank") <= pl.col("_torso_end"))
            .then(pl.lit("Torso"))
            .otherwise(pl.lit("Tail"))
            .alias("content_tier")
        )
        .drop("_rank", "_head_end", "_torso_end")
    )
    return items


def mart_user_general(source: pl.LazyFrame) -> pl.LazyFrame:
    return (
        source.group_by(["time_period", "uid"])
        .agg(
            (eligible_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_listening"),
            (eligible_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_listening"),
            ((pl.col("event_type") == "like") & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_likes"),
            ((pl.col("event_type") == "unlike") & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_unlikes"),
            ((pl.col("event_type") == "dislike") & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_dislikes"),
            ((pl.col("event_type") == "undislike") & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_undislikes"),
            ((pl.col("event_type") == "like") & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_likes"),
            ((pl.col("event_type") == "unlike") & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_unlikes"),
            ((pl.col("event_type") == "dislike") & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_dislikes"),
            ((pl.col("event_type") == "undislike") & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_undislikes"),
            (completed_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_completed_listens"),
            (completed_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_completed_listens"),
            (short_listen() & (pl.col("is_organic") == 0))
            .sum()
            .alias("algo_short_listens"),
            (short_listen() & (pl.col("is_organic") == 1))
            .sum()
            .alias("organic_short_listens"),
            pl.col("item_id")
            .filter(eligible_listen() & (pl.col("is_organic") == 0))
            .n_unique()
            .alias("algo_unique_items"),
            pl.col("item_id")
            .filter(eligible_listen() & (pl.col("is_organic") == 1))
            .n_unique()
            .alias("organic_unique_items"),
        )
        .sort(["time_period", "uid"])
    )


def build_all(input_path: Path, marts_dir: Path) -> None:
    source = scan_source(input_path)
    plans = {
        "mart_daily_metrics.parquet": mart_daily_metrics(source),
        "mart_event_daily.parquet": mart_event_daily(source),
        "mart_user_segments.parquet": mart_user_segments(source),
        "mart_content_health.parquet": mart_content_health(source),
        "mart_user_general.parquet": mart_user_general(source),
    }
    for name, plan in plans.items():
        write_mart(plan, marts_dir / name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--marts-dir", type=Path, default=DEFAULT_MARTS_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_all(
        args.input.expanduser().resolve(),
        args.marts_dir.expanduser().resolve(),
    )
