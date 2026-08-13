"""Build dashboard-ready marts from cleaned Yambda multi-event data."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_DIR / "data" / "processed" / "multi_event_clean.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "data" / "marts"

DAY_IN_FIVE_SECOND_BINS = 24 * 60 * 60
EVENT_TYPES = ["listen", "like", "unlike", "dislike", "undislike"]

REQUIRED_COLUMNS = {
    "uid",
    "item_id",
    "timestamp",
    "is_organic",
    "event_type",
    "played_ratio_pct",
    "track_length_seconds",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Cleaned multi-event parquet (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for marts (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def scan_source(path: Path) -> pl.LazyFrame:
    """Read the cleaned dataset and create common derived fields."""
    if not path.is_file():
        raise FileNotFoundError(f"Input parquet does not exist: {path}")

    source = pl.scan_parquet(path)
    schema = source.collect_schema()

    missing = REQUIRED_COLUMNS.difference(schema.names())
    if missing:
        raise ValueError(f"Source is missing required columns: {sorted(missing)}")

    return (
        source
        .select(
            "uid",
            "item_id",
            "timestamp",
            "is_organic",
            "event_type",
            "played_ratio_pct",
            "track_length_seconds",
        )
        .filter(
            pl.col("uid").is_not_null()
            & pl.col("item_id").is_not_null()
            & pl.col("timestamp").is_not_null()
            & pl.col("is_organic").is_in([0, 1])
            & pl.col("event_type").cast(pl.String).is_in(EVENT_TYPES)
        )
        .with_columns(
            (pl.col("timestamp").cast(pl.UInt64) // DAY_IN_FIVE_SECOND_BINS)
            .cast(pl.UInt32)
            .alias("time_period"),

            (pl.col("event_type") == "listen").alias("is_listen"),

            pl.when(pl.col("event_type") == "listen")
            .then(pl.col("played_ratio_pct").clip(0, 100).cast(pl.Float64))
            .otherwise(None)
            .alias("played_ratio_capped_pct"),

            pl.when(pl.col("event_type") == "listen")
            .then(
                pl.col("track_length_seconds").cast(pl.Float64)
                * pl.col("played_ratio_pct").clip(0, 100).cast(pl.Float64)
                / 100
            )
            .otherwise(None)
            .alias("played_seconds_capped"),
        )
    )


def mart_daily_metrics(source: pl.LazyFrame) -> pl.LazyFrame:
    """One row = one time period. Main KPI/dynamics mart."""
    return (
        source
        .group_by("time_period")
        .agg(
            pl.col("uid").n_unique().alias("dau"),
            pl.col("item_id").n_unique().alias("active_items"),
            pl.len().alias("total_events"),

            pl.col("is_listen").sum().alias("listens"),
            (pl.col("event_type") == "like").sum().alias("likes"),
            (pl.col("event_type") == "unlike").sum().alias("unlikes"),
            (pl.col("event_type") == "dislike").sum().alias("dislikes"),
            (pl.col("event_type") == "undislike").sum().alias("undislikes"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") >= 90)
            ).sum().alias("completed_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") < 10)
            ).sum().alias("short_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("is_organic") == 1)
            ).sum().alias("organic_listens"),

            (pl.col("played_seconds_capped").sum() / 3600).alias("played_hours"),
            pl.col("played_ratio_capped_pct").mean().alias("avg_played_ratio_pct"),
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
    """One row = time period × event type × organic/non-organic."""
    return (
        source
        .group_by(["time_period", "event_type", "is_organic"])
        .agg(
            pl.len().alias("events"),
            pl.col("uid").n_unique().alias("users"),
            pl.col("item_id").n_unique().alias("items"),
        )
        .with_columns(pl.col("event_type").cast(pl.String))
        .sort(["time_period", "event_type", "is_organic"])
    )


def mart_user_segments(source: pl.LazyFrame) -> pl.LazyFrame:
    """One row = one user. Behaviour metrics + the project's user segment."""
    users = (
        source
        .group_by("uid")
        .agg(
            pl.len().alias("total_events"),
            pl.col("item_id").n_unique().alias("unique_items"),
            pl.col("is_listen").sum().alias("listens"),

            (pl.col("event_type") == "like").sum().alias("total_likes"),
            (pl.col("event_type") == "dislike").sum().alias("dislikes"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") >= 90)
            ).sum().alias("completed_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") < 10)
            ).sum().alias("short_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("is_organic") == 1)
            ).sum().alias("organic_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("is_organic") == 0)
            ).sum().alias("algo_listens"),

            (
                (pl.col("event_type") == "like")
                & (pl.col("is_organic") == 1)
            ).sum().alias("organic_likes"),

            (
                (pl.col("event_type") == "like")
                & (pl.col("is_organic") == 0)
            ).sum().alias("algo_likes"),

            (pl.col("played_seconds_capped").sum() / 3600).alias("played_hours"),
            pl.col("played_ratio_capped_pct").mean().alias("avg_played_ratio_pct"),
        )
        .with_columns(
            pl.when(pl.col("listens") > 0)
            .then(pl.col("completed_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("completion_rate"),

            pl.when(pl.col("listens") > 0)
            .then(pl.col("organic_listens") / pl.col("listens"))
            .otherwise(None)
            .alias("organic_listen_ratio"),
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
        .sort("uid")
    )

    return users


def mart_content_health(source: pl.LazyFrame) -> pl.LazyFrame:
    """One row = one item. Popularity, consumption quality and content tier."""
    items = (
        source
        .group_by("item_id")
        .agg(
            pl.col("uid")
            .filter(pl.col("event_type") == "listen")
            .n_unique()
            .alias("listeners"),

            pl.col("is_listen").sum().alias("listens"),
            (pl.col("event_type") == "like").sum().alias("total_likes"),
            (pl.col("event_type") == "dislike").sum().alias("dislikes"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") >= 90)
            ).sum().alias("completed_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("played_ratio_capped_pct") < 10)
            ).sum().alias("short_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("is_organic") == 1)
            ).sum().alias("organic_listens"),

            (
                (pl.col("event_type") == "listen")
                & (pl.col("is_organic") == 0)
            ).sum().alias("algo_listens"),

            (pl.col("played_seconds_capped").sum() / 3600).alias("played_hours"),
            pl.col("played_ratio_capped_pct").mean().alias("avg_played_ratio_pct"),
            pl.col("track_length_seconds")
            .filter(pl.col("event_type") == "listen")
            .max()
            .alias("track_length_seconds"),
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


def write_mart(plan: pl.LazyFrame, destination: Path) -> None:
    """Write a mart with Polars streaming engine."""
    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary = destination.with_suffix(destination.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()

    plan.sink_parquet(
        temporary,
        compression="zstd",
        statistics=True,
        mkdir=True,
    )
    temporary.replace(destination)
    print(f"Wrote: {destination}")


def main() -> None:
    args = parse_args()

    input_path = args.input.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    print(f"Input: {input_path}")
    source = scan_source(input_path)

    write_mart(
        mart_daily_metrics(source),
        output_dir / "mart_daily_metrics.parquet",
    )
    write_mart(
        mart_event_daily(source),
        output_dir / "mart_event_daily.parquet",
    )
    write_mart(
        mart_user_segments(source),
        output_dir / "mart_user_segments.parquet",
    )
    write_mart(
        mart_content_health(source),
        output_dir / "mart_content_health.parquet",
    )

    print("Done.")


if __name__ == "__main__":
    main()
