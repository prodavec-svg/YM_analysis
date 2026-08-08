"""Build dashboard-ready Yambda data marts with Polars.

The source is scanned lazily and every aggregation is executed with Polars'
streaming engine. Output files are replaced only after a complete mart has
been collected and written successfully.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from validate_marts import validate_marts


PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "data" / "marts"
DEFAULT_INPUT_CANDIDATES = (
    PROJECT_DIR / "data" / "raw" / "likes.parquet",
    PROJECT_DIR / "data" / "raw" / "multi_event.parquet",
    PROJECT_DIR / "likes_50m.parquet",
    PROJECT_DIR / "likes.parquet",
    PROJECT_DIR / "multi_event.parquet",
)
REQUIRED_COLUMNS = {"uid", "item_id", "timestamp", "is_organic"}
DAY_IN_FIVE_SECOND_BINS = 24 * 60 * 60 // 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        help="Source likes parquet. If omitted, common project paths are tried.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for generated marts (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Do not run post-build cross-mart validation.",
    )
    return parser.parse_args()


def resolve_input(requested: Path | None) -> Path:
    if requested is not None:
        path = requested.expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Input parquet does not exist: {path}")
        return path

    for path in DEFAULT_INPUT_CANDIDATES:
        if path.is_file():
            return path.resolve()
    tried = "\n  - ".join(str(path) for path in DEFAULT_INPUT_CANDIDATES)
    raise FileNotFoundError(f"Could not find an input parquet. Tried:\n  - {tried}")


def scan_source(path: Path) -> pl.LazyFrame:
    source = pl.scan_parquet(path)
    schema = source.collect_schema()
    missing = REQUIRED_COLUMNS.difference(schema.names())
    if missing:
        raise ValueError(f"Source is missing required columns: {sorted(missing)}")

    integer_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    }
    invalid_types = {
        name: schema[name]
        for name in REQUIRED_COLUMNS
        if schema[name] not in integer_types
    }
    if invalid_types:
        raise TypeError(f"Required columns must be integers: {invalid_types}")

    return source.select(
        pl.col("uid").cast(pl.UInt32),
        pl.col("item_id").cast(pl.UInt32),
        pl.col("timestamp").cast(pl.UInt64),
        pl.col("is_organic").cast(pl.UInt8),
    )


def daily_metrics(source: pl.LazyFrame) -> pl.LazyFrame:
    return (
        source.with_columns(
            (pl.col("timestamp") // DAY_IN_FIVE_SECOND_BINS)
            .cast(pl.UInt32)
            .alias("time_period")
        )
        .group_by("time_period")
        .agg(
            pl.col("uid").n_unique().cast(pl.UInt32).alias("dau"),
            pl.len().cast(pl.UInt32).alias("total_interactions"),
            pl.col("is_organic").mean().cast(pl.Float64).alias("organic_ratio"),
        )
        .sort("time_period")
    )


def user_segments(source: pl.LazyFrame) -> pl.LazyFrame:
    return (
        source.group_by("uid")
        .agg(
            pl.len().cast(pl.UInt32).alias("total_likes"),
            pl.col("is_organic").sum().cast(pl.UInt32).alias("organic_likes"),
        )
        .with_columns(
            (pl.col("total_likes") - pl.col("organic_likes"))
            .cast(pl.UInt32)
            .alias("algo_likes")
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
        .select("uid", "total_likes", "organic_likes", "algo_likes", "segment")
        .sort("uid")
    )


def content_health(source: pl.LazyFrame) -> pl.LazyFrame:
    # Ordinal rank makes tier sizes deterministic even when many items tie.
    ranked = (
        source.group_by("item_id")
        .agg(
            pl.len().cast(pl.UInt32).alias("total_likes"),
            pl.col("is_organic").mean().cast(pl.Float64).alias("organic_ratio"),
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
    )
    return (
        ranked.with_columns(
            pl.when(pl.col("_rank") <= pl.col("_head_end"))
            .then(pl.lit("Head"))
            .when(pl.col("_rank") <= pl.col("_torso_end"))
            .then(pl.lit("Torso"))
            .otherwise(pl.lit("Tail"))
            .alias("content_tier")
        )
        .select("item_id", "total_likes", "organic_ratio", "content_tier")
    )


def write_mart(plan: pl.LazyFrame, destination: Path) -> None:
    # engine="streaming" is the current Polars equivalent of the deprecated
    # collect(streaming=True) API requested in the assignment.
    frame = plan.collect(engine="streaming")
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    frame.write_parquet(temporary, compression="zstd", statistics=True)
    temporary.replace(destination)
    print(f"Wrote {destination.name}: {frame.height:,} rows")


def main() -> None:
    args = parse_args()
    input_path = resolve_input(args.input)
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    source = scan_source(input_path)
    print(f"Input: {input_path}")
    write_mart(daily_metrics(source), output_dir / "mart_daily_metrics.parquet")
    write_mart(user_segments(source), output_dir / "mart_user_segments.parquet")
    write_mart(content_health(source), output_dir / "mart_content_health.parquet")

    if not args.skip_validation:
        validate_marts(input_path=input_path, marts_dir=output_dir, verbose=True)


if __name__ == "__main__":
    main()
