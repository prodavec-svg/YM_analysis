from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

from scripts.build_marts_v2 import (
    build_all,
    mart_content_health,
    mart_user_general,
    mart_user_segments,
)
from scripts.validate_marts import validate_marts


class ListeningMetricTests(unittest.TestCase):
    @staticmethod
    def source() -> pl.LazyFrame:
        rows = [
            # Eligible algorithm completion (80% boundary).
            (1, 10, 0, 0, "listen", 80.0, 100.0, 80.0, True, True, False, False),
            # Eligible algorithm short listen by seconds, although ratio is 20%.
            (1, 11, 0, 0, "listen", 20.0, 100.0, 20.0, True, True, False, False),
            # Eligible organic short listen by ratio, although duration exceeds 30s.
            (1, 12, 0, 1, "listen", 9.0, 400.0, 36.0, True, True, False, False),
            # Repeated item must count once in the unique-item metric.
            (1, 12, 0, 1, "listen", 90.0, 400.0, 360.0, True, True, False, False),
            # Ineligible/offline, bot-session and suspected-bot listens are excluded.
            (1, 13, 0, 0, "listen", 100.0, 100.0, 100.0, True, False, False, False),
            (1, 14, 0, 0, "listen", 100.0, 100.0, 100.0, True, True, True, False),
            (1, 15, 0, 0, "listen", 100.0, 100.0, 100.0, True, True, False, True),
            # Reaction metrics remain available alongside listening metrics.
            (1, 10, 0, 0, "like", None, None, None, False, True, False, False),
            (2, 20, 1, 1, "listen", 100.0, 200.0, 200.0, True, True, False, False),
        ]
        return pl.DataFrame(
            rows,
            schema={
                "uid": pl.Int64,
                "item_id": pl.Int64,
                "time_period": pl.UInt32,
                "is_organic": pl.Int8,
                "event_type": pl.String,
                "played_ratio_pct": pl.Float64,
                "track_length_seconds": pl.Float64,
                "played_seconds_capped": pl.Float64,
                "is_listen": pl.Boolean,
                "sequence_eligible": pl.Boolean,
                "is_bot_session": pl.Boolean,
                "is_suspected_bot_user": pl.Boolean,
            },
            orient="row",
        ).with_columns(
            pl.col("played_ratio_pct").alias("played_ratio_capped_pct")
        ).lazy()

    def test_user_metrics_apply_thresholds_and_filters(self) -> None:
        result = mart_user_general(self.source()).collect().sort(["time_period", "uid"])
        first = result.row(0, named=True)
        self.assertEqual(first["algo_listening"], 2)
        self.assertEqual(first["organic_listening"], 2)
        self.assertEqual(first["algo_completed_listens"], 1)
        self.assertEqual(first["organic_completed_listens"], 1)
        self.assertEqual(first["algo_short_listens"], 1)
        self.assertEqual(first["organic_short_listens"], 1)
        self.assertEqual(first["algo_unique_items"], 2)
        self.assertEqual(first["organic_unique_items"], 1)
        self.assertEqual(first["algo_likes"], 1)

    def test_content_splits_reconcile_with_pooled_metrics(self) -> None:
        result = mart_content_health(self.source()).collect()
        totals = result.select(
            pl.col("listens").sum(),
            pl.col("organic_listens").sum(),
            pl.col("algo_listens").sum(),
            pl.col("completed_listens").sum(),
            pl.col("algo_completed_listens").sum(),
            pl.col("organic_completed_listens").sum(),
            pl.col("short_listens").sum(),
            pl.col("algo_short_listens").sum(),
            pl.col("organic_short_listens").sum(),
        ).row(0, named=True)
        self.assertEqual(totals["listens"], totals["organic_listens"] + totals["algo_listens"])
        self.assertEqual(
            totals["completed_listens"],
            totals["algo_completed_listens"] + totals["organic_completed_listens"],
        )
        self.assertEqual(
            totals["short_listens"],
            totals["algo_short_listens"] + totals["organic_short_listens"],
        )

    def test_full_build_exports_and_validates_all_datalens_csvs(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "multi_event_clean.parquet"
            marts_dir = root / "marts"
            csv_dir = root / "csv"
            self.source().collect().drop("played_ratio_capped_pct").write_parquet(
                input_path
            )

            build_all(input_path, marts_dir, csv_dir)

            self.assertEqual(
                {path.name for path in csv_dir.glob("*.csv")},
                {
                    "mart_user_general.csv",
                    "mart_user_segments.csv",
                    "mart_content_health.csv",
                },
            )
            result = validate_marts(
                input_path,
                marts_dir,
                csv_dir=csv_dir,
            )
            self.assertEqual(result["users"], 2)
            self.assertEqual(result["items"], 7)

    def test_segments_are_cumulative_and_prolonged(self) -> None:
        rows = []
        for period in range(5):
            rows.append((1, period, 1, "like"))
        # A later event from another user extends the global period range.
        rows.append((2, 5, 0, "like"))
        source = pl.DataFrame(
            rows,
            schema={
                "uid": pl.Int64,
                "time_period": pl.UInt32,
                "is_organic": pl.Int8,
                "event_type": pl.String,
            },
            orient="row",
        ).lazy()

        segments = (
            mart_user_segments(source)
            .collect()
            .filter(pl.col("uid") == 1)
            .sort("time_period")
        )

        self.assertEqual(segments["time_period"].to_list(), list(range(6)))
        self.assertEqual(
            segments["segment"].to_list(),
            ["Cold", "Cold", "Cold", "Cold", "Explorer", "Explorer"],
        )


if __name__ == "__main__":
    unittest.main()
