import argparse
from pathlib import Path
import logging
import duckdb
import shutil

logging.basicConfig(
    filename='build_marts.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_DIR / "data" / "processed" / "multi_event_clean.parquet"
DEFAULT_MARTS_DIR = PROJECT_DIR / "data" / "marts"
TMP_DIR = PROJECT_DIR / "data" / "processed" / "tmp_duckdb_marts"

def build_all(input_path: Path, marts_dir: Path) -> None:
    logging.info(f"Starting mart compilation using DuckDB. Input: {input_path}")
    marts_dir.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='6GB'")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}'")
    
    # Base view with in-line eligibility flag to avoid massive self-joins!
    con.execute(f"""
        CREATE VIEW source AS 
        SELECT *, 
               LEAST(GREATEST(played_ratio_pct, 0.0), 100.0) AS played_ratio_capped_pct,
               (is_listen = true 
                AND sequence_eligible = true 
                AND is_bot_session = false 
                AND is_suspected_bot_user = false) AS eligible
        FROM read_parquet('{input_path.as_posix()}');
    """)

    # 1. mart_daily_metrics
    logging.info("[1/5] Compiling mart_daily_metrics.parquet...")
    con.execute(f"""
        COPY (
            SELECT 
                time_period,
                COUNT(DISTINCT uid) AS dau,
                COUNT(DISTINCT item_id) AS active_items,
                COUNT(*) AS total_events,
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS listens,
                SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) AS likes,
                SUM(CASE WHEN event_type = 'unlike' THEN 1 ELSE 0 END) AS unlikes,
                SUM(CASE WHEN event_type = 'dislike' THEN 1 ELSE 0 END) AS dislikes,
                SUM(CASE WHEN event_type = 'undislike' THEN 1 ELSE 0 END) AS undislikes,
                SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 THEN 1 ELSE 0 END) AS completed_listens,
                SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) THEN 1 ELSE 0 END) AS short_listens,
                SUM(CASE WHEN eligible AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_listens,
                SUM(CASE WHEN eligible THEN played_seconds_capped ELSE 0 END) / 3600.0 AS played_hours,
                AVG(CASE WHEN eligible THEN played_ratio_capped_pct ELSE NULL END) AS avg_played_ratio_pct,
                
                CASE WHEN SUM(CASE WHEN eligible THEN 1 ELSE 0 END) > 0 THEN 
                    CAST(SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 THEN 1 ELSE 0 END) AS FLOAT) / SUM(CASE WHEN eligible THEN 1 ELSE 0 END) 
                ELSE NULL END AS completion_rate,
                
                CASE WHEN SUM(CASE WHEN eligible THEN 1 ELSE 0 END) > 0 THEN 
                    CAST(SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) THEN 1 ELSE 0 END) AS FLOAT) / SUM(CASE WHEN eligible THEN 1 ELSE 0 END) 
                ELSE NULL END AS short_listen_rate,
                
                CASE WHEN SUM(CASE WHEN eligible THEN 1 ELSE 0 END) > 0 THEN 
                    CAST(SUM(CASE WHEN eligible AND is_organic = 1 THEN 1 ELSE 0 END) AS FLOAT) / SUM(CASE WHEN eligible THEN 1 ELSE 0 END) 
                ELSE NULL END AS organic_listen_ratio
                
            FROM source
            GROUP BY time_period
            ORDER BY time_period
        ) TO '{(marts_dir / 'mart_daily_metrics.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)

    # 2. mart_event_daily
    logging.info("[2/5] Compiling mart_event_daily.parquet...")
    con.execute(f"""
        COPY (
            SELECT 
                time_period,
                event_type,
                is_organic,
                COUNT(*) AS events,
                COUNT(DISTINCT uid) AS users,
                COUNT(DISTINCT item_id) AS items
            FROM source
            GROUP BY time_period, event_type, is_organic
            ORDER BY time_period, event_type, is_organic
        ) TO '{(marts_dir / 'mart_event_daily.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)

    # 3. mart_user_general
    logging.info("[3/5] Compiling mart_user_general.parquet...")
    con.execute(f"""
        COPY (
            SELECT 
                time_period,
                uid,
                SUM(CASE WHEN eligible AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_listening,
                SUM(CASE WHEN eligible AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_listening,
                SUM(CASE WHEN event_type = 'like' AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_likes,
                SUM(CASE WHEN event_type = 'unlike' AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_unlikes,
                SUM(CASE WHEN event_type = 'dislike' AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_dislikes,
                SUM(CASE WHEN event_type = 'undislike' AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_undislikes,
                SUM(CASE WHEN event_type = 'like' AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_likes,
                SUM(CASE WHEN event_type = 'unlike' AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_unlikes,
                SUM(CASE WHEN event_type = 'dislike' AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_dislikes,
                SUM(CASE WHEN event_type = 'undislike' AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_undislikes,
                SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_completed_listens,
                SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_completed_listens,
                SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_short_listens,
                SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_short_listens,
                COUNT(DISTINCT CASE WHEN eligible AND is_organic = 0 THEN item_id ELSE NULL END) AS algo_unique_items,
                COUNT(DISTINCT CASE WHEN eligible AND is_organic = 1 THEN item_id ELSE NULL END) AS organic_unique_items
            FROM source
            GROUP BY time_period, uid
            ORDER BY time_period, uid
        ) TO '{(marts_dir / 'mart_user_general.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)

    # 4. mart_user_segments
    logging.info("[4/5] Compiling mart_user_segments.parquet...")
    con.execute(f"""
        COPY (
            WITH daily_stats AS (
                SELECT uid, time_period,
                       SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) AS daily_total_likes,
                       SUM(CASE WHEN event_type = 'like' AND is_organic = 1 THEN 1 ELSE 0 END) AS daily_organic_likes,
                       SUM(CASE WHEN event_type = 'like' AND is_organic = 0 THEN 1 ELSE 0 END) AS daily_algo_likes
                FROM source
                GROUP BY uid, time_period
            ),
            user_spans AS (
                SELECT uid, MIN(time_period) as min_period, (SELECT MAX(time_period) FROM source) as max_period
                FROM source
                GROUP BY uid
            ),
            user_grid AS (
                SELECT uid, UNNEST(GENERATE_SERIES(min_period, max_period)) AS time_period
                FROM user_spans
            ),
            joined AS (
                SELECT 
                    g.uid, 
                    g.time_period,
                    COALESCE(d.daily_total_likes, 0) AS daily_total_likes,
                    COALESCE(d.daily_organic_likes, 0) AS daily_organic_likes,
                    COALESCE(d.daily_algo_likes, 0) AS daily_algo_likes
                FROM user_grid g
                LEFT JOIN daily_stats d ON g.uid = d.uid AND g.time_period = d.time_period
            ),
            cumulative AS (
                SELECT 
                    uid,
                    time_period,
                    SUM(daily_total_likes) OVER (PARTITION BY uid ORDER BY time_period) AS total_likes,
                    SUM(daily_organic_likes) OVER (PARTITION BY uid ORDER BY time_period) AS organic_likes,
                    SUM(daily_algo_likes) OVER (PARTITION BY uid ORDER BY time_period) AS algo_likes
                FROM joined
            )
            SELECT 
                time_period,
                uid,
                CASE 
                    WHEN total_likes < 5 THEN 'Cold'
                    WHEN CAST(organic_likes AS FLOAT) / GREATEST(total_likes, 1) > 0.7 THEN 'Explorer'
                    WHEN CAST(algo_likes AS FLOAT) / GREATEST(total_likes, 1) > 0.7 THEN 'Passive'
                    ELSE 'Mixed'
                END AS segment
            FROM cumulative
            ORDER BY time_period, uid
        ) TO '{(marts_dir / 'mart_user_segments.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)

    # 5. mart_content_health
    logging.info("[5/5] Compiling mart_content_health.parquet...")
    con.execute(f"""
        COPY (
            WITH item_stats AS (
                SELECT 
                    item_id,
                    COUNT(DISTINCT CASE WHEN eligible THEN uid ELSE NULL END) AS listeners,
                    SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS listens,
                    SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) AS total_likes,
                    SUM(CASE WHEN event_type = 'dislike' THEN 1 ELSE 0 END) AS dislikes,
                    SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 THEN 1 ELSE 0 END) AS completed_listens,
                    SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) THEN 1 ELSE 0 END) AS short_listens,
                    SUM(CASE WHEN eligible AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_listens,
                    SUM(CASE WHEN eligible AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_listens,
                    SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_completed_listens,
                    SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80.0 AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_completed_listens,
                    SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) AND is_organic = 0 THEN 1 ELSE 0 END) AS algo_short_listens,
                    SUM(CASE WHEN eligible AND (played_seconds_capped <= 30.0 OR played_ratio_capped_pct < 10.0) AND is_organic = 1 THEN 1 ELSE 0 END) AS organic_short_listens,
                    SUM(CASE WHEN eligible THEN played_seconds_capped ELSE 0 END) / 3600.0 AS played_hours,
                    AVG(CASE WHEN eligible THEN played_ratio_capped_pct ELSE NULL END) AS avg_played_ratio_pct,
                    MAX(CASE WHEN eligible THEN track_length_seconds ELSE NULL END) AS track_length_seconds
                FROM source
                GROUP BY item_id
            ),
            ranked_items AS (
                SELECT *,
                       organic_listens AS organic_listening,
                       algo_listens AS algo_listening,
                       CASE WHEN listens > 0 THEN CAST(completed_listens AS FLOAT) / listens ELSE NULL END AS completion_rate,
                       CASE WHEN listens > 0 THEN CAST(short_listens AS FLOAT) / listens ELSE NULL END AS short_listen_rate,
                       CASE WHEN listens > 0 THEN CAST(organic_listens AS FLOAT) / listens ELSE NULL END AS organic_ratio,
                       ROW_NUMBER() OVER (ORDER BY total_likes DESC, item_id ASC) AS _rank,
                       COUNT(*) OVER () AS _total_count
                FROM item_stats
            )
            SELECT 
                item_id, listeners, listens, total_likes, dislikes, completed_listens, short_listens,
                organic_listens, algo_listens, algo_completed_listens, organic_completed_listens,
                algo_short_listens, organic_short_listens, played_hours, avg_played_ratio_pct,
                track_length_seconds, organic_listening, algo_listening, completion_rate,
                short_listen_rate, organic_ratio,
                CASE 
                    WHEN _rank <= GREATEST(1, CEIL(_total_count * 0.01)) THEN 'Head'
                    WHEN _rank <= GREATEST(1, CEIL(_total_count * 0.20)) THEN 'Torso'
                    ELSE 'Tail'
                END AS content_tier
            FROM ranked_items
            ORDER BY total_likes DESC, item_id ASC
        ) TO '{(marts_dir / 'mart_content_health.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """)

    logging.info("All marts compiled successfully.")
    shutil.rmtree(TMP_DIR, ignore_errors=True)

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
