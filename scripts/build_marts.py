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
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_DIR / "data" / "processed" / "multi_event_clean_subset.parquet"
DEFAULT_MARTS_DIR = PROJECT_DIR / "data" / "marts"
TMP_DIR = PROJECT_DIR / "data" / "processed" / "tmp_duckdb_marts"

def build_all(input_path: str, marts_dir: Path) -> None:
    logging.info(f"Starting optimized unified mart compilation using DuckDB. Input: {input_path}")
    marts_dir.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}'")
    
    logging.info("Compiling mart_unified.parquet (Optimized Pre-Aggregation Logic)...")
    
    query = f"""
    COPY (
        WITH source AS (
            SELECT *, 
                   LEAST(GREATEST(played_ratio_pct, 0.0), 100.0) AS played_ratio_capped_pct,
                   (is_listen = true 
                    AND sequence_eligible = true 
                    AND is_bot_session = false 
                    AND is_suspected_bot_user = false) AS eligible
            FROM read_parquet('{input_path}')
        ),
        
        -- 1. Pre-aggregate raw events at the user-item-day level
        -- This compresses 500M rows into ~30M rows!
        user_item_daily AS (
            SELECT 
                time_period,
                uid,
                item_id,
                MAX(CASE WHEN eligible THEN 1 ELSE 0 END) AS has_eligible_activity,
                MAX(CAST(is_organic AS INTEGER)) AS is_organic,
                
                SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS listens,
                SUM(CASE WHEN eligible THEN played_seconds_capped ELSE 0 END) / 3600.0 AS played_hours,
                SUM(CASE WHEN eligible AND played_ratio_capped_pct >= 80 THEN 1 ELSE 0 END) AS completed_listens,
                SUM(CASE WHEN eligible AND (played_seconds_capped <= 30 OR played_ratio_capped_pct < 10) THEN 1 ELSE 0 END) AS short_listens,
                
                SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) AS likes,
                SUM(CASE WHEN event_type = 'unlike' THEN 1 ELSE 0 END) AS unlikes,
                SUM(CASE WHEN event_type = 'dislike' THEN 1 ELSE 0 END) AS dislikes,
                SUM(CASE WHEN event_type = 'undislike' THEN 1 ELSE 0 END) AS undislikes
            FROM source
            GROUP BY time_period, uid, item_id
        ),
        
        -- 2. Daily User Segments
        user_segments AS (
            SELECT 
                time_period, uid,
                SUM(likes) AS total_likes,
                
                CASE 
                    WHEN SUM(likes) < 5 THEN 'Cold'
                    WHEN CAST(SUM(CASE WHEN is_organic = 1 THEN likes ELSE 0 END) AS FLOAT) / GREATEST(SUM(likes), 1) > 0.7 THEN 'Explorer'
                    WHEN CAST(SUM(CASE WHEN is_organic = 0 THEN likes ELSE 0 END) AS FLOAT) / GREATEST(SUM(likes), 1) > 0.7 THEN 'Passive'
                    ELSE 'Mixed'
                END AS segment
            FROM user_item_daily
            GROUP BY time_period, uid
        ),
        
        -- 3. Daily Content Tiers
        item_ranked AS (
            SELECT 
                time_period, item_id,
                SUM(likes) AS total_likes,
                PERCENT_RANK() OVER (PARTITION BY time_period ORDER BY SUM(likes) DESC) AS percentile
            FROM user_item_daily
            GROUP BY time_period, item_id
        ),
        item_tiers AS (
            SELECT 
                time_period, item_id,
                CASE 
                    WHEN percentile <= 0.01 THEN 'Head'
                    WHEN percentile <= 0.20 THEN 'Torso'
                    ELSE 'Tail'
                END AS content_tier
            FROM item_ranked
        ),
        
        -- 4. Enriched User-Item Daily (Joining to the SMALL table!)
        enriched_user_item AS (
            SELECT 
                u.*,
                COALESCE(s.segment, 'Cold') AS segment,
                COALESCE(i.content_tier, 'Tail') AS content_tier
            FROM user_item_daily u
            LEFT JOIN user_segments s ON u.time_period = s.time_period AND u.uid = s.uid
            LEFT JOIN item_tiers i ON u.time_period = i.time_period AND u.item_id = i.item_id
        ),
        
        -- 5. User-Level Aggregation
        -- Since the base table is unique on (uid, item_id), COUNT(item_id) is exactly COUNT DISTINCT!
        user_level_agg AS (
            SELECT 
                time_period,
                uid,
                segment,
                content_tier,
                1 AS has_activity,
                
                COUNT(CASE WHEN is_organic = 0 AND has_eligible_activity > 0 THEN item_id END) AS user_algo_unique,
                COUNT(CASE WHEN is_organic = 1 AND has_eligible_activity > 0 THEN item_id END) AS user_organic_unique,
                
                SUM(CASE WHEN is_organic = 0 THEN listens ELSE 0 END) AS algo_listens,
                SUM(CASE WHEN is_organic = 1 THEN listens ELSE 0 END) AS organic_listens,
                
                SUM(CASE WHEN is_organic = 0 THEN played_hours ELSE 0 END) AS algo_played_hours,
                SUM(CASE WHEN is_organic = 1 THEN played_hours ELSE 0 END) AS organic_played_hours,
                
                SUM(CASE WHEN is_organic = 0 THEN completed_listens ELSE 0 END) AS algo_completed_listens,
                SUM(CASE WHEN is_organic = 1 THEN completed_listens ELSE 0 END) AS organic_completed_listens,
                
                SUM(CASE WHEN is_organic = 0 THEN short_listens ELSE 0 END) AS algo_short_listens,
                SUM(CASE WHEN is_organic = 1 THEN short_listens ELSE 0 END) AS organic_short_listens,
                
                SUM(CASE WHEN is_organic = 0 THEN likes ELSE 0 END) AS algo_likes,
                SUM(CASE WHEN is_organic = 1 THEN likes ELSE 0 END) AS organic_likes,
                
                SUM(CASE WHEN is_organic = 0 THEN unlikes ELSE 0 END) AS algo_unlikes,
                SUM(CASE WHEN is_organic = 1 THEN unlikes ELSE 0 END) AS organic_unlikes,
                
                SUM(CASE WHEN is_organic = 0 THEN dislikes ELSE 0 END) AS algo_dislikes,
                SUM(CASE WHEN is_organic = 1 THEN dislikes ELSE 0 END) AS organic_dislikes
            FROM enriched_user_item
            GROUP BY time_period, uid, segment, content_tier
        ),
        
        -- 6. Final Mart Aggregation
        final_mart AS (
            SELECT 
                time_period,
                segment,
                content_tier,
                SUM(has_activity) AS slice_users_count,
                COUNT(DISTINCT uid) AS segment_dau, 
                
                SUM(algo_listens) AS algo_listens,
                SUM(organic_listens) AS organic_listens,
                
                SUM(algo_played_hours) AS algo_played_hours,
                SUM(organic_played_hours) AS organic_played_hours,
                
                SUM(algo_completed_listens) AS algo_completed_listens,
                SUM(organic_completed_listens) AS organic_completed_listens,
                
                SUM(algo_short_listens) AS algo_short_listens,
                SUM(organic_short_listens) AS organic_short_listens,
                
                SUM(algo_likes) AS algo_likes,
                SUM(organic_likes) AS organic_likes,
                
                SUM(algo_unlikes) AS algo_unlikes,
                SUM(organic_unlikes) AS organic_unlikes,
                
                SUM(algo_dislikes) AS algo_dislikes,
                SUM(organic_dislikes) AS organic_dislikes,
                
                SUM(user_algo_unique) AS sum_algo_unique_items,
                SUM(user_organic_unique) AS sum_organic_unique_items
                
            FROM user_level_agg
            GROUP BY time_period, segment, content_tier
        )
        SELECT * FROM final_mart ORDER BY time_period, segment, content_tier
        
    ) TO '{(marts_dir / 'mart_unified.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    con.execute(query)
    logging.info("Mart compiled successfully.")
    shutil.rmtree(TMP_DIR, ignore_errors=True)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT))
    parser.add_argument("--marts-dir", type=Path, default=DEFAULT_MARTS_DIR)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    build_all(args.input, args.marts_dir.expanduser().resolve())
