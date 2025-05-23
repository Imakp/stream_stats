import sqlite3
import pandas as pd
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_DIR = BASE_DIR / "db"
CLEANED_DATA_PATH = DATA_DIR / "cleaned.csv"
DB_PATH = DB_DIR / "youtube.db"

def load_to_db():
    try:
        logger.info(f"Reading cleaned data from {CLEANED_DATA_PATH}")
        if not CLEANED_DATA_PATH.exists():
            logger.error(f"File not found: {CLEANED_DATA_PATH}")
            raise FileNotFoundError(f"The file {CLEANED_DATA_PATH} does not exist.")
        
        df = pd.read_csv(CLEANED_DATA_PATH)
        logger.info(f"Successfully read {len(df)} records")
        
        os.makedirs(DB_DIR, exist_ok=True)
        
        logger.info(f"Connecting to database at {DB_PATH}")
        conn = sqlite3.connect(str(DB_PATH))
        
        logger.info("Loading data into video_stats table")
        df.to_sql("video_stats", conn, if_exists="replace", index=False)
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM video_stats")
        count = cursor.fetchone()[0]
        logger.info(f"Loaded {count} records into the database")
        
        logger.info("Creating indices")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_id ON video_stats (video_id)")
        if 'publish_date' in df.columns:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_publish_date ON video_stats (publish_date)")
        if 'category_id' in df.columns:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON video_stats (category_id)")
        
        conn.close()
        logger.info("Database loading complete")
        
        return True
    except Exception as e:
        logger.error(f"Error during database loading: {str(e)}")
        raise

if __name__ == "__main__":
    success = load_to_db()
    if success:
        logger.info("Data successfully loaded to database")