import os
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw_videos.csv"
INGESTED_DATA_PATH = DATA_DIR / "ingested.csv"

def ingest_from_csv():
    """Ingest data from a local CSV file."""
    try:
        logger.info(f"Reading data from {RAW_DATA_PATH}")
        if not RAW_DATA_PATH.exists():
            logger.error(f"File not found: {RAW_DATA_PATH}")
            raise FileNotFoundError(f"The file {RAW_DATA_PATH} does not exist.")
        
        df = pd.read_csv(RAW_DATA_PATH)
        logger.info(f"Successfully read {len(df)} records from CSV")
        
        df.to_csv(INGESTED_DATA_PATH, index=False)
        logger.info(f"Data saved to {INGESTED_DATA_PATH}")
        
        return df
    except Exception as e:
        logger.error(f"Error during CSV ingestion: {str(e)}")
        raise

def ingest_from_youtube_api():
    logger.info("YouTube API ingestion not implemented yet")
    return None

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    
    df = ingest_from_csv()
    logger.info(f"Ingestion complete. Shape of data: {df.shape}")