import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
INGESTED_DATA_PATH = DATA_DIR / "ingested.csv"
CLEANED_DATA_PATH = DATA_DIR / "cleaned.csv"

def clean_transform():
    try:
        logger.info(f"Reading ingested data from {INGESTED_DATA_PATH}")
        if not INGESTED_DATA_PATH.exists():
            logger.error(f"File not found: {INGESTED_DATA_PATH}")
            raise FileNotFoundError(f"The file {INGESTED_DATA_PATH} does not exist.")
        
        df = pd.read_csv(INGESTED_DATA_PATH)
        logger.info(f"Successfully read {len(df)} records")
        
        logger.info("Starting data cleaning and transformation")
        
        if 'publish_time' in df.columns:
            df['publish_time'] = pd.to_datetime(df['publish_time'])
            df['publish_date'] = df['publish_time'].dt.date
            df['publish_hour'] = df['publish_time'].dt.hour
            df['publish_day_of_week'] = df['publish_time'].dt.dayofweek
            logger.info("Date columns processed")
        
        numeric_cols = ['views', 'likes', 'dislikes', 'comment_count']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if set(numeric_cols).issubset(df.columns):
            df = df.dropna(subset=numeric_cols)
            logger.info(f"Dropped rows with missing values in critical columns")
        
        if all(col in df.columns for col in ['views', 'likes', 'dislikes']):
            df['engagement_ratio'] = (df['likes'] + df['dislikes']) / df['views'].replace(0, np.nan)
            df['like_ratio'] = df['likes'] / (df['likes'] + df['dislikes']).replace(0, np.nan)
            logger.info("Engagement metrics calculated")
        
        df.to_csv(CLEANED_DATA_PATH, index=False)
        logger.info(f"Cleaned data saved to {CLEANED_DATA_PATH}")
        
        return df
    except Exception as e:
        logger.error(f"Error during data cleaning: {str(e)}")
        raise

if __name__ == "__main__":
    df = clean_transform()
    logger.info(f"Data cleaning complete. Shape of cleaned data: {df.shape}")