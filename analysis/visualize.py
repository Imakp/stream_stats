import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DB_DIR = BASE_DIR / "db"
ANALYSIS_DIR = BASE_DIR / "analysis"
DB_PATH = DB_DIR / "youtube.db"
FIGURES_DIR = ANALYSIS_DIR / "figures"

def connect_to_db():
    try:
        logger.info(f"Connecting to database at {DB_PATH}")
        conn = sqlite3.connect(str(DB_PATH))
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def top_videos_by_views(conn, limit=10):
    try:
        logger.info(f"Querying top {limit} videos by views")
        query = f"""
        SELECT title, views 
        FROM video_stats 
        ORDER BY views DESC 
        LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("No data returned for top videos query")
            return
        
        plt.figure(figsize=(12, 8))
        bars = plt.barh(df['title'], df['views'])
        plt.xlabel('Views')
        plt.title(f'Top {limit} Most Viewed Videos')
        plt.gca().invert_yaxis()
        
        for bar in bars:
            width = bar.get_width()
            label_x_pos = width * 1.01
            plt.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{width:,.0f}',
                    va='center')
        
        plt.tight_layout()
        
        figure_path = FIGURES_DIR / "top_videos_by_views.png"
        plt.savefig(figure_path)
        logger.info(f"Figure saved to {figure_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error creating top videos visualization: {str(e)}")

def category_analysis(conn):
    try:
        logger.info("Analyzing video statistics by category")
        query = """
        SELECT category_id, 
               COUNT(*) as video_count,
               AVG(views) as avg_views,
               AVG(likes) as avg_likes,
               AVG(dislikes) as avg_dislikes
        FROM video_stats
        GROUP BY category_id
        ORDER BY avg_views DESC
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("No data returned for category analysis query")
            return
        
        plt.figure(figsize=(12, 8))
        sns.barplot(x='category_id', y='avg_views', data=df)
        plt.title('Average Views by Category')
        plt.xlabel('Category ID')
        plt.ylabel('Average Views')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        figure_path = FIGURES_DIR / "avg_views_by_category.png"
        plt.savefig(figure_path)
        logger.info(f"Figure saved to {figure_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error creating category analysis visualization: {str(e)}")

def time_trend_analysis(conn):
    try:
        logger.info("Analyzing trends over time")
        query = """
        SELECT publish_date, 
               COUNT(*) as video_count,
               AVG(views) as avg_views
        FROM video_stats
        GROUP BY publish_date
        ORDER BY publish_date
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            logger.warning("No data returned for time trend analysis query")
            return
        
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        
        plt.figure(figsize=(14, 8))
        plt.plot(df['publish_date'], df['avg_views'], marker='o', linestyle='-')
        plt.title('Average Views Over Time')
        plt.xlabel('Publish Date')
        plt.ylabel('Average Views')
        plt.grid(True, alpha=0.3)
        
        plt.gcf().autofmt_xdate()
        
        plt.tight_layout()
        
        figure_path = FIGURES_DIR / "avg_views_over_time.png"
        plt.savefig(figure_path)
        logger.info(f"Figure saved to {figure_path}")
        
        plt.close()
    except Exception as e:
        logger.error(f"Error creating time trend visualization: {str(e)}")

def run_all_visualizations():
    try:
        FIGURES_DIR.mkdir(parents=True, exist_ok=True)
        
        conn = connect_to_db()
        
        top_videos_by_views(conn)
        category_analysis(conn)
        time_trend_analysis(conn)
        
        conn.close()
        logger.info("All visualizations completed")
    except Exception as e:
        logger.error(f"Error running visualizations: {str(e)}")

if __name__ == "__main__":
    run_all_visualizations()