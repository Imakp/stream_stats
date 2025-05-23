import logging
import subprocess
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / "scripts"
ANALYSIS_DIR = BASE_DIR / "analysis"

def run_script(script_path):
    try:
        logger.info(f"Running {script_path}")
        result = subprocess.run(
            ["python", str(script_path)],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Successfully completed {script_path}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_path}: {e}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False

def run_pipeline():
    logger.info("Starting YouTube data pipeline")
    
    ingest_script = SCRIPTS_DIR / "ingest.py"
    if not run_script(ingest_script):
        logger.error("Data ingestion failed. Stopping pipeline.")
        return False
    
    clean_script = SCRIPTS_DIR / "clean_transform.py"
    if not run_script(clean_script):
        logger.error("Data cleaning failed. Stopping pipeline.")
        return False
    
    load_script = SCRIPTS_DIR / "load_to_db.py"
    if not run_script(load_script):
        logger.error("Database loading failed. Stopping pipeline.")
        return False
    
    visualize_script = ANALYSIS_DIR / "visualize.py"
    if not run_script(visualize_script):
        logger.error("Data visualization failed.")
    
    logger.info("Pipeline completed successfully")
    return True

if __name__ == "__main__":
    success = run_pipeline()
    if success:
        logger.info("YouTube data pipeline executed successfully")
    else:
        logger.error("YouTube data pipeline failed")