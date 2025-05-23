# YouTube Data Pipeline

A local data pipeline for analyzing YouTube video statistics.

## Project Overview

This project implements a data pipeline that:
1. Ingests YouTube video statistics data
2. Cleans and transforms the data
3. Loads it into a local SQLite database
4. Performs analysis and creates visualizations

## Project Structure

youtube-data-pipeline/
├── data/
│   └── raw_videos.csv       # Input data (you need to provide this)
├── scripts/
│   ├── ingest.py            # Data ingestion script
│   ├── clean_transform.py   # Data cleaning and transformation
│   └── load_to_db.py        # Database loading script
├── db/
│   └── youtube.db           # SQLite database (created by the pipeline)
├── analysis/
│   ├── visualize.py         # Visualization script
│   └── figures/             # Generated visualizations
├── run_pipeline.py          # Main script to run the entire pipeline
├── README.md                # This file
└── requirements.txt         # Python dependencies

```plaintext

## Setup and Installation

1. Clone this repository
2. Install the required dependencies:
 ```

pip install -r requirements.txt

```plaintext
3. Place your YouTube dataset in `data/raw_videos.csv`
- You can download a dataset from Kaggle, such as [YouTube Trending Videos](https://www.kaggle.com/datasets/datasnaek/youtube-new)

## Running the Pipeline

To run the complete pipeline:

```bash
python run_pipeline.py
 ```
```

Or run individual steps:

```bash
python scripts/ingest.py
python scripts/clean_transform.py
python scripts/load_to_db.py
python analysis/visualize.py
 ```

## Data Analysis
The pipeline generates several visualizations:

- Top videos by view count
- Average views by category
- Trends over time
These visualizations are saved in the analysis/figures/ directory.

## Extending the Pipeline
To extend this pipeline:

- Add more complex transformations in clean_transform.py
- Create additional visualizations in visualize.py
- Implement YouTube API data fetching in ingest.py
- Set up Airflow for scheduled runs