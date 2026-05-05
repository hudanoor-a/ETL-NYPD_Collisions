# NYPD Motor Vehicle Collisions Data Warehouse & Analytics Pipeline

## Project Overview
This project implements a complete big data pipeline using the NYPD Motor Vehicle Collisions dataset, covering the full lifecycle from ingestion to analytics.
The pipeline is divided into three milestones:

* Milestone 1 (M1) — Data Ingestion into HDFS
* Milestone 2 (M2) — ETL Pipeline & Star Schema Warehouse
* Milestone 3 (M3) — Analytics, Visualizations & Optimization

The final system processes raw CSV data into a dimensional warehouse (Star Schema) and generates business insights using Spark SQL and visualizations.

## Dataset
* **Dataset:** NYPD Motor Vehicle Collisions — Crashes
* **Source:** NYC Open Data
* **Format:** CSV
* **Size:** ~2.25 million rows, 29 columns
* **Domain:** Road accidents, injuries, fatalities, vehicle types, contributing factors

## Technologies Used
* Python 3.12
* PySpark 3.5.1
* Hadoop 3.3.6
* HDFS
* Spark SQL
* Pandas
* Matplotlib
* Windows OS

## Project Architecture
1.  Raw CSV
2.  HDFS Raw Layer
3.  PySpark ETL Pipeline
4.  Star Schema Warehouse (Fact + Dimensions)
5.  Parquet Files in HDFS
6.  Spark SQL Analytics
7.  Visualizations + Insights

## Milestone Summary

### Milestone 1 — Data Ingestion
**Tasks completed:**
* Verified dataset structure
* Uploaded dataset to HDFS
* Validated HDFS storage

**Raw HDFS Path:**
`/warehouse/raw/nypd_collisions/year_2026/month_04/`


### Milestone 2 — ETL & Data Warehouse
**Tasks completed:**
* Removed null/blank COLLISION_ID
* Removed duplicates
* Cleaned invalid coordinates
* Parsed date/time
* Added `property_damage_only` and `injury_count_flagged`
* Built Star Schema

**Star Schema**
* **Fact Table:** `fact_crash`
* **Dimension Tables:** `dim_date`, `dim_location`, `dim_vehicle`, `dim_factor`

**Processed HDFS Path:**
`/warehouse/processed/nypd_collisions/`

**Optimized HDFS Path:**
`/warehouse/processed_optimized/nypd_collisions/`

* Successful ETL execution
![Alt text](outputs\4.png)
* Processed HDFS directory
![Alt text](outputs\hdfs-processed.png)



### Milestone 3 — Analytics & Insights
**Tasks completed:**
* Loaded warehouse from HDFS
* Joined fact + dimensions
* Executed Spark SQL queries
* Generated visualizations
* Produced insights
* Implemented optimization techniques

## Business Questions Answered
* **Q1 — Borough Fatality Trends:** Analyzed fatality rates across boroughs over time.
* **Q2 — Pedestrian Injury Peaks:** Identified peak hours and contributing factors.
* **Q3 — Vehicle Type vs Crash Severity:** Compared fatal vs non-fatal crashes by borough.
* **Q4 — Cyclist Injury Trends:** Analyzed monthly trends and pre/post-2019 patterns.
* **Q5 — Severe Multi-Vehicle Crashes:** Identified key contributing factors in high-injury crashes.

## Visualizations Generated
* **Line Chart** -> Fatality trends
* **Heatmap** -> Pedestrian injuries
* **Stacked Bar** -> Crash severity
* **Time-Series Line** -> Cyclist trends
* **Bar Chart** -> Contributing factors
* **Scatter Plot** -> Geographic density
* **Dashboard** -> Combined summary

![Alt text](m3_outputs\charts\dashboard.png)

## Optimization Implemented
1.  **Partitioning:** Fact table written as `partition_year` and `partition_month`.
2.  **Caching:** Used `.cache()` for repeated queries.
3.  **Broadcast Joins:** Used for joining small dimension tables.
4.  **Performance Comparison:** Measured execution time before/after caching.

## Output Structure
* **Raw Layer:** `/warehouse/raw/nypd_collisions/year_2026/month_04/`
* **Processed Warehouse:** `/warehouse/processed/nypd_collisions/`
* **Optimized Warehouse:** `/warehouse/processed_optimized/nypd_collisions/`

## How to Run
1.  **Start Hadoop**
    ```bash
    cd C:\hadoop-3.3.6\sbin
    start-dfs.cmd
    start-yarn.cmd
    ```
2.  **Run Optimized ETL**
    ```bash
    cd /d "D:\sem 6\BDA\project\prj"
    python optimized_pipeline.py
    ```
3.  **Run Analytics**
    ```bash
    python analytics.py
    ```

## M3 Output Folder
`m3_outputs/`
├── `charts/`
├── `csv/`
└── `text/`

**Charts**
* q1_borough_fatality_rate.png
* q2_pedestrian_injuries_heatmap.png
* q3_fatal_nonfatal_stacked.png
* q4_cyclist_injury_trend.png
* q5_multi_vehicle_factors.png
* summary_dashboard.png
* bonus_geo_density_scatter.png

**Text Outputs**
* insights_summary.txt
* performance_notes.txt
* chart_interpretations.txt

## Required Proofs
* M2 ETL success
* M2 processed HDFS
* M3 analytics success
* M3 charts
* M3 partitioned HDFS

## Key Design Decisions
* Star schema for efficient analytics
* Surrogate keys for dimensions
* Null-safe joins using normalized columns
* Parquet storage for performance
* Partitioned fact table
* Spark SQL for analytics

## Challenges Faced
* Hadoop setup on Windows
* Spark–Java compatibility
* HDFS command issues
* Join duplication problems
* Chart readability issues

## Final Status
* M1 Completed
* M2 Completed
* M3 Completed
* Optimized Pipeline Completed
* Analytics Outputs Generated

## GitHub Repository
<PASTE_YOUR_GITHUB_REPO_LINK_HERE>

## Team Members
Huda Noor - 458675

Humna Tariq - 460497
