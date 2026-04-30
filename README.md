# NYPD Motor Vehicle Collisions — ETL Pipeline

**Course:** CS-404 Big Data Analytics
**Instructor:** Ms. Zahida Kausar  
**Group:** Humna Tariq (460497) & Huda Noor Ahmad (458675)

---

## Project Overview

An end-to-end ETL pipeline for the NYPD Motor Vehicle Collisions — Crashes dataset. The pipeline ingests raw crash records, loads them into HDFS via Hadoop, and produces a data profiling report covering schema inspection, missing value analysis, statistical summaries, distribution plots, data quality audits, and a proposed cleaning strategy.

---

## Dataset

| Property | Detail |
|---|---|
| Name | NYPD Motor Vehicle Collisions — Crashes |
| Source | NYC Open Data |
| File | `./data/Motor_Vehicle_Collisions_Crashes.csv` |
| Rows | 2,255,537 |
| Columns | 29 |

Key columns: `CRASH DATE`, `BOROUGH`, `ZIP CODE`, `LATITUDE`, `LONGITUDE`, `COLLISION_ID`, injury/fatality counts, contributing factors, vehicle type codes.

---

## Setup Instructions

**Requirements:** Python 3.14, Hadoop 3.3.6

1. Clone/download the project folder.
2. Place the dataset CSV at `./data/Motor_Vehicle_Collisions_Crashes.csv`.
3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Ensure Hadoop is running and `$HADOOP_HOME` is set in your environment.

---

## How to Run `ingest.py`

`ingest.py` reads the CSV and uploads it to HDFS.

```bash
python ingest.py
```

The script will:
1. Load the CSV with `dtype=str` to preserve raw values.
2. Convert numeric and datetime columns explicitly.
3. Push the cleaned data to the configured HDFS path.

To verify the upload:
```bash
hdfs dfs -ls -R /warehouse/raw/nypd_collisions/```

---

## How to View the Profiling Report

Open the Jupyter notebook:

```bash
jupyter notebook profiling_report.ipynb
```

Then run all cells top-to-bottom (**Kernel → Restart & Run All**). The notebook produces:

| Section | Contents |
|---|---|
| 1 — Schema Description | Column names, dtypes, sample values |
| 2 — Missing Value Analysis | Null counts, percentages, bar chart |
| 3 — Statistical Summary | Mean, median, std, min, max |
| 4 — Distribution Analysis | Histograms/KDE for 5 key attributes |
| 5 — Data Quality Issues | Duplicates, invalid coords, outliers |
| 6 — Cleaning Strategy | Justified action for every issue found |

---

## Submission Details

| Item | Detail |
|---|---|
| Assignment | CS-404 BDA — Assignment 2 |
| Submitted by | Humna Tariq (460497), Huda Noor Ahmad (458675) |
| Submitted to | Ms. Zahida Kausar |
| Semester | Spring 2026, NUST SEECS |
| Files | `ingest.py`, `ingest.log`, `profiling_report.ipynb`, `README.md`, `requirements.txt`, `hdfs_screenshot.png`, `Task1_DatasetJustification_Humna_Huda.pdf` |
