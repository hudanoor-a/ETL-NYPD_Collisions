"""
BDA ASSIGNMENT 2 FOR PROJECT: BY HUMNA AND HUDA
Dataset: NYPD Motor Vehicle Collisions - Crashes
Source: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
"""

import os
import sys
import logging
import subprocess
import pandas as pd
import chardet

# Configuration

HDFS_BIN = r"C:\hadoop\bin\hdfs"
HDFS_TARGET_DIR = "/warehouse/raw/nypd_collisions/year_2026/month_04/"
LOCAL_CSV = os.path.join(".", "data", "Motor_Vehicle_Collisions_Crashes.csv")

# Columns we consider essential for any downstream warehouse work
REQUIRED_COLUMNS = [
    "COLLISION_ID",
    "CRASH DATE",
    "BOROUGH",
    "ZIP CODE",
    "LATITUDE",
    "LONGITUDE",
]

# Logging
def setup_logging():
    """Set up logging to both the terminal and ingest.log."""
    logger = logging.getLogger("ingest")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Print INFO and above to the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Write everything (DEBUG and above) to the log file
    file_handler = logging.FileHandler("ingest.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

# Step 1 - Load

def load_csv(path, logger):
    """Read the CSV into a pandas DataFrame. Exits if the file is missing."""
    logger.info(f"Loading CSV from: {path}")

    if not os.path.exists(path):
        logger.error(f"File not found at path: {path}")
        sys.exit(1)

    # Read everything as strings to preserve raw values before any cleaning
    df = pd.read_csv(path, dtype=str, low_memory=False)
    logger.info(f"Loaded {len(df):,} rows successfully.")
    return df

# Step 2 - Validate

def validate(df, path, logger):
    """
    Run pre-upload checks on the file and the loaded DataFrame.

    Checks performed: File extension must be .csv, and file size is logged in MB. Encoding is detected using chardet, and all required columns must be present ++ no duplicate COLLISION_IDs allowed

    Returns true if all critical checks pass, false otherwise.
    """
    logger.info("Bismillah, starting pre-upload validation")
    all_good = True

    # Check 1: file extension
    _, ext = os.path.splitext(path)
    if ext.lower() != ".csv":
        logger.error(f"Wrong file extension: '{ext}' - only .csv is accepted.")
        all_good = False
    else:
        logger.info(f"File extension OK: '{ext}'")

    # Check 2: file size
    size_bytes = os.path.getsize(path)
    size_mb = size_bytes / (1024 * 1024)
    logger.info(f"File size: {size_mb:.2f} MB ({size_bytes:,} bytes)")

    # Check 3: encoding detection (read first 100 KB, ig should be enough for a reliable guess, but pls check this again huda)
    with open(path, "rb") as f:
        sample = f.read(100_000)
    result = chardet.detect(sample)
    detected_enc = result.get("encoding", "unknown")
    confidence = result.get("confidence", 0.0)
    logger.info(f"Detected encoding: {detected_enc} (confidence: {confidence:.0%})")

    # Check 4: required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        for col in missing_cols:
            logger.warning(f"Missing required column: '{col}'")
        all_good = False
    else:
        logger.info("All required columns are present.")

    # Check 5: duplicate COLLISION_IDs
    if "COLLISION_ID" in df.columns:
        duplicates = df["COLLISION_ID"].duplicated().sum()
        if duplicates > 0:
            logger.warning(f"Found {duplicates:,} duplicate COLLISION_ID(s).")
        else:
            logger.info("No duplicate COLLISION_IDs found.")

    if all_good:
        logger.info("Validation passed!! file is ready for upload.")
    else:
        logger.warning("Validation finished with warnings:(( Check log for details.")

    return all_good

# HDFS helpers
def run_hdfs_command(args, logger, label):
    """
    Execute an HDFS shell command via subprocess.
    args  : list of arguments that follow the hdfs binary
    label : short description used in log messages
    """

    full_cmd = f'"{HDFS_BIN}" {" ".join(args)}'
    logger.debug(f"Executing: {full_cmd}")

    proc = subprocess.run(
        full_cmd,
        shell=True,
        capture_output=True,
        text=True,
    )

    if proc.returncode == 0:
        logger.info(f"{label} — OK.")
        if proc.stdout.strip():
            logger.debug(f"Output: {proc.stdout.strip()}")
    else:
        logger.error(f"{label} — FAILED (exit code {proc.returncode}).")
        if proc.stderr.strip():
            logger.error(f"Error details: {proc.stderr.strip()}")

    return proc

# Step 3 - Create HDFS directory

def create_hdfs_directory(logger):
    """Create the partitioned warehouse directory on HDFS (skips if exists taake baar baar na hojaye)."""
    return run_hdfs_command(
        ["dfs", "-mkdir", "-p", f'"{HDFS_TARGET_DIR}"'],
        logger,
        f"Create HDFS dir {HDFS_TARGET_DIR}",
    )

# Step 4 - Upload
def upload_to_hdfs(local_path, logger):
    """Upload the local CSV to the HDFS target directory."""

    abs_path = os.path.abspath(local_path)
    filename = os.path.basename(local_path)
    return run_hdfs_command(
        ["dfs", "-put", "-f", f'"{abs_path}"', f'"{HDFS_TARGET_DIR}"'],
        logger,
        f"Upload {filename}",
    )

# Step 5 - Verification (huda pls isko ek baar phirse check kr lena, mujhe thoda doubt hai ki hdfs dfs -ls command ka output kaise aayega aur usme se file name kaise extract karna hai, ya phir bas pura output log kar dena chahiye)

def verify_upload(logger):
    """List the HDFS target directory to confirm the file landed correctly."""

    logger.info(f"Verifying upload at: {HDFS_TARGET_DIR}")
    proc = run_hdfs_command(
        ["dfs", "-ls", f'"{HDFS_TARGET_DIR}"'],
        logger,
        "HDFS directory listing",
    )
    if proc.stdout.strip():
        logger.info(f"Contents:\n{proc.stdout.strip()}")
    return proc


#main FUNCCC LESSGOOOO

if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Ingest pipeline started")

    # Step 1 — Load
    df = load_csv(LOCAL_CSV, logger)
    logger.info(f"Shape: {len(df):,} rows x {len(df.columns)} columns")

    # Step 2 — Validate
    validate(df, LOCAL_CSV, logger)

    # Step 3 — Create HDFS directory
    mkdir_proc = create_hdfs_directory(logger)
    if mkdir_proc.returncode != 0:
        logger.warning("mkdir returned non-zero — directory may already exist, continuing.")

    # Step 4 — Upload
    upload_proc = upload_to_hdfs(LOCAL_CSV, logger)
    if upload_proc.returncode != 0:
        logger.error("Upload failed — stopping pipeline.")
        sys.exit(1)

    # Step 5 — Verify
    verify_upload(logger)

    logger.info("Ingest pipeline completed successfully LESSGOOOO!!!")