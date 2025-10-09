# ======================================================
# 🧹 S3 Transformation Pipeline (Auto Detects Unprocessed Files)
# Author: Manikanta Sai Varma Indukuri
# ======================================================

import boto3
import pandas as pd
import io
import re
import logging
from datetime import datetime

# ------------------- CONFIG ---------------------------
BUCKET = "job-skill-analytics"
RAW_PREFIX = "extract/raw/"         # <-- change if your folder name differs
PROCESSED_PREFIX = "transformed/"
LOG_FILE = "s3_transform_pipeline.log"
FORCE_REPROCESS = False  # ✅ set True to force all files to reprocess

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------- AWS CLIENT -----------------------
s3 = boto3.client("s3")

# ------------------- UTILITIES ------------------------
def list_s3_files(prefix):
    """List all CSV files under a given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
    files = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                files.append(obj["Key"])
    return files


def infer_job_type(row):
    """Infer job type from job_type + job_title combined."""
    jt = str(row.get("job_type", "")).lower()
    title = str(row.get("job_title", "")).lower()
    text = f"{jt} {title}"

    types = set()
    if re.search(r"full[- ]?time", text):
        types.add("Full-Time")
    if re.search(r"part[- ]?time", text):
        types.add("Part-Time")
    if re.search(r"contract", text):
        types.add("Contract")
    if re.search(r"intern|internship", text):
        types.add("Internship")
    if re.search(r"temp|temporary", text):
        types.add("Temporary")
    if re.search(r"freelance|consult", text):
        types.add("Freelance")

    return ", ".join(sorted(types)) if types else "Not specified"


def transform_data(df):
    """Perform full cleaning & normalization."""
    # ---- Data type conversions ----
    df["job_posted_date"] = pd.to_datetime(df["job_posted_date"], errors="coerce")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    # ---- Text cleanup ----
    text_cols = ["company_name", "job_title", "job_type", "job_location", "job_posted_site"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    # ---- Infer job type intelligently ----
    df["job_type"] = df.apply(infer_job_type, axis=1)

    # ---- Handle missing values ----
    df.fillna({
        "company_name": "Unknown",
        "technical_skills": "Not listed",
        "soft_skills": "Not listed"
    }, inplace=True)

    # ---- Parse skills ----
    df["technical_skills"] = df["technical_skills"].apply(
        lambda x: [i.strip().lower() for i in x.split(",")] if isinstance(x, str) else []
    )
    df["soft_skills"] = df["soft_skills"].apply(
        lambda x: [i.strip().lower() for i in x.split(",")] if isinstance(x, str) else []
    )

    # ---- Drop duplicates ----
    df.drop_duplicates(subset=["company_name", "job_title", "job_location", "job_posted_site"], inplace=True)

    # ---- Filter salary outliers ----
    if "salary" in df.columns:
        df = df[(df["salary"] >= 20000) & (df["salary"] <= 400000)]

    # ---- Derived columns ----
    df["job_posted_year"] = df["job_posted_date"].dt.year
    df["city"] = df["job_location"].apply(lambda x: x.split(",")[0] if "," in x else x)

    return df


def process_file(file_key):
    """Download → transform → upload to processed folder."""
    logging.info(f"Processing file: {file_key}")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        df_transformed = transform_data(df)

        # Generate processed key (same name, different folder)
        output_key = file_key.replace(RAW_PREFIX, PROCESSED_PREFIX)

        buffer = io.StringIO()
        df_transformed.to_csv(buffer, index=False)
        s3.put_object(Bucket=BUCKET, Key=output_key, Body=buffer.getvalue().encode("utf-8"))

        logging.info(f"✅ Uploaded processed file: {output_key}")
        print(f"✅ Uploaded: {output_key}")

    except Exception as e:
        logging.error(f"❌ Error processing {file_key}: {e}")
        print(f"❌ Error processing {file_key}: {e}")


# ------------------- MAIN EXECUTION -------------------
if __name__ == "__main__":
    print("🔍 Checking for new raw files on S3...")

    raw_files = list_s3_files(RAW_PREFIX)
    processed_files = list_s3_files(PROCESSED_PREFIX)

    print(f"Raw files found: {len(raw_files)}")
    print(f"Processed files found: {len(processed_files)}")

    # Determine which raw files have not been processed
    if FORCE_REPROCESS:
        new_files = raw_files
        print("⚠️ FORCE_REPROCESS=True — all files will be reprocessed.")
    else:
        processed_set = set(processed_files)
        new_files = [
            f for f in raw_files
            if f.replace(RAW_PREFIX, PROCESSED_PREFIX) not in processed_set
        ]

    print(f"🆕 Files to process: {len(new_files)}")

    if not new_files:
        print("✅ No new unprocessed files found. Exiting.")
        logging.info("No new unprocessed files found.")
    else:
        for file_key in new_files:
            print(f"🚀 Processing {file_key}...")
            process_file(file_key)

    print("🎯 Pipeline completed.")
