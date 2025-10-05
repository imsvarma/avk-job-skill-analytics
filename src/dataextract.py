# -*- coding: utf-8 -*-
"""
Created on Sat Oct  4 03:39:10 2025

@author: imani
"""

import pandas as pd
import os, re, time
from tqdm import tqdm
import openai
from datetime import datetime

# ---------- CONFIG ----------
openai.api_key = "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"   # 🔑 Add your key here
ARCHIVE_PATH = "archive"
EXPORT_PATH = "daily_exports"
NUM_JOBS = 10               # total output size
KAGGLE_SHARE = 0.7          # 70% Kaggle, 30% HuggingFace
COUNTRY_FILTER = ["usa", "us", "united states"]

# ---------- UTILITIES ----------
def progress_bar(stage, total_stages):
    percent = int((stage / total_stages) * 100)
    bar = "█" * (percent // 5) + "-" * (20 - percent // 5)
    print(f"[{bar}] {percent}% - Step {stage}/{total_stages}")

# ---------- STEP 1: Load Kaggle dataset (.csv or .xlsx) ----------
print("📂 Loading Kaggle dataset...")
progress_bar(1, 7)

kaggle_files = [
    os.path.join(ARCHIVE_PATH, f)
    for f in os.listdir(ARCHIVE_PATH)
    if f.endswith((".csv", ".xlsx"))
]
if not kaggle_files:
    raise FileNotFoundError(f"❌ No files in {os.path.abspath(ARCHIVE_PATH)}")

frames = []
for f in kaggle_files:
    print(f"📄 Reading {os.path.basename(f)} ...")
    if f.endswith(".csv"):
        frames.append(pd.read_csv(f, low_memory=False))
    else:
        frames.append(pd.read_excel(f))
kaggle_df = pd.concat(frames, ignore_index=True)
print(f"✅ Kaggle loaded: {len(kaggle_df)} rows, {len(kaggle_df.columns)} columns")

# ---------- STEP 2: Load Hugging Face dataset ----------
progress_bar(2, 7)
print("📥 Loading Hugging Face dataset...")
try:
    from datasets import load_dataset
    hf = load_dataset("lukebarousse/data_jobs")
    hf_df = pd.DataFrame(hf["train"])
    print(f"✅ Hugging Face loaded: {len(hf_df)} rows")
except Exception as e:
    print("⚠️ Hugging Face load failed:", e)
    hf_df = pd.DataFrame()

# ---------- STEP 3: Filter U.S. jobs ----------
progress_bar(3, 7)
print("🇺🇸 Filtering U.S. jobs...")

def filter_usa(df, country_col, loc_col):
    cols = [c.lower() for c in df.columns]
    if country_col and country_col in df.columns:
        df = df[df[country_col].astype(str).str.lower().isin(COUNTRY_FILTER)]
    elif loc_col and loc_col in df.columns:
        df = df[df[loc_col].astype(str).str.contains(r"\b(US|United States|USA)\b", case=False, na=False)]
    return df

kaggle_df = filter_usa(kaggle_df, "country" if "country" in kaggle_df.columns else None,
                       "location" if "location" in kaggle_df.columns else "job_location")
hf_df = filter_usa(hf_df, "job_country", "job_location")
print(f"✅ Kaggle after filter: {len(kaggle_df)} | Hugging Face: {len(hf_df)}")

# ---------- STEP 4: Random Sampling (70/30 split) ----------
progress_bar(4, 7)
kaggle_sample = kaggle_df.sample(int(NUM_JOBS * KAGGLE_SHARE), random_state=42)
hf_sample = hf_df.sample(NUM_JOBS - len(kaggle_sample), random_state=42)
print(f"🎲 Sampling {len(kaggle_sample)} Kaggle + {len(hf_sample)} Hugging Face")

# ---------- STEP 5: AI Skill Extraction from Description ----------
progress_bar(5, 7)
print("🧠 Extracting technical & soft skills using AI...")

def extract_skills(text):
    if not isinstance(text, str) or len(text.strip()) < 30:
        return "", ""
    prompt = f"""
    Extract two comma-separated lists from this job description:
    1. Technical skills (languages, tools, frameworks)
    2. Soft skills (communication, teamwork, leadership, problem-solving)

    Description: {text[:4000]}
    Format strictly as:
    TECH: [..]
    SOFT: [..]
    """
    try:
        res = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4
        )
        reply = res.choices[0].message["content"]
        tech, soft = "", ""
        if "TECH:" in reply:
            parts = reply.split("SOFT:")
            tech = parts[0].replace("TECH:", "").strip(" []\n")
            soft = parts[1].strip(" []\n") if len(parts) > 1 else ""
        return tech, soft
    except Exception:
        return "", ""

tqdm.pandas()

# Kaggle → use description column
desc_col_kaggle = "description" if "description" in kaggle_sample.columns else None
if desc_col_kaggle:
    kaggle_sample[["technical_skills", "soft_skills"]] = kaggle_sample[desc_col_kaggle].progress_apply(
        lambda x: pd.Series(extract_skills(x))
    )
else:
    kaggle_sample["technical_skills"], kaggle_sample["soft_skills"] = "", ""

# Hugging Face → use job_type_skills / job_skills as text
desc_col_hf = "job_type_skills" if "job_type_skills" in hf_sample.columns else "job_skills"
hf_sample[["technical_skills", "soft_skills"]] = hf_sample[desc_col_hf].progress_apply(
    lambda x: pd.Series(extract_skills(str(x)))
)

# Fill blanks
for df in [kaggle_sample, hf_sample]:
    df["soft_skills"] = df["soft_skills"].replace("", "communication, teamwork")
    df["source"] = "Kaggle" if df is kaggle_sample else "HuggingFace"

# ---------- STEP 6: Clean and Standardize ----------
progress_bar(6, 7)
print("🧹 Cleaning and standardizing columns...")

FINAL_COLS = [
    "company_name", "job_title", "job_type", "job_location", "country",
    "salary", "job_posted_date", "job_posted_site",
    "technical_skills", "soft_skills", "source"
]

def normalize(df, colmap):
    out = pd.DataFrame()
    for col, src in colmap.items():
        out[col] = df[src] if src in df.columns else ""
    return out

kaggle_map = {
    "company_name": "company" if "company" in kaggle_df.columns else "company_name",
    "job_title": "title" if "title" in kaggle_df.columns else "job_title",
    "job_type": "job_type" if "job_type" in kaggle_df.columns else "",
    "job_location": "location" if "location" in kaggle_df.columns else "job_location",
    "country": "country" if "country" in kaggle_df.columns else "",
    "salary": "mean_salary" if "mean_salary" in kaggle_df.columns else "salary",
    "job_posted_date": "date_posted" if "date_posted" in kaggle_df.columns else "job_posted_date",
    "job_posted_site": "site" if "site" in kaggle_df.columns else "",
    "technical_skills": "technical_skills",
    "soft_skills": "soft_skills",
    "source": "source"
}
hf_map = {
    "company_name": "company_name",
    "job_title": "job_title",
    "job_type": "job_title_short",
    "job_location": "job_location",
    "country": "job_country",
    "salary": "salary_year_avg",
    "job_posted_date": "job_posted_date",
    "job_posted_site": "job_via",
    "technical_skills": "technical_skills",
    "soft_skills": "soft_skills",
    "source": "source"
}

kaggle_std = normalize(kaggle_sample, kaggle_map)
hf_std = normalize(hf_sample, hf_map)

# Fill country blanks
for df in [kaggle_std, hf_std]:
    df["country"] = df["country"].replace("", "United States")

# Normalize salary
def normalize_salary(x):
    try:
        x = float(str(x).replace(",", "").replace("$", "").strip())
        return int(x) if x > 1000 else int(x * 2000)
    except:
        return ""
for df in [kaggle_std, hf_std]:
    df["salary"] = df["salary"].apply(normalize_salary)

combined = pd.concat([kaggle_std, hf_std], ignore_index=True)
print(f"🧩 Combined final dataset: {combined.shape}")

# ---------- STEP 7: Export Results ----------
progress_bar(7, 7)

# ---------- STEP 7: Export Results ----------
import boto3
from io import StringIO
from datetime import datetime

# AWS S3 configuration
bucket_name = "job-skill-analytics"
date_str = datetime.now().strftime("%Y-%m-%d")
file_name = f"fetch_jobs_{date_str}.csv"
s3_key = f"extract/raw/{file_name}"

# Convert DataFrame to CSV in memory
csv_buffer = StringIO()
combined.to_csv(csv_buffer, index=False)

# Upload directly to S3
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket_name,
    Key=s3_key,
    Body=csv_buffer.getvalue(),
    ACL="public-read",      # makes it publicly accessible
    ContentType="text/csv"
)

# Public URL
public_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
print(f"\n✅ Uploaded {len(combined)} jobs → {public_url}")
