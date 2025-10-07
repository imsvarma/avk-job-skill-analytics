import boto3
import pandas as pd
from io import StringIO

# ---------- CONFIG ----------
BUCKET_NAME = "job-skill-analytics"
RAW_PREFIX = "extract/raw/"
COMBINED_KEY = "extract/combined/combined.csv"

s3 = boto3.client('s3')

def list_files(prefix):
    """List all CSV files in a given prefix."""
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    return [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.csv')]

def read_csv_from_s3(key):
    """Read CSV file from S3 into DataFrame."""
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(obj['Body'])

def write_csv_to_s3(df, key):
    """Write DataFrame to S3 as CSV."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())

def main():
    all_files = list_files(RAW_PREFIX)
    print(f"📂 Found {len(all_files)} files in raw folder")

    # Try reading the existing combined file (if exists)
    try:
        combined_df = read_csv_from_s3(COMBINED_KEY)
        processed_files = set(combined_df['source_file'].unique())
    except Exception:
        combined_df = pd.DataFrame()
        processed_files = set()

    new_dataframes = []

    for key in all_files:
        if key not in processed_files:
            df = read_csv_from_s3(key)
            df['source_file'] = key  # track file origin
            new_dataframes.append(df)
            print(f"✅ Added new file: {key}")

    if new_dataframes:
        new_data = pd.concat(new_dataframes, ignore_index=True)
        updated_df = pd.concat([combined_df, new_data], ignore_index=True)
        write_csv_to_s3(updated_df, COMBINED_KEY)
        print("📦 Combined file updated successfully!")
    else:
        print("🚫 No new files to append.")

if __name__ == "__main__":
    main()
