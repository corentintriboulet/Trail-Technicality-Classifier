import sys
from pathlib import Path
import pandas as pd
import numpy as np
from src.data.Tenth_Best_Time_Estimator import TenthBestTimeEstimator

def SegmentCleaner(input_path, output_path, repo_root):
    """
    Preprocess raw segment data by filling missing tenth_best_time values.
    
    Args:
        input_path: Path to raw parquet file
        output_path: Path to save cleaned parquet file
        repo_root: Repository root directory path
    """
    # Load raw data
    raw_parquet = pd.read_parquet(input_path)
    df_parquet = pd.DataFrame(raw_parquet)
    
    # Initialize estimators for both activity types
    tbte_ride = TenthBestTimeEstimator(repo_root / "src" / "models" / "missing_tenth_best_time_estimator_ride.pkl")
    tbte_run = TenthBestTimeEstimator(repo_root / "src" / "models" / "missing_tenth_best_time_estimator_run.pkl")
    
    # Separate data with missing tenth_best_time by activity type
    df_parquet_missing_tbt = df_parquet[df_parquet['tenth_best_time'].isna()]
    df_parquet_missing_tbt_ride = df_parquet_missing_tbt[df_parquet_missing_tbt['activity_type'] == 'Ride'].copy()
    df_parquet_missing_tbt_run = df_parquet_missing_tbt[df_parquet_missing_tbt['activity_type'] == 'Run'].copy()
    
    # Fill missing values using trained models
    df_parquet_adding_tbt_ride = tbte_ride.add_tenth_best_time(df_parquet_missing_tbt_ride)
    df_parquet_adding_tbt_run = tbte_run.add_tenth_best_time(df_parquet_missing_tbt_run)
    
    # Combine existing and filled data
    df_existing = df_parquet.dropna(subset=['tenth_best_time'])
    df_parquet_clean = pd.concat([df_existing, df_parquet_adding_tbt_ride, df_parquet_adding_tbt_run], ignore_index=True)
    
    # Drop unnecessary columns
    df_parquet_clean = df_parquet_clean.drop(columns=["log_efforts", "log_athletes"], errors='ignore')
    
    # Rename id column if present
    if 'id' in df_parquet_clean.columns:
        df_parquet_clean = df_parquet_clean.rename(columns={"id": "segment_id"})
    
    # Save cleaned data
    df_parquet_clean.to_parquet(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[2]
    input_path = repo_root / "data" / "raw" / "reunion_segments.parquet"
    output_path = repo_root / "data" / "processed" / "reunion_segments_cleaned.parquet"
    
    SegmentCleaner(input_path, output_path, repo_root)