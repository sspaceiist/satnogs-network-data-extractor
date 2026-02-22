"""
Generate Level 1 data from Level 0C by setting DAXSS UTC datetime as index and sorting.

This script reads Level 0C data from is1_health.db and creates a Level 1 table
with daxss_utc_datetime as the index, sorted chronologically.
"""

import sqlite3
import pandas as pd
from pathlib import Path


def load_level0c_data(db_path):
    """
    Load Level 0C packets from is1_health.db.
    
    Args:
        db_path: Path to is1_health.db
        
    Returns:
        pd.DataFrame: Packets data from level0c table
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM level0c", conn)
    conn.close()
    
    return df


def generate_level1(level0c_df):
    """
    Generate Level 1 data from Level 0C by setting DAXSS datetime as index and sorting.
    
    Args:
        level0c_df: DataFrame with Level 0C packet data
        
    Returns:
        pd.DataFrame: Level 1 data with daxss_utc_datetime as index, sorted chronologically
    """
    level1_df = level0c_df.copy()
    
    # Convert daxss_utc_datetime to datetime type if it's not already
    print(f"Converting daxss_utc_datetime to datetime format...")
    level1_df['daxss_utc_datetime'] = pd.to_datetime(level1_df['daxss_utc_datetime'])
    
    # Sort data by DAXSS UTC datetime
    print(f"Sorting {len(level1_df)} records by daxss_utc_datetime...")
    level1_df = level1_df.sort_values('daxss_utc_datetime')

    level1_df = level1_df[~level1_df.index.duplicated(keep="first")]

    
    # Set DAXSS UTC datetime as index
    print(f"Setting daxss_utc_datetime as index...")
    level1_df.set_index('daxss_utc_datetime', inplace=True)
    level1_df = level1_df[level1_df["daxss_year"] > 2000]
    
    return level1_df


def save_level1_data(level1_df, db_path):
    """
    Save Level 1 data to level1 table in is1_health.db.
    
    Args:
        level1_df: DataFrame with Level 1 data (indexed by daxss_utc_datetime)
        db_path: Path to is1_health.db
    """
    conn = sqlite3.connect(db_path)
    
    # Convert index to string for SQLite storage
    level1_df_to_save = level1_df.copy()
    level1_df_to_save.index = level1_df_to_save.index.astype(str)
    
    level1_df_to_save.to_sql('level1', conn, if_exists='replace', index=True, index_label='daxss_utc_datetime')
    conn.close()
    
    print(f"Level 1 data saved to {db_path} (table: level1)")


def main():
    """Main process to generate Level 1 from Level 0C."""
    # Define file paths
    script_dir = Path(__file__).parent
    db_path = script_dir / 'is1_health.db'
    
    # Verify input file exists
    if not db_path.exists():
        print(f"Error: {db_path} not found")
        return
    
    # Check if level0c table exists
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='level0c'")
    table_exists = cursor.fetchone() is not None
    conn.close()
    
    if not table_exists:
        print(f"Error: level0c table not found in {db_path}")
        print(f"Please run level0b_to_level0c.py first to generate level0c table")
        return
    
    print(f"Loading Level 0C data from {db_path} (table: level0c)...")
    level0c_df = load_level0c_data(db_path)
    
    print(f"Loaded {len(level0c_df)} packets")
    
    print(f"\nGenerating Level 1 data...")
    level1_df = generate_level1(level0c_df)
    
    print(f"\nSaving Level 1 data to {db_path} (table: level1)...")
    save_level1_data(level1_df, db_path)
    
    print("Level 1 generation complete!")
    print(f"\nSummary:")
    print(f"  Input records (Level 0C): {len(level0c_df)}")
    print(f"  Output records (Level 1): {len(level1_df)}")
    print(f"  Index: daxss_utc_datetime (sorted chronologically)")
    print(f"  Data columns: daxss_year, daxss_month, daxss_day,")
    print(f"                daxss_hour, daxss_minute, daxss_second + 133 telemetry fields")
    
    # Display sample data
    if len(level1_df) > 0:
        print(f"\nFirst 3 records:")
        print(level1_df.head(3))
        print(f"\nLast 3 records:")
        print(level1_df.tail(3))


if __name__ == '__main__':
    main()
