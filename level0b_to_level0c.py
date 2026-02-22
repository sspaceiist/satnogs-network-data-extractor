"""
Convert Level 0B packet data to Level 0C by adding human-readable datetime fields.

This script reads Level 0B data from is1_health.db and converts DAXSS GPS timestamps
to Python datetime objects in UTC. The result is stored in the level0c table.

GPS Time Reference:
- GPS epoch: January 6, 1980 00:00:00 UTC
- DAXSS timestamp is in GPS seconds (seconds since GPS epoch)
- Leap seconds are accounted for to convert to UTC
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


# GPS epoch: January 6, 1980 00:00:00 UTC
GPS_EPOCH = datetime(1980, 1, 6, 0, 0, 0)

# Leap seconds as of the time this script was created
# This list contains (GPS week, leap seconds) pairs
# Current leap second count: 18 as of 2026
LEAP_SECONDS = 18


def gps_time_to_utc(gps_seconds):
    """
    Convert GPS time (seconds since GPS epoch) to UTC datetime.
    
    GPS time is ahead of UTC by the number of leap seconds that have occurred
    since the GPS epoch (Jan 6, 1980).
    
    Args:
        gps_seconds: Number of seconds since GPS epoch (Jan 6, 1980 00:00:00 UTC)
        
    Returns:
        datetime: UTC datetime object, or None if input is None or invalid
    """
    if gps_seconds is None:
        return None
    
    try:
        # GPS time includes leap seconds, so to get UTC we subtract leap seconds
        gps_seconds_as_float = float(gps_seconds)
        
        # GPS time = UTC time + leap seconds
        # Therefore: UTC time = GPS time - leap seconds
        utc_seconds = gps_seconds_as_float - LEAP_SECONDS
        
        # Create datetime from GPS epoch plus UTC seconds
        utc_datetime = GPS_EPOCH + timedelta(seconds=utc_seconds)
        
        return utc_datetime
    except (TypeError, ValueError):
        return None


def get_daxss_timestamp_column_name(db_path):
    """
    Determine the actual column name for DAXSS timestamp from the database.
    
    Args:
        db_path: Path to is1_health.db
        
    Returns:
        str: Column name containing DAXSS timestamp, or None if not found
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(level0b)")
    columns = cursor.fetchall()
    conn.close()
    
    # Look for DAXSS timestamp column
    for col in columns:
        col_name = col[1]
        if 'daxss' in col_name.lower() and 'time' in col_name.lower():
            return col_name
    
    return None


def load_level0b_packets(db_path):
    """
    Load Level 0B packets from is1_health.db.
    
    Args:
        db_path: Path to is1_health.db
        
    Returns:
        pd.DataFrame: Packets data from level0b table
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM level0b", conn)
    conn.close()
    
    return df


def convert_level0b_to_level0c(level0b_df, daxss_col_name):
    """
    Convert Level 0B data to Level 0C by adding datetime fields.
    
    Args:
        level0b_df: DataFrame with Level 0B packet data
        daxss_col_name: Name of DAXSS timestamp column
        
    Returns:
        pd.DataFrame: Level 0C data with added datetime columns
    """
    level0c_df = level0b_df.copy()
    
    # Convert DAXSS timestamp to UTC datetime
    print(f"Converting DAXSS GPS timestamps to UTC datetime...")
    level0c_df['daxss_utc_datetime'] = level0b_df[daxss_col_name].apply(gps_time_to_utc)
    
    # Extract datetime components for easier querying
    level0c_df['daxss_year'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.year if x is not None else None
    )
    level0c_df['daxss_month'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.month if x is not None else None
    )
    level0c_df['daxss_day'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.day if x is not None else None
    )
    level0c_df['daxss_hour'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.hour if x is not None else None
    )
    level0c_df['daxss_minute'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.minute if x is not None else None
    )
    level0c_df['daxss_second'] = level0c_df['daxss_utc_datetime'].apply(
        lambda x: x.second if x is not None else None
    )
    
    return level0c_df


def save_level0c_data(level0c_df, db_path):
    """
    Save Level 0C data to level0c table in is1_health.db.
    
    Args:
        level0c_df: DataFrame with Level 0C data
        db_path: Path to is1_health.db
    """
    conn = sqlite3.connect(db_path)
    
    # Convert datetime columns to strings for SQLite storage
    # (SQLite stores datetime as TEXT in ISO format)
    level0c_df_to_save = level0c_df.copy()
    level0c_df_to_save['daxss_utc_datetime'] = level0c_df_to_save['daxss_utc_datetime'].apply(
        lambda x: x.isoformat() if x is not None else None
    )
    
    level0c_df_to_save.to_sql('level0c', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Level 0C data saved to {db_path} (table: level0c)")


def main():
    """Main conversion process."""
    # Define file paths
    script_dir = Path(__file__).parent
    db_path = script_dir / 'is1_health.db'
    
    # Verify input file exists
    if not db_path.exists():
        print(f"Error: {db_path} not found")
        return
    
    print(f"Loading Level 0B packets from {db_path} (table: level0b)")
    level0b_df = load_level0b_packets(db_path)
    
    print(f"Loaded {len(level0b_df)} packets")
    
    # Find DAXSS timestamp column
    daxss_col_name = get_daxss_timestamp_column_name(db_path)
    if daxss_col_name is None:
        print("Error: Could not find DAXSS timestamp column in level0b table")
        return
    
    print(f"Found DAXSS timestamp column: '{daxss_col_name}'")
    
    print(f"\nConverting {len(level0b_df)} packets to Level 0C...")
    level0c_df = convert_level0b_to_level0c(level0b_df, daxss_col_name)
    
    print(f"Saving Level 0C data to {db_path} (table: level0c)")
    save_level0c_data(level0c_df, db_path)
    
    print("Conversion complete!")
    print(f"\nSummary:")
    print(f"  Input records: {len(level0b_df)}")
    print(f"  Output records: {len(level0c_df)}")
    print(f"  New columns: daxss_utc_datetime, daxss_year, daxss_month, daxss_day,")
    print(f"               daxss_hour, daxss_minute, daxss_second")
    
    # Display sample conversions
    print(f"\nSample conversions (first 5 records):")
    print(f"  GPS Time (seconds)  →  UTC Datetime")
    print(f"  " + "-" * 50)
    for i in range(min(5, len(level0c_df))):
        gps_time = level0b_df.iloc[i][daxss_col_name]
        utc_dt = level0c_df.iloc[i]['daxss_utc_datetime']
        if gps_time is not None and utc_dt is not None:
            print(f"  {gps_time:>18.1f}  →  {utc_dt}")
        else:
            print(f"  {gps_time!s:>18}  →  {utc_dt}")


if __name__ == '__main__':
    main()
