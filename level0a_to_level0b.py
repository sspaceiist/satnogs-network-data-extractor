"""
Convert Level 0A packet data to Level 0B by applying conversion coefficients.

This script loads raw Level 0A packets from level0a.db and applies the conversion
formulas defined in beacon_conversion.json to produce calibrated Level 0B data.
"""

import json
import sqlite3
import pandas as pd
from pathlib import Path


def load_conversion_coefficients(json_file):
    """
    Load conversion coefficients from beacon_conversion.json.
    
    Args:
        json_file: Path to beacon_conversion.json
        
    Returns:
        dict: Mapping from field index to conversion parameters
    """
    with open(json_file, 'r') as f:
        conversions = json.load(f)
    
    # Convert string keys to integers for easier lookup
    conversion_map = {}
    for idx_str, params in conversions.items():
        idx = int(idx_str)
        conversion_map[idx] = params
    
    return conversion_map


def get_field_names_from_db(db_path):
    """
    Get all field names from the level0a table in is1_health.db.
    
    Args:
        db_path: Path to is1_health.db
        
    Returns:
        list: Column names from level0a table
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(level0a)")
    columns = cursor.fetchall()
    conn.close()
    
    return [col[1] for col in columns]


def apply_conversion(raw_value, coefficients):
    """
    Apply polynomial conversion formula to raw value.
    
    Formula: converted_value = C0 + C1*raw + C2*raw^2 + C3*raw^3 + C4*raw^4
    
    Args:
        raw_value: Raw sensor value
        coefficients: Dict with Conversion_C0 through Conversion_C4
        
    Returns:
        float: Converted value, or None if coefficients are missing or raw_value is None
    """
    if raw_value is None:
        return None
    
    # Check if coefficients are available
    c_values = [
        coefficients.get('Conversion_C0'),
        coefficients.get('Conversion_C1'),
        coefficients.get('Conversion_C2'),
        coefficients.get('Conversion_C3'),
        coefficients.get('Conversion_C4')
    ]
    
    # If all coefficients are None, return raw value unchanged
    if all(c is None for c in c_values):
        return raw_value
    
    # Fill None coefficients with 0
    c_values = [c if c is not None else 0 for c in c_values]
    
    # Apply polynomial conversion
    c0, c1, c2, c3, c4 = c_values
    converted = (c0 + 
                c1 * raw_value + 
                c2 * (raw_value ** 2) + 
                c3 * (raw_value ** 3) + 
                c4 * (raw_value ** 4))
    
    return converted


def load_level0a_packets(db_path):
    """
    Load Level 0A packets from is1_health.db.
    
    Args:
        db_path: Path to is1_health.db
        
    Returns:
        pd.DataFrame: Packets data from level0a table
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM level0a", conn)
    conn.close()
    
    return df


def convert_level0a_to_level0b(level0a_df, conversion_map, field_names):
    """
    Convert Level 0A data to Level 0B by applying conversion coefficients.
    
    Args:
        level0a_df: DataFrame with Level 0A packet data
        conversion_map: Mapping from field index to conversion parameters
        field_names: List of field names from packets table
        
    Returns:
        pd.DataFrame: Level 0B data with converted values
    """
    level0b_df = level0a_df.copy()
    
    # Apply conversions to each field
    for col_idx, col_name in enumerate(field_names):
        if col_idx in conversion_map:
            coefficients = conversion_map[col_idx]
            
            # Skip if field name doesn't match expected name
            if col_name != coefficients.get('Name'):
                print(f"Warning: Field {col_idx} name mismatch - DB: {col_name}, Expected: {coefficients.get('Name')}")
            
            # Apply conversion to each row
            level0b_df[col_name] = level0a_df[col_name].apply(
                lambda x: apply_conversion(x, coefficients)
            )
    
    return level0b_df


def save_level0b_data(level0b_df, output_path):
    """
    Save Level 0B data to level0b table in is1_health.db.
    
    Args:
        level0b_df: DataFrame with Level 0B data
        output_path: Path to is1_health.db
    """
    conn = sqlite3.connect(output_path)
    level0b_df.to_sql('level0b', conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Level 0B data saved to {output_path} (table: level0b)")


def main():
    """Main conversion process."""
    # Define file paths
    script_dir = Path(__file__).parent
    db_path = script_dir / 'is1_health.db'
    json_path = script_dir / 'beacon_conversion.json'
    
    # Verify input files exist
    if not db_path.exists():
        print(f"Error: {db_path} not found")
        return
    
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        return
    
    print(f"Loading conversion coefficients from {json_path}")
    conversion_map = load_conversion_coefficients(json_path)
    
    print(f"Loading Level 0A packets from {db_path} (table: level0a)")
    level0a_df = load_level0a_packets(db_path)
    field_names = get_field_names_from_db(db_path)
    
    print(f"Loaded {len(level0a_df)} packets with {len(field_names)} fields")
    print(f"Converting {len(conversion_map)} fields...")
    
    level0b_df = convert_level0a_to_level0b(level0a_df, conversion_map, field_names)
    
    print(f"Saving Level 0B data to {db_path} (table: level0b)")
    save_level0b_data(level0b_df, db_path)
    
    print("Conversion complete!")
    print(f"\nSummary:")
    print(f"  Input records: {len(level0a_df)}")
    print(f"  Output records: {len(level0b_df)}")
    print(f"  Fields converted: {len(conversion_map)}")


if __name__ == '__main__':
    main()
