from inspiresat1 import Inspiresat1
import os
import pandas as pd
import sqlite3


def packet_to_dict(pkt):
    """
    Convert a CCSDS packet to a dictionary of telemetry fields.
    
    Args:
        pkt: Inspiresat1 packet object
        
    Returns:
        dict: Telemetry data with secondary header and user data fields
    """
    ccsds_pkt = pkt.ax25_frame.payload.ax25_info.ccsds_space_packet
    data = ccsds_pkt.data_section
    secondary_header = data.secondary_header
    sh_coarse = secondary_header.sh_coarse
    sh_fine = secondary_header.sh_fine
    dict_data = {
        "sh_coarse": sh_coarse,
        "sh_fine": sh_fine
    }
    user_data = data.user_data_field
    fields = list(user_data.__dict__.keys())[3:]
    for field in fields:
        dict_data[field] = getattr(user_data, field)
    return dict_data


def decode_packets_to_is1_health(source_dir, db_path='is1_health.db'):
    """
    Decode all packets from source directory and store in is1_health.db.
    
    Args:
        source_dir: Directory containing demodulated packet files
        db_path: Path to is1_health.db (default: is1_health.db in current directory)
    """
    df = pd.DataFrame()
    
    print(f"Reading packets from {source_dir}...")
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        try:
            pkt = Inspiresat1.from_file(filepath)
            packet_dict = packet_to_dict(pkt)
            df = pd.concat([df, pd.DataFrame([packet_dict])], ignore_index=True)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    print(f"Loaded {len(df)} packets")
    print(f"Saving to {db_path} (table: level0a)...")
    
    conn = sqlite3.connect(db_path)
    df.to_sql('level0a', conn, if_exists='replace', index=False)
    conn.close()
    
    print("✓ Packets saved to is1_health.db (level0a table)")


if __name__ == '__main__':
    # Example usage
    source_directory = "/mnt/is1-health/demodulated"
    
    if os.path.exists(source_directory):
        decode_packets_to_is1_health(source_directory)
    else:
        print(f"Source directory not found: {source_directory}")
        print("Update the source_directory path in the script as needed.")

