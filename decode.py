from inspiresat1 import Inspiresat1
import os
import pandas as pd
import sqlite3


def packet_to_dict(pkt):
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

df = pd.DataFrame()

for filename in os.listdir("/mnt/is1-health/demodulated"):
    filepath = os.path.join("/mnt/is1-health/demodulated", filename)
    pkt = Inspiresat1.from_file(filepath)
    packet_to_dict(pkt)
    df = pd.concat([df, pd.DataFrame([packet_to_dict(pkt)])], ignore_index=True)


conn = sqlite3.connect('level0a.db')
df.to_sql('packets', conn, if_exists='replace', index=False)
conn.close()
