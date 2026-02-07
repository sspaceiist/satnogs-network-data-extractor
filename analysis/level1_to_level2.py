import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib import pyplot as plt
df = pd.read_sql('SELECT * from is1_health', con='sqlite:///is1_health_data.db')

def gps_to_utc(gps_seconds):
    """Convert GPS time (seconds since Jan 6, 1980) to UTC datetime."""
    gps_epoch = datetime(1980, 1, 6)
    # GPS time is ahead of UTC by leap seconds (currently 18 as of 2017)
    leap_seconds = 18
    utc_time = gps_epoch + pd.to_timedelta(gps_seconds - leap_seconds, unit='s')
    return utc_time

df['UTC_DateTime'] = df['DAXSS_time'].apply(gps_to_utc)

# Remove outliers using IQR method
Q1 = df['DAXSS_time'].quantile(0.25)
Q3 = df['DAXSS_time'].quantile(0.75)
IQR = Q3 - Q1
time = df[(df['DAXSS_time'] >= Q1 - 1.5 * IQR) & 
            (df['DAXSS_time'] <= Q3 + 1.5 * IQR)]
# plt.plot(df['DAXSS Time Stamp (seconds)'][df['DAXSS Time Stamp (seconds)']>1000])
# plt.title('DAXSS Time Stamp (seconds)')
# plt.savefig("daxss_time_stamp.png", dpi=300, bbox_inches="tight")
# plt.clf()
# plt.plot(df['SHCOARSE'])
# plt.title('SHCOARSE')
# plt.savefig("shcoarse.png", dpi=300, bbox_inches="tight")
# plt.clf()
# plt.plot(df['CCSDS_Header_2_Seq'])
# plt.title('CCSDS_Header_2_Seq')
# plt.savefig("CCSDS_Header_2_Seq.png", dpi=300, bbox_inches="tight")

# time_sorted = time.sort_values('UTC_DateTime')
# plt.figure(figsize=(12, 6))
# plt.plot(time_sorted['UTC_DateTime'])
# plt.xlabel('Index')
# plt.ylabel('UTC DateTime')
# plt.title('Sorted Time Data')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig('sorted_time.png', dpi=300, bbox_inches='tight')
# plt.show()

print(f"Min: {time['UTC_DateTime'].min()}")
print(f"Max: {time['UTC_DateTime'].max()}")
df = df.set_index('UTC_DateTime').sort_index().reset_index()
df.to_sql('is1_health_data_level2.db', if_exists='replace', index=True, con='sqlite:///is1_health_data_level2.db')