import sqlite3

from influxdb_client import InfluxDBClient, Point, WritePrecision

from influxdb_client.client.write_api import SYNCHRONOUS

from datetime import datetime

from tqdm import tqdm



# -------- CONFIG --------

SQLITE_DB = "is1_health.db"

TABLE_NAME = "level1"



INFLUX_URL = "http://localhost:8086"

INFLUX_TOKEN = "8UQ4FlrQlrijgdy0IaL75CiZ0-m800lW_EeVza0L34jLa2bAV3FdYWASxLf3MfpCCnPjEPkc4ekHmsfzVskihg=="

INFLUX_ORG = "TARANG"

INFLUX_BUCKET = "GS"



MEASUREMENT = "level1"

BATCH_SIZE = 2000

# ------------------------



def parse_time(timestr):

    # Adjust format if needed

    return datetime.fromisoformat(timestr)



def main():

    print("Connecting to SQLite...")

    conn = sqlite3.connect(SQLITE_DB)

    cursor = conn.cursor()



    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")

    total = cursor.fetchone()[0]

    print(f"Total rows: {total}")



    cursor.execute(f"SELECT * FROM {TABLE_NAME}")

    columns = [desc[0] for desc in cursor.description]



    print("Connecting to InfluxDB...")

    client = InfluxDBClient(

        url=INFLUX_URL,

        token=INFLUX_TOKEN,

        org=INFLUX_ORG

    )

    write_api = client.write_api(write_options=SYNCHRONOUS)



    batch = []

    processed = 0



    print("Starting migration...")



    while True:

        rows = cursor.fetchmany(BATCH_SIZE)

        if not rows:

            break



        for row in rows:

            row_dict = dict(zip(columns, row))



            timestr = row_dict.pop("daxss_utc_datetime")

            if not timestr:

                continue



            try:

                timestamp = parse_time(timestr)

            except Exception:

                continue



            point = Point(MEASUREMENT).time(timestamp, WritePrecision.NS)



            for key, value in row_dict.items():

                if value is None:

                    continue



                if isinstance(value, (int, float)):

                    point.field(key, value)



            batch.append(point)

            processed += 1



        write_api.write(bucket=INFLUX_BUCKET, record=batch)

        batch.clear()



        print(f"Processed {processed}/{total}")



    print("Migration complete.")



if __name__ == "__main__":

    main()
