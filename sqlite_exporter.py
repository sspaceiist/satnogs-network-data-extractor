import sqlite3
import time
from prometheus_client import start_http_server, Gauge

DB_PATH = "is1_health.db"
TABLE = "level1"

metric = Gauge(
    "level1_value",
    "All numeric fields from level1 table",
    ["field"]
)

# Columns we EXCLUDE
SKIP_COLUMNS = {
    "daxss_utc_datetime",
    "daxss_year",
    "daxss_month",
    "daxss_day",
    "daxss_hour",
    "daxss_minute",
    "daxss_second"
}

def collect():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get column names
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]

    # Fetch latest row
    cur.execute(f"""
        SELECT * FROM {TABLE}
        ORDER BY daxss_utc_datetime DESC
        LIMIT 1
    """)
    row = cur.fetchone()

    if row is None:
        conn.close()
        return

    for col, val in zip(columns, row):
        if col in SKIP_COLUMNS:
            continue
        if isinstance(val, (int, float)):
            metric.labels(field=col).set(val)

    conn.close()

if __name__ == "__main__":
    start_http_server(8003)
    while True:
        collect()
        time.sleep(5)
