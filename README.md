level | source | description | fields
## IS1 Health Data Processing Pipeline

### Database Structure: `is1_health.db`

All telemetry data is now consolidated in a single SQLite database with two tables:

| Table | Records | Columns | Description |
|-------|---------|---------|-------------|
| `level0a` | 112,901 | 133 | Raw Level 0A packets (direct CCSDS decoder output) |
| `level0b` | 112,901 | 133 | Calibrated Level 0B packets (converted using beacon coefficients) |
| `level0c` | 112,901 | 140 | Level 0B with human-readable datetime fields |

#### Level 0A (Raw Data)
- **Source**: Kaitai Struct compiled CCSDS decoder (`inspiresat1.py`)
- **Content**: Directly converted CCSDS packets from satellite
- **Fields**: 
  - Secondary header: `sh_coarse`, `sh_fine` (timestamps)
  - User data: 131 telemetry fields (temperatures, currents, voltages, status bits, etc.)

#### Level 0B (Calibrated Data)
- **Source**: Level 0A converted using polynomial coefficients
- **Conversion**: Applies calibration formulas from `beacon_conversion.json`
- **Formula**: `converted = C0 + C1×raw + C2×raw² + C3×raw³ + C4×raw⁴`
- **Fields**: Same 133 fields as Level 0A with calibrated values

#### Level 0C (With Datetime Fields)
- **Source**: Level 0B with added datetime conversions
- **Conversion**: DAXSS GPS timestamps converted to UTC datetime
- **GPS Time**: Number of seconds since GPS epoch (January 6, 1980 00:00:00 UTC)
- **New Fields**: 
  - `daxss_utc_datetime`: ISO format datetime in UTC
  - `daxss_year`, `daxss_month`, `daxss_day`: Date components
  - `daxss_hour`, `daxss_minute`, `daxss_second`: Time components

---

### Scripts

#### 1. `decode.py` - Packet Decoding
Decodes demodulated binary packets from satellite and stores raw data in `is1_health.db`.

**Function**: `decode_packets_to_is1_health(source_dir, db_path='is1_health.db')`
- **Input**: Directory with demodulated packet files
- **Output**: `is1_health.db` with `level0a` table

**Usage**:
```python
from decode import decode_packets_to_is1_health
decode_packets_to_is1_health("/path/to/demodulated/packets")
```

#### 2. `level0a_to_level0b.py` - Data Calibration
Converts raw Level 0A data to calibrated Level 0B data using conversion coefficients.

**Input**: `is1_health.db` (level0a table)
**Output**: `is1_health.db` (level0b table, replaces existing)

**Usage**:
```bash
python level0a_to_level0b.py
```

**Process**:
1. Loads Level 0A packets from `is1_health.db` (level0a table)
2. Reads conversion coefficients from `beacon_conversion.json`
3. Applies polynomial conversion formulas to each field
4. Saves calibrated data to `is1_health.db` (level0b table)

#### 3. `level0b_to_level0c.py` - Datetime Conversion
Converts DAXSS GPS timestamps to human-readable UTC datetime fields.

**Input**: `is1_health.db` (level0b table)
**Output**: `is1_health.db` (level0c table, includes all level0b data plus datetime fields)

**Usage**:
```bash
python level0b_to_level0c.py
```

**Process**:
1. Loads Level 0B packets from `is1_health.db` (level0b table)
2. Finds DAXSS GPS timestamp column (`daxss_time_sec`)
3. Converts GPS time to UTC using GPS epoch (Jan 6, 1980) and 18 leap seconds
4. Extracts datetime components (year, month, day, hour, minute, second)
5. Saves full data to `is1_health.db` (level0c table)

**GPS Time Conversion**:
- GPS epoch: January 6, 1980 00:00:00 UTC
- Leap seconds (current): 18
- Formula: `UTC datetime = GPS epoch + (GPS seconds - leap seconds)`

#### 4. `inspiresat1.py` - CCSDS Packet Parser
Kaitai Struct compiled packet parser for INSPIRE-SAT 1 CCSDS space packets.
- Generated from `.ksy` file using Kaitai Struct compiler
- Parses AX.25 frames containing CCSDS space packets
- Extracts secondary header and 131 telemetry fields

#### 5. `beacon_conversion.json` - Calibration Coefficients
Defines polynomial coefficients for converting raw ADC values to calibrated units.

**Structure**:
```json
{
    "field_index": {
	"Name": "field_name",
	"Conversion_C0": 0.0,
	"Conversion_C1": 0.0078125,
	"Conversion_C2": 0.0,
	"Conversion_C3": 0.0,
	"Conversion_C4": 0.0
    }
}
```

**Examples**:
- Linear temperature conversion: `cip_temp1` → `C1 = 0.0078125`
- Polynomial temperature: `obc_temp` → `C0=91.394, C1=-0.0894932, C2=0.0000355, C3=-0.0000000063`
- Current/voltage: `sband_pa_curr` → `C1 = 0.00004`

---

### Workflow

```
Demodulated Packets
	↓
   decode.py
	↓
is1_health.db (level0a table - raw)
        ↓
level0a_to_level0b.py (calibration)
        ↓
is1_health.db (level0b table - calibrated)
        ↓
level0b_to_level0c.py (datetime conversion)
        ↓
is1_health.db (level0c table - with UTC datetime)
```

---

### Statistics

- **Total packets**: 112,901
- **Total fields per packet**: 133 (level0a/0b) → 140 (level0c with datetime)
- **Calibrated fields**: 133 (all fields have conversion entries)
- **Fields with active conversion coefficients**: ~60 (others have `null` coefficients and pass through raw values)
- **Datetime fields added in level0c**: 7 (daxss_utc_datetime + date/time components)
- **Database size**: ~107 MB
- **Date range in data**: 1980-01-05 to 2025-12-27
- **Unique dates**: 1,256

### Accessing Data

**Python (pandas + SQLite)**:
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('is1_health.db')

# Load raw Level 0A data
df_0a = pd.read_sql_query("SELECT * FROM level0a", conn)

# Load calibrated Level 0B data
df_0b = pd.read_sql_query("SELECT * FROM level0b", conn)

# Load Level 0C with datetime fields
df_0c = pd.read_sql_query("SELECT * FROM level0c", conn)

conn.close()
```

**SQL Query Examples**:
```sql
-- Get temperature readings from Level 0B
SELECT sh_coarse, sh_fine, obc_temp, eps_temp, int_temp FROM level0b;

-- Get telemetry with UTC datetime
SELECT daxss_utc_datetime, obc_temp, eps_temp, bat_volt FROM level0c;

-- Find readings from specific date
SELECT * FROM level0c 
WHERE daxss_year = 2025 AND daxss_month = 4;

-- Compare raw vs calibrated temperature
SELECT a.obc_temp as raw_temp, b.obc_temp as calibrated_temp 
FROM level0a a JOIN level0b b 
ON rowid(a) = rowid(b);
```