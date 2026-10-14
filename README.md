# Lending Risk Overview

A PySpark-based lending risk data pipeline that validates source records, rejects invalid data, resolves duplicate business keys, calculates breach indicators, generates aggregated summaries, and exports report-ready CSV files for Power BI.

---

# 1. Project Overview

This project processes monthly lending risk data and produces five output datasets for reporting and data quality monitoring.

### Input File

```text
data/input/lending_risk.csv
```

### Expected Source Columns

```text
facility_id
customer_segment
metric
period_end
actual_value
threshold_value
source_updated_at
```

### Key Processing Rules

- Trim whitespace from text fields.
- Validate mandatory fields.
- Validate date, timestamp, and numeric formats.
- Reject invalid records while preserving source values.
- Deduplicate records using:

```text
facility_id + metric + period_end
```

- Retain the latest record based on:

```text
source_updated_at
```

- Determine breach status using:

```text
actual_value >= threshold_value
```

---

# 2. Prerequisites

Install the following software before running the project.

## Python

Install:

```text
Python 3.14
```

Verify:

```powershell
python --version
```

---

## Java

Install:

```text
JDK 17
```

Verify:

```powershell
java -version
```

Expected output:

```text
Java version 17
```

---

## Git

Verify:

```powershell
git --version
```

---

# 3. Clone the Repository

```powershell
git clone https://github.com/Ambikach606/lending_risk.git
cd lending_risk
```

Or add the remote to an existing repository:

```powershell
git remote add origin https://github.com/Ambikach606/lending_risk.git
```

Verify:

```powershell
git remote -v
```

---

# 4. Create a Virtual Environment

From the project root folder:

```powershell
py -3.14 -m venv .venv
```

---

# 5. Activate the Virtual Environment

## PowerShell

```powershell
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks script execution:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Enter:

```text
Y
```

Then activate again:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

## Command Prompt Alternative

```cmd
.venv\Scripts\activate.bat
```

---

# 6. Install Dependencies

Upgrade pip:

```powershell
python -m pip install --upgrade pip
```

Install required packages:

```powershell
python -m pip install pandas
python -m pip install jupyterlab ipykernel pyspark pyarrow pytest
```

Or install all dependencies from requirements.txt:

```powershell
pip install -r requirements.txt
```

---

# 7. Launch JupyterLab

Start JupyterLab:

```powershell
python -m jupyterlab
```

Open:

```text
notebooks/lending_risk_pipeline.ipynb
```

Run all notebook cells from top to bottom.

---

# 8. Project Structure

```text
lending_risk/
│
├── data/
│   ├── input/
│   │   └── lending_risk.csv
│   │
│   └── output/
│       ├── raw.csv
│       ├── detail.csv
│       ├── rejected.csv
│       ├── duplicates.csv
│       └── summary.csv
│
├── notebooks/
│   └── lending_risk_pipeline.ipynb
│
├── powerbi/
│   ├── dax_measures.txt
│   ├── mock_report.html
│   └── mock_report.md
│
├── src/
│   └── transformations.py
│
├── tests/
│   └── test_transformations.py
│
├── requirements.txt
└── README.md
```

---

# 9. Run Automated Tests

Execute the full test suite:

```powershell
python -m pytest -q
```

If FutureWarnings appear:

```powershell
python -m pytest -W ignore::FutureWarning -q
```

If UserWarnings appear:

```powershell
python -m pytest -W ignore::UserWarning -q
```

Expected result:

```text
6 passed
```

---

# 10. Output Files

After running the notebook, the following files are generated.

## raw.csv

Original source records before validation.

Expected record count:

```text
6
```

---

## detail.csv

Validated, typed, and deduplicated records.

Additional field:

```text
is_breach
```

Expected record count:

```text
3
```

---

## rejected.csv

Invalid source records.

Additional field:

```text
rejection_reason
```

Expected record count:

```text
2
```

Example reasons:

```text
missing customer_segment
invalid actual_value (must be a finite number)
```

---

## duplicates.csv

Valid records removed during deduplication because a newer version exists.

Additional field:

```text
duplicate_reason
```

Expected record count:

```text
1
```

---

## summary.csv

Aggregated reporting dataset grouped by:

```text
period_end
customer_segment
metric
```

---

# 11. Expected Results

For the provided sample input file:

| Metric | Expected Value |
|----------|----------|
| Raw Rows | 6 |
| Valid Facilities | 3 |
| Rejected Rows | 2 |
| Superseded Rows | 1 |
| Breached Facilities | 2 |
| Breach Rate | 66.7% |
| Average Actual Value | 3.13 |

---

# 12. Power BI Setup

## Import CSV Files

In Power BI Desktop:

```text
Home → Get Data → Text/CSV
```

Import:

```text
raw.csv
detail.csv
rejected.csv
duplicates.csv
summary.csv
```

Use the following table names exactly:

```text
raw
detail
rejected
duplicates
summary
```

---

## Configure Data Types

### detail

| Column | Data Type |
|----------|----------|
| period_end | Date |
| actual_value | Decimal Number |
| threshold_value | Decimal Number |
| is_breach | True/False |

### raw

Keep:

```text
actual_value
```

as:

```text
Text
```

### rejected

Keep:

```text
actual_value
```

as:

```text
Text
```

This preserves source values such as:

```text
not_available
```

---

# 13. Create DAX Measures

In Power BI Desktop:

```text
Modeling → New Measure
```

Create each measure separately.

### Raw Rows

```DAX
Raw Rows =
CALCULATE(
    COUNTROWS('raw'),
    ALL('raw')
)
```

### Superseded Rows

```DAX
Superseded Rows =
COUNTROWS(duplicates)
```

### Valid Facilities

```DAX
Valid Facilities =
DISTINCTCOUNT(detail[facility_id])
```

### Breached Facilities

```DAX
Breached Facilities =
CALCULATE(
    DISTINCTCOUNT(detail[facility_id]),
    detail[is_breach] = TRUE()
)
```

### Breach Rate

```DAX
Breach Rate =
DIVIDE(
    [Breached Facilities],
    [Valid Facilities],
    0
)
```

Format:

```text
Percentage
```

### Average Actual Value

```DAX
Average Actual Value =
AVERAGE(detail[actual_value])
```

### Rejected Rows

```DAX
Rejected Rows =
COUNTROWS(rejected)
```

---

# 14. Build the Power BI Report

Create a report page named:

```text
Lending Risk Overview
```

## Report Header

Add the title:

```text
Lending Risk Overview
```

---

## Slicers

Add slicers for:

```text
detail[period_end]
detail[customer_segment]
detail[metric]
```

---

## KPI Cards

| Card | Measure |
|--------|----------|
| Valid Facilities | [Valid Facilities] |
| Breached Facilities | [Breached Facilities] |
| Breach Rate | [Breach Rate] |
| Average Actual Value | [Average Actual Value] |

Expected values:

```text
Valid Facilities = 3
Breached Facilities = 2
Breach Rate = 66.7%
Average Actual Value = 3.13
```

---

## Data Quality Cards

| Card | Measure |
|--------|----------|
| Raw Rows | [Raw Rows] |
| Valid Facilities | [Valid Facilities] |
| Rejected Rows | [Rejected Rows] |
| Superseded Rows | [Superseded Rows] |

Expected values:

```text
Raw Rows = 6
Valid Facilities = 3
Rejected Rows = 2
Superseded Rows = 1
```

---

## Breach Rate by Customer Segment

Visual:

```text
Clustered Bar Chart
```

Category:

```text
detail[customer_segment]
```

Value:

```text
[Breach Rate]
```

Tooltips:

```text
[Breached Facilities]
[Valid Facilities]
```

Expected results:

| Customer Segment | Breach Rate |
|------------------|-------------|
| SME | 100% |
| Corporate | 50% |

---

## Facility Detail Table

Display the following columns from the detail table:

```text
facility_id
customer_segment
metric
period_end
actual_value
threshold_value
is_breach
```

---

# 15. Data Model Options

## Option 1: Simple Model (Recommended)

Keep all tables disconnected.

```text
raw
detail
rejected
duplicates
summary
```

Data quality cards read directly from their respective tables, while risk and breach visuals use the detail table.

---

## Option 2: Dimensional Model

Create the following dimensions:

```text
DimFacility
DimSegment
DimMetric
DimDate
```

Create:

```text
One-to-Many
Single Direction
```

relationships from dimensions to fact tables.

Optional enhancement:

```text
Map null customer_segment values to "Unknown"
```

to include rejected rows in customer segment analysis.

---

# 16. Production Enhancements

For a production-grade solution:

- Store outputs as Delta tables.
- Use Delta MERGE for incremental processing.
- Add audit and lineage columns.
- Implement schema validation.
- Add data quality monitoring and alerts.
- Track rejected record trends.
- Introduce stable source row identifiers.
- Schedule workflows using Databricks Jobs or orchestration tools.
- Persist historical reporting metrics for trend analysis.