# Lending Risk Pipeline (JupyterLab + PySpark)

A small local pipeline based on the candidate exercise. It validates monthly lending-risk CSV data, keeps rejected rows for review, resolves duplicates, calculates breaches, and exports CSV files for Power BI.

## 1. Requirements

- Python 3.10 or newer (use a version supported by your chosen PySpark release)
- Java JDK installed and `java -version` works
- JupyterLab, PySpark and pytest (installed below)

## 2. Setup

Run these commands from the `lending-risk-pipeline` project folder.

### Windows PowerShell

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks activation, use Command Prompt and run `.venv\Scripts\activate.bat`.

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Check Java before starting Spark:

```bash
java -version
```

## 3. Add your CSV

A small six-row sample CSV is already included at `data/input/lending_risk.csv`. Replace it with the supplied CSV if you have a separate input file. The expected columns are:

`facility_id, customer_segment, metric, period_end, actual_value, threshold_value, source_updated_at`

The sample rows shown in the assignment can be entered into a CSV with those headers if a separate CSV was not supplied.

## 4. Run in JupyterLab

```bash
jupyter lab
```

Open `notebooks/lending_risk_pipeline.ipynb` and run cells from top to bottom. The notebook reads all CSV columns as strings first, preserving the source representation.

## 5. Run tests

From the project folder:

```bash
python -m pytest -q
```

## 6. Output files

- `data/output/raw_csv_folder/` — source rows as read
- `data/output/detail/` — latest valid row per business key, including `is_breach`
- `data/output/rejected/` — invalid rows and `rejection_reason`
- `data/output/duplicates/` — older valid rows superseded by a newer update
- `data/output/summary/` — grouped report-ready measures

Each output is a Spark CSV folder containing a `part-*.csv` file and a success marker. Open the `part-*.csv` file in Power BI or Excel.

The pipeline overwrites these output files on each run, so rerunning the same input does not append duplicate business results.

## 7. Business assumptions

- Business key: `facility_id + metric + period_end`.
- Rows with invalid required fields or invalid date/numeric/timestamp values are rejected before deduplication.
- `actual_value >= threshold_value` means breach, including equality.
- The latest `source_updated_at` wins for duplicate valid business keys.
- If two rows for the same key have exactly the same timestamp, this simple version does not define a further tie-breaker; in production, add a stable source row ID.
- The supplied brief expects 6 source rows, 2 rejected rows, 1 superseded duplicate, and 3 detail rows. It expects 2 breaches out of 3 valid rows overall.

## 8. Power BI

Import `detail.csv` and `summary.csv` into Power BI Desktop. Add KPI cards for facility count, breached facility count and breach rate; slicers for period, segment and metric; a breach-rate-by-segment chart; and a facility detail table.

## 9. Databricks discussion

For a production version, keep the same transformations in PySpark, store outputs in Delta tables, use a stable source ID and Delta MERGE/upserts for idempotent loads, and monitor rejection counts, schema changes and late-arriving updates.
