## Create Report Page

Create a report page named:

```text
Lending Risk Overview
```

---

## Import CSV Files

In Power BI Desktop:

```text
Home → Get Data → Text/CSV
```

Import the following files:

```text
raw.csv
detail.csv
rejected.csv
duplicates.csv
summary.csv
```

Rename the tables exactly as:

```text
raw
detail
rejected
duplicates
summary
```

---

## Configure Data Types

### detail Table

| Column | Data Type |
|----------|----------|
| period_end | Date |
| actual_value | Decimal Number |
| threshold_value | Decimal Number |
| is_breach | True/False |

### raw Table

Keep:

```text
actual_value
```

as:

```text
Text
```

### rejected Table

Keep:

```text
actual_value
```

as:

```text
Text
```

This preserves invalid source values such as:

```text
not_available
```

---

## Create DAX Measures

Go to:

```text
Modeling → New Measure
```

Create the following measures.

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

## Report Layout

### Header Section

Add a text box with the title:

```text
Lending Risk Overview
```

---

### Slicers

Add the following slicers:

```text
detail[period_end]
detail[customer_segment]
detail[metric]
```

Recommended format:

```text
Dropdown
```

---

### KPI Cards

Create four KPI cards using these measures:

| Card Title | Measure |
|------------|----------|
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

### Data Quality Cards

Create four additional cards:

| Card Title | Measure |
|------------|----------|
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

### Breach Rate by Customer Segment

Add a:

```text
Clustered Bar Chart
```

#### Axis

```text
detail[customer_segment]
```

#### Values

```text
[Breach Rate]
```

#### Tooltips

```text
[Breached Facilities]
[Valid Facilities]
```

Expected output:

| Customer Segment | Breach Rate |
|------------------|-------------|
| SME | 100% |
| Corporate | 50% |

---

### Facility Detail Table

Add a Table visual using the following columns from the detail table:

```text
facility_id
customer_segment
metric
period_end
actual_value
threshold_value
is_breach
```

Expected output:

| Facility ID | Customer Segment | Metric | Period End | Actual Value | Threshold Value | Is Breach |
|-------------|------------------|---------|------------|--------------|-----------------|-----------|
| F001 | SME | PD | 2026-09-30 | 3.2 | 3.0 | TRUE |
| F002 | Corporate | PD | 2026-09-30 | 2.7 | 3.0 | FALSE |
| F005 | Corporate | PD | 2026-09-30 | 3.5 | 3.0 | TRUE |

---

## Data Model

### Recommended for This Exercise

No relationships are required.

Keep all tables disconnected:

```text
raw
detail
rejected
duplicates
summary
```

Data quality metrics will read directly from their respective tables, while breach and lending-risk visuals will use the detail table.

---

### Optional Dimensional Model

If common slicers should filter all tables, create the following dimensions:

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

relationships from each dimension table to any fact table containing the matching key.

Optional enhancement:

```text
Map null customer_segment values to "Unknown"
```

This allows rejected records to participate in customer segment analysis.

---

## Validation Checklist

Verify the report returns the following values:

```text
✓ Raw Rows = 6
✓ Valid Facilities = 3
✓ Rejected Rows = 2
✓ Superseded Rows = 1
✓ Breached Facilities = 2
✓ Breach Rate = 66.7%
✓ Average Actual Value = 3.13
✓ SME Breach Rate = 100%
✓ Corporate Breach Rate = 50%
```

The report is considered complete when all validation checks match the expected results 