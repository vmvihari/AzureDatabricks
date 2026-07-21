# Mortgage Platform Data Generator

This utility is used to generate highly realistic, synthetic datasets for the Mortgage Data Platform. Instead of checking in large CSV/JSON files to Git, you can run these scripts to generate as much data as you need locally for your PySpark/Databricks experiments.

## Prerequisites

Ensure you have Python installed, then install the dependencies:
```bash
pip install -r requirements.txt
```

## Usage

You can generate three types of data sets. By default, they will output to `../mortgage-data-platform/data/raw/` to simulate an Azure Data Lake landing zone.

### 1. Loan Applications (CSV)
Simulates raw mortgage applications.
```bash
# Generate 1,000 rows (default)
python generate_loans.py

# Generate 5,000,000 rows for performance tuning (Module 5)
python generate_loans.py --rows 5000000
```

### 2. Fraud Blacklist (JSON)
Simulates an external REST API payload containing known fraudulent entities (SSNs, Emails, IP Addresses).
```bash
python generate_fraud.py --rows 500
```

### 3. Servicing / CDC Events (CSV)
Simulates Change Data Capture (CDC) events, such as a customer updating their address or their credit score changing over time. Used heavily in Delta Live Tables (Module 4).
```bash
python generate_servicing.py --rows 200
```

## Extensibility
As you progress through the curriculum and require new columns or new tables (e.g., property valuations), simply modify the Python scripts here and re-run them!
