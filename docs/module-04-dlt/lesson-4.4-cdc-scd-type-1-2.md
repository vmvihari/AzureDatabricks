# Lesson 4.4: Change Data Capture (CDC) & SCDs

## Table of Contents
- [What is Change Data Capture (CDC)?](#what-is-change-data-capture-cdc)
- [SCD Type 1 vs. SCD Type 2](#scd-type-1-vs-scd-type-2)
- [The DLT `APPLY CHANGES INTO` API](#the-dlt-apply-changes-into-api)
- [Action Step: The Loan Servicing Pipeline](#action-step-the-loan-servicing-pipeline)
- [Interview Preparation](#interview-preparation)

---

## What is Change Data Capture (CDC)?

Most modern databases do not simply append new data. They update and delete existing records. 
For example, our Loan Servicing SQL database receives updates when a customer makes a monthly payment (e.g., updating their `current_balance` from $400,000 to $395,000).

Extracting these updates from the source DB and applying them to our Silver Delta Tables is called **Change Data Capture (CDC)**.

---

## SCD Type 1 vs. SCD Type 2

When an update arrives, we must decide how to handle the historical record in the Lakehouse. These are called **Slowly Changing Dimensions (SCD)**.

### SCD Type 1 (Overwrite)
The new record simply overwrites the old record. We only ever care about the absolute latest state of the world. 
- *Use Case:* Correcting a typo in a customer's name.

### SCD Type 2 (History Retention)
We insert the new record but keep the old record for historical auditing. We add columns like `valid_from`, `valid_to`, and `is_current`.
- *Use Case:* Tracking a customer's credit score over time to build a machine learning model analyzing score degradation.

---

## The DLT `APPLY CHANGES INTO` API

In standard PySpark, implementing CDC requires complex `MERGE INTO` SQL statements. If the upstream database can send out-of-order events (e.g., the update from 1:00 PM arrives *before* the update from 12:00 PM), the PySpark code becomes a massive, error-prone headache to ensure you don't overwrite a newer record with an older one.

DLT solves this with the `dlt.apply_changes()` Python function. You simply define the primary key and the sequence column (usually a timestamp). DLT handles the massive `MERGE` operation behind the scenes and automatically resolves out-of-order events.

---

## 🛠️ Action Step: The Loan Servicing Pipeline

Let's build a CDC pipeline to handle updates from our operational servicing database. 

1. Navigate to `apps/mortgage-data-platform/src/dlt/` and create `dlt_cdc_servicing.py`.
2. First, we ingest the raw CDC events (which contain an `event_id`, the `loan_id`, the updated `balance`, and an `event_timestamp`) into Bronze.
3. Then, we use `apply_changes` to maintain a pristine, real-time Silver table of the current loan balances using SCD Type 1.

```python
import dlt

# 1. Ingest the raw CDC stream into Bronze using Auto Loader
@dlt.table(name="bronze_servicing_events")
def bronze_servicing_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/servicing_cdc/")
    )

# 2. Define the empty target Silver table (required for apply_changes)
dlt.create_streaming_table("silver_current_loan_balances")

# 3. Apply the CDC changes using SCD Type 1 (Overwrite with latest state)
dlt.apply_changes(
    target="silver_current_loan_balances",
    source="bronze_servicing_events",
    keys=["loan_id"],                  # The primary key to merge on
    sequence_by="event_timestamp",     # Ensures out-of-order events don't overwrite newer data
    apply_as_deletes=None,             # Optional: expression to flag deleted records
    except_column_list=None
)
```

---

## 5. 🛠️ Action Step: Validation & Testing

CDC logic is critical to validate, ensuring updates apply correctly and out-of-order events don't corrupt the data.

1. Inject a mock JSON payload into your `servicing_cdc` landing path containing an initial balance for a mock `loan_id`.
2. Run the DLT pipeline using Databricks Asset Bundles (`databricks bundle run`) and verify the Silver table reflects the initial balance.
3. Inject a second JSON payload with a *newer* timestamp but a different balance (simulating a payment). Run the pipeline again.
4. Verify the Silver table updated the balance correctly.
5. (Optional but highly recommended) Inject a third JSON payload with an *older* timestamp than the second payload. Run the pipeline again and verify the Silver table ignores it!

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain the difference between SCD Type 1 and SCD Type 2. When would you use each in a Medallion Architecture?**
> **Answer:** "SCD Type 1 overwrites the existing record; it does not maintain history. I use this in the Silver layer when the business only requires the absolute latest state (e.g., correcting an improperly spelled mailing address). SCD Type 2 maintains a full historical log by adding new rows with `valid_from` and `is_current` flags. I use SCD Type 2 when the business requires time-travel auditing or when building temporal features for Machine Learning models (e.g., tracking how a borrower's debt-to-income ratio fluctuated over the last 5 years)."

> [!TIP]
> **Q2: Why is implementing CDC manually via PySpark `MERGE INTO` dangerous when dealing with distributed event streams?**
> **Answer:** "In distributed message buses like Kafka or Azure Event Hubs, events can arrive out of order. If a customer changes their phone number at 1:00 PM, and again at 1:05 PM, network latency might cause the 1:05 PM event to arrive at the Lakehouse *before* the 1:00 PM event. A naive PySpark `MERGE INTO` will process the 1:05 event, and then wrongly overwrite it with the stale 1:00 event. DLT's `apply_changes` solves this inherently because it requires a `sequence_by` column (like an event timestamp), guaranteeing that an older event will never overwrite a newer state, regardless of arrival order."

---
[⬅️ Previous: Lesson 4.3: DLT Expectations](lesson-4.3-dlt-expectations.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 4.5: Asset Bundles](lesson-4.5-asset-bundles.md)
