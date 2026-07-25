# Lesson 2.4: Ingesting HTTP REST APIs (Fraud Blacklist)

## Table of Contents
- [The Challenge of API Ingestion in Spark](#the-challenge-of-api-ingestion-in-spark)
- [The Decoupled Architecture Pattern](#the-decoupled-architecture-pattern)
- [Python Requests for Ingestion](#python-requests-for-ingestion)
- [Interview Preparation](#interview-preparation)

---

## The Challenge of API Ingestion in Spark

In our Mortgage Data Platform, we need to cross-reference loan applicants against a 3rd-party Fraud Blacklist API. 

A common mistake juniors make is attempting to embed HTTP `GET` requests directly inside PySpark User Defined Functions (UDFs) to fetch data while the dataframe is processing.

**Why is this a bad idea?**
Spark is a massively distributed processing engine. If you have a cluster with 100 worker nodes, and you trigger an HTTP request inside a distributed transformation, your cluster will launch a Distributed Denial of Service (DDoS) attack against the external API! You will instantly exhaust your API rate limits, get blocked, or crash the external service.

---

## The Decoupled Architecture Pattern

Instead of distributed API calls, we use a **Decoupled Ingestion Pattern**:
1. **Single-threaded Ingestion:** We write a simple, single-threaded Python script (using the `requests` library).
2. **Land in Bronze:** This script securely connects to the API, downloads the JSON payload, and saves it directly to Azure Data Lake Storage (ADLS) in the Bronze landing zone.
3. **Distributed Processing:** Once the file is safely in the data lake, PySpark (or Auto Loader) picks it up and processes it in a distributed manner.

This protects the external API while fully leveraging Spark for the heavy lifting of transformation.

---

## Python Requests for Ingestion

In Databricks, you can write standard Python alongside PySpark. Here is how you would securely ingest the Fraud Blacklist API:

```python
import requests
import json

# 1. Fetch the secure API token
api_token = dbutils.secrets.get(scope="mortgage-secrets", key="fraud-api-token")

# 2. Configure the HTTP headers
url = "https://api.fraud-detection-service.com/v1/blacklist"
headers = {
    "Authorization": f"Bearer {api_token}",
    "Accept": "application/json"
}

# 3. Make the single-threaded request (Executes entirely on the Driver Node)
response = requests.get(url, headers=headers)

if response.status_code == 200:
    fraud_data = response.json()
    
    # 4. Save the raw JSON to the ADLS Gen2 Bronze Landing Zone
    adls_path = "/mnt/bronze/landing/fraud_blacklist/blacklist_today.json"
    
    with open(adls_path, "w") as f:
        json.dump(fraud_data, f)
        
    print(f"Successfully landed API data to {adls_path}")
else:
    raise Exception(f"API request failed with status {response.status_code}")
```

*(Note: In Databricks, writing directly to `/dbfs/mnt/` or using `dbutils.fs.put` allows standard Python libraries to interact with cloud storage as if it were a local file system).*

---

## 🛠️ Action Step: Ingesting the Fraud Blacklist
Let's build the decoupled Python extraction script.

1. Navigate to `apps/mortgage-data-platform/src/bronze/` and create `ingest_fraud_api.py`.
2. Write a single-threaded Python script using the `requests` library.
3. Fetch the `fraud-api-token` from the `mortgage-secrets` scope and pass it in the HTTP headers.
4. Hit the external API (`https://api.fraud-detection-service.com/v1/blacklist`) and save the raw JSON payload to the ADLS Gen2 landing zone (`abfss://bronze@.../landing/fraud_blacklist/blacklist_today.json`).

---

## 🛠️ Action Step: Validation & Testing

As with the previous ingestion scripts, testing a script that relies on external secure APIs and cloud storage requires a configured environment.

1. **Unit Testing:** You would use the `responses` or `requests-mock` libraries in `pytest` to mock the HTTP API call, ensuring your script correctly handles successful payloads and 4xx/5xx errors without actually hitting the network.
2. **Integration Testing:** In **Lesson 3.5**, Databricks Connect will allow you to run this script locally while securely resolving the `dbutils.secrets` for the token and seamlessly pushing the JSON file up to the `abfss://` ADLS container.

*Note: For now, simply save your script!*

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why shouldn't you make external REST API calls from inside a PySpark DataFrame transformation (like a UDF)?**
> **Answer:** "Making HTTP requests from inside a distributed Spark transformation is an anti-pattern. Because Spark executes tasks concurrently across many executor nodes, doing so acts like a DDoS attack on the API server, causing rate-limiting, timeouts, and network bottlenecks. Instead, I follow a decoupled pattern: I use a single-threaded Python script (or an Azure Data Factory Web Activity) to fetch the API payload and dump the raw JSON into the Bronze layer of ADLS. From there, Spark can read the JSON file and process it efficiently in a distributed manner."

---
[⬅️ Previous: Lesson 2.3: Ingesting JDBC](lesson-2.3-ingest-jdbc.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.5: PySpark Transformations](lesson-2.5-pyspark-transformations.md)
