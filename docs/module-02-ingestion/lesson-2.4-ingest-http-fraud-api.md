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

```python
import requests
import json

# In a Databricks environment, dbutils is available by default.
try:
    api_token = dbutils.secrets.get(scope="mortgage-secrets", key="fraud-api-token")
except NameError:
    api_token = "mock_token"

url = "https://api.fraud-detection-service.com/v1/blacklist"
headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    
    # In Databricks, we write to ADLS using standard python I/O by writing to the /dbfs mount or using dbutils.fs.put
    # For this exercise, we will assume dbutils.fs.put
    destination_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/fraud_blacklist/blacklist_today.json"
    
    try:
        dbutils.fs.put(destination_path, json.dumps(data), overwrite=True)
        print(f"Successfully landed API data to {destination_path}")
    except NameError:
        print(f"Local Mock: Would have written data to {destination_path}")
else:
    raise Exception(f"API request failed with status {response.status_code}")
```

---

## 🛠️ Action Step: Local Unit Testing for HTTP APIs

When writing scripts that make network calls, your unit tests should *never* actually hit the real API. This causes tests to be slow, fragile, and potentially blocked by firewalls. We use a library called `responses` to mock the HTTP call.

1. Install the testing library locally:
```bash
pip install responses pytest
```

2. Create a new file `tests/test_ingest_fraud_api.py`.
3. Add the following code to simulate the API response:

```python
import pytest
import requests
import responses

# The logic from our ingestion script, refactored into a testable function
def fetch_fraud_blacklist(api_url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    return response.json()

@responses.activate
def test_fetch_fraud_blacklist_success():
    # 1. Arrange: Tell the 'responses' library to intercept any calls to this URL
    api_url = "https://api.fraud-detection-service.com/v1/blacklist"
    mock_payload = {"blacklisted_ssns": ["000-00-0000", "999-99-9999"]}
    
    responses.add(
        responses.GET,
        api_url,
        json=mock_payload,
        status=200
    )
    
    # 2. Act: Call our function
    result = fetch_fraud_blacklist(api_url, "fake-test-token")
    
    # 3. Assert: Verify the function correctly returned the JSON payload
    assert "blacklisted_ssns" in result
    assert len(result["blacklisted_ssns"]) == 2
```

4. Run the test:
```bash
pytest tests/test_ingest_fraud_api.py
```

*(Note: We will cover integration testing—actually interacting with Azure Key Vault and ADLS—using Databricks Connect in **Lesson 3.5**).*

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why shouldn't you make external REST API calls from inside a PySpark DataFrame transformation (like a UDF)?**
> **Answer:** "Making HTTP requests from inside a distributed Spark transformation is an anti-pattern. Because Spark executes tasks concurrently across many executor nodes, doing so acts like a DDoS attack on the API server, causing rate-limiting, timeouts, and network bottlenecks. Instead, I follow a decoupled pattern: I use a single-threaded Python script (or an Azure Data Factory Web Activity) to fetch the API payload and dump the raw JSON into the Bronze layer of ADLS. From there, Spark can read the JSON file and process it efficiently in a distributed manner."

---
[⬅️ Previous: Lesson 2.3: Ingesting JDBC](lesson-2.3-ingest-jdbc.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.5: PySpark Transformations](lesson-2.5-pyspark-transformations.md)
