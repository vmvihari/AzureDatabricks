import json

import requests


def fetch_fraud_blacklist(api_url, token):
    """
    Fetches the fraud blacklist from the external API.
    Importing this function has zero side effects — safe for pytest.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    # In a Databricks environment, dbutils is available by default.
    try:
        api_token = dbutils.secrets.get(scope="mortgage-secrets", key="fraud-api-token")
    except NameError:
        api_token = "mock_token"

    url = "<YOUR_MOCKY_URL>"  # Replace with your generated designer.mocky.io URL

    data = fetch_fraud_blacklist(url, api_token)

    # In Databricks, we write to ADLS using standard python I/O by writing to the /dbfs mount or using dbutils.fs.put
    # For this exercise, we will assume dbutils.fs.put
    base_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net"
    destination_path = f"{base_path}/landing/fraud_blacklist/blacklist_today.json"

    try:
        dbutils.fs.put(destination_path, json.dumps(data), overwrite=True)
        print(f"Successfully landed API data to {destination_path}")
    except NameError:
        print(f"Local Mock: Would have written data to {destination_path}")
