# Project Task 1: Environment Provisioning

## Table of Contents
- [Objective](#objective)
- [Instructions](#instructions)
- [Acceptance Criteria](#acceptance-criteria)


## Objective
Simulate the provisioning of the Azure environment for the Mortgage Data Platform.

## Instructions
Since this is a simulated local environment, we will represent our cloud infrastructure via the folder structure we just created.

1. **Verify the "Storage Account":**
   Ensure the following directories exist under `apps/mortgage-data-platform/src`:
   - `bronze`
   - `silver`
   - `gold`

2. **Simulate Azure Key Vault:**
   - Navigate to `apps/mortgage-data-platform/config/`.
   - Create a file named `secrets_mock.json`.
   - Add the following simulated secrets:
     ```json
     {
       "adls-access-key": "mock-storage-key-123",
       "fraud-api-token": "mock-fraud-token-abc"
     }
     ```
   *(Note: In a real Azure Databricks workspace, these would be configured via the Databricks CLI and Azure Portal, not in a local JSON file. We use this file to simulate `dbutils.secrets.get` locally).*

## Acceptance Criteria
- [ ] You understand the purpose of Bronze, Silver, and Gold layers.
- [ ] The `config/secrets_mock.json` file is created and contains the mock secrets.

[⬅️ Previous: Lesson 1.3: Security Fundamentals](lesson-1.3-security-fundamentals.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 2: PySpark & Data Ingestion](../module-02-ingestion/README.md)
