# Lesson 1.4: Infrastructure Smoke Test

## Table of Contents
- [Why Verify Before Writing Code?](#why-verify-before-writing-code)
- [Action Step: Preparing the Landing Zone](#action-step-preparing-the-landing-zone)
- [Action Step: Verifying ADLS Connectivity from Databricks](#action-step-verifying-adls-connectivity-from-databricks)
- [Action Step: Verifying the Secret Scope](#action-step-verifying-the-secret-scope)

[⬅️ Back to Main Course Directory](../../README.md)

---

> [!IMPORTANT]
> **Sequencing Note:** This lesson contains no Python scripts — those are built in Module 2. The sole purpose here is to verify that the Azure infrastructure you provisioned in Lessons 1.2 and 1.3 is fully wired together and working before you write a single line of PySpark. Discovering a misconfiguration here is much cheaper than discovering it in Module 3 when you are trying to run a pipeline.

---

## Why Verify Before Writing Code?

You have now provisioned:
- An **ADLS Gen2 storage account** with Bronze, Silver, and Gold containers
- An **Azure Databricks workspace** with a running cluster
- An **Azure Key Vault**-backed **Secret Scope** containing your database credentials

These three services need to communicate with each other for the Mortgage Data Platform to work. A misconfigured firewall rule, a missing role assignment, or a typo in a secret name will cause cryptic errors deep inside your pipeline code. The right time to catch those is now, by running a simple smoke test.

---

## 🛠️ Action Step: Preparing the Landing Zone

Before we can run any ingestion code, the raw source data needs to be in ADLS. In production, an upstream operational system drops files here automatically. For our learning environment, we upload a sample file manually.

1. Download or create a sample CSV file at `apps/mortgage-data-platform/data/sample_loans.csv` with the following content:

```csv
loan_id,applicant_ssn,loan_amount,credit_score,state,status
L001,123-45-6789,350000.00,720,TX,APPROVED
L002,987-65-4321,500000.00,680,CA,APPROVED
L003,111-22-3333,250000.00,590,FL,DENIED
L004,444-55-6666,425000.00,750,TX,APPROVED
L005,777-88-9999,600000.00,640,NY,PENDING
```

2. Using the **Azure Portal** or **Azure Storage Explorer**, upload this file to:
   ```
   bronze container → landing/loan_applications/sample_loans.csv
   ```

   Full ADLS path:
   ```
   abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/sample_loans.csv
   ```

> [!TIP]
> In production, this landing zone path is where the upstream Loan Origination System deposits files on a schedule. Our Auto Loader pipeline (Module 4) will watch this path and process new files as they arrive.

---

## 🛠️ Action Step: Verifying ADLS Connectivity from Databricks

Now let's confirm that Databricks can actually reach your storage account.

1. Open your **Azure Databricks workspace** in the browser.
2. Navigate to **Workspace → Create → Notebook**.
3. Name it `smoke-test` and attach it to your cluster.
4. In the first cell, run the following:

```python
# Cell 1: Verify ADLS Gen2 is reachable from Databricks
files = dbutils.fs.ls("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/")
display(files)
```

**Expected output:** A table showing `sample_loans.csv` with its file size and modification time.

**If you get an error like `AuthorizationPermissionMismatch`:** Your Databricks cluster's **Managed Identity** does not have the **Storage Blob Data Contributor** role assigned on the storage account. Return to Lesson 1.2 and verify the role assignment in the Azure Portal.

---

## 🛠️ Action Step: Verifying the Secret Scope

In the same notebook, add a second cell to confirm the Key Vault-backed Secret Scope is correctly configured:

```python
# Cell 2: Verify the Secret Scope is working
# This should print your SQL DB username without raising an error
db_user = dbutils.secrets.get(scope="mortgage-secrets", key="sql-db-username")
print(f"Secret retrieved successfully. Username starts with: {db_user[:3]}***")
```

**Expected output:** Something like `Secret retrieved successfully. Username starts with: adm***`

> [!CAUTION]
> Never print the full secret value (`db_user`) — only print partial values as confirmation. Even in a development notebook, full secret values in output are a security violation that can be captured in logs.

**If you get `Secret does not exist`:** Return to Lesson 1.3 and verify that the `sql-db-username` key was added to the Key Vault and that the Secret Scope is correctly linked.

---

## ✅ Smoke Test Checklist

Before proceeding to Module 2, confirm all three items pass:

- [ ] `dbutils.fs.ls(...)` returns `sample_loans.csv` — ADLS is accessible
- [ ] `dbutils.secrets.get(...)` returns a value without error — Secret Scope is working
- [ ] Your cluster is running on **Databricks Runtime 14.3+** — required for Databricks Connect in Lesson 2.2

Once all three pass, your infrastructure is fully operational. **The data in the landing zone is ready and waiting for the ingestion script you will build in Lesson 2.2.**

---

[⬅️ Previous: Lesson 1.3: Security Fundamentals](lesson-1.3-security-fundamentals.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 1](project-task-01.md)
