# Project Task 1: Solution

Here is the best-practice solution for provisioning an Azure SQL Database and securing its credentials in Databricks.

## 1. Provision the Database (Azure Portal)

While production databases are provisioned via CI/CD, creating one manually in the portal requires these steps:
1. Search for **SQL Databases** in the Azure Portal and click **Create**.
2. Select your `rg-mortgage-prod` Resource Group.
3. Database name: `sqldb-mortgage-servicing`.
4. Server: Click **Create new**.
   - Server name: `sqlserver-mortgage-<your_initials>`.
   - Authentication: Use **SQL authentication**.
   - Set a strong Server admin login (e.g., `mortgage_admin`) and Password.
5. Compute + Storage: Click **Configure database** and select the **Basic** tier (to minimize costs).
6. Networking: 
   - Connectivity method: **Public endpoint**.
   - Allow Azure services and resources to access this server: **Yes** *(This is critical—without this, Databricks cannot connect).*
   - Add current client IP address: **Yes**.
7. Click **Review + create**.

## 2. Secure the Credentials (Databricks CLI)

Open your terminal (ensuring you are authenticated via `databricks configure --token` with a token that has the `secrets` scope enabled).

1. Store the username:
   ```bash
   databricks secrets put-secret mortgage-secrets sql-db-username
   ```
   *When the prompt opens, type your admin username (e.g., `mortgage_admin`), save, and close.*

2. Store the password:
   ```bash
   databricks secrets put-secret mortgage-secrets sql-db-password
   ```
   *When the prompt opens, type your secure password, save, and close.*

## Verification
You can verify the secrets exist (though you cannot see their values) by listing them:
```bash
databricks secrets list-secrets mortgage-secrets
```

---
[⬅️ Back to Project Task 1](project-task-01.md) | [🏠 Main Directory](../../README.md)
