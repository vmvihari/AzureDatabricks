# Project Task 1: Provisioning the Operational Database

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
Now that you have built the foundational Azure resources (ADLS Gen2 and Databricks) and secured the Fraud API token in a Secret Scope via the lesson Action Steps, it is time for your first independent challenge.

## The Challenge
The Mortgage Data Platform receives operational updates (Change Data Capture) from an upstream Loan Servicing System. This system stores its data in a relational SQL database.

You must provision an **Azure SQL Database** and secure its credentials, without a step-by-step tutorial.

1. **Provision the Database:**
   - Use the Azure Portal to create a new Azure SQL Database (e.g., `sqldb-mortgage-servicing`).
   - Use the cheapest tier (Basic) for this learning environment.
   - Configure the firewall rules to allow Azure services (like Databricks) to access the server.
   
2. **Secure the Credentials:**
   - Retrieve the admin username and password you created for the SQL Server.
   - Using the Databricks CLI, add these two credentials into the `mortgage-secrets` scope you created earlier.
   - Name the keys `sql-db-username` and `sql-db-password`.

## Acceptance Criteria
- [ ] An Azure SQL Database is running in your Resource Group.
- [ ] You have successfully used `databricks secrets put-secret` to store the database username and password in the `mortgage-secrets` scope.

---
[⬅️ Previous: Lesson 1.3: Security Fundamentals](lesson-1.3-security-fundamentals.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 2: PySpark & Data Ingestion](../module-02-ingestion/README.md)
