# Retail Data Platform (Enterprise CDC Pipeline)

This project contains the end-to-end declarative data pipelines for the Retail Data Platform, designed to extract Change Data Capture (CDC) events from an on-premise transactional database and process them through the Databricks Lakehouse architecture.

## Architecture Overview
1. **Extraction (ADF)**: Azure Data Factory securely extracts data from on-premise SQL Server using a Self-Hosted Integration Runtime and a robust 4-step Watermarking Architecture.
2. **Ingestion (Databricks Auto Loader)**: The CDC Parquet files dropped in ADLS Gen2 are incrementally ingested by Databricks Auto Loader into the Bronze Delta layer.
3. **Processing (Delta Live Tables)**: The pipeline uses DLT to manage data quality expectations and declarative pipeline execution.

---

# ADF Development Deployment Guide

This section provides step-by-step instructions for an enterprise data engineer to provision a Dev Azure Data Factory and integrate it with this repository. 

By following this guide, ADF will automatically parse our JSON files in the `adf/` directory and generate the visual pipelines.

## Step 1: Provision Core Azure Resources

You will need the following resources deployed in your Development Azure Resource Group:
1. **Azure Data Factory (V2)**: The core orchestration service.
2. **Azure Key Vault**: To securely store the on-premise SQL database password.
3. **Azure Data Lake Storage Gen2 (ADLS)**: The drop zone for the CDC Parquet files.

## Step 2: Configure Git Integration in ADF

This is the most critical step to connect the cloud ADF workspace to our code base.

1. Open **Azure Data Factory Studio**.
2. Go to the **Manage** tab (the wrench icon on the far left).
3. Under **Source Control**, select **Git configuration** -> **Configure**.
4. Select your repository type (e.g., Azure DevOps Git or GitHub).
5. Enter your repository details (Organization, Project, Repository name).
6. **CRITICAL SETTINGS**:
   - **Collaboration Branch**: `main` (or your current working branch).
   - **Publish Branch**: `adf_publish` (default).
   - **Root Folder**: `/apps/retail-data-platform/adf` (This tells ADF exactly where to look for our JSON files!).
7. Click **Save**. ADF will instantly sync the repo, parse our JSON files, and your pipelines will magically appear in the **Author** tab!
   - *Note on Parameters (Design Time vs Run Time):* ADF parses the JSON files purely for visual representation (Design Time). The parameters defined in the JSON (like `DltPipelineId`) are not evaluated until you actually execute the pipeline. When you click "Debug" in the UI, ADF switches to Run Time and will prompt you to input the real values!

## Step 3: Setup the Self-Hosted Integration Runtime (SHIR)

1. In ADF Studio, go to the **Manage** tab -> **Integration Runtimes**.
2. Click **+ New** -> **Azure, Self-Hosted** -> **Self-Hosted**.
3. Name it `SelfHostedIntegrationRuntime` (This must exactly match the name we hardcoded in our `OnPremSqlLinkedService.json`).
4. Install the provided SHIR executable on a Windows Server VM that has line-of-sight to your on-premise SQL database.
5. Register the VM using the authentication key provided by ADF.

## Step 4: Setup Azure Key Vault Credentials

1. Go to your **Azure Key Vault** in the Azure Portal.
2. Navigate to **Secrets** and click **+ Generate/Import**.
3. Name the secret `SqlPassword` (Must exactly match the secret name in our JSON).
4. Paste the actual database password as the value.
5. Navigate to **Access control (IAM)** in the Key Vault.
6. Grant the **Data Factory's System Assigned Managed Identity** the `Key Vault Secrets User` role so ADF is authorized to read the password at runtime.

## Step 5: Test the Pipeline

1. In ADF Studio, go to the **Author** tab.
2. Open the `cdc_ingestion_pipeline`.
3. Click **Debug** to run a test execution. ADF will:
   - Use the SHIR to connect to the SQL server using the Key Vault password.
   - Run the Watermark lookups.
   - Execute the CDC extraction query.
   - Drop the Parquet files into your ADLS drop zone, ready for our Databricks pipeline to pick them up!

## Step 6: Schedule the End-to-End Execution

In a real-world enterprise environment, you will automate this pipeline using a Schedule Trigger that passes the required parameters to the Databricks pipeline.

1. In ADF Studio, go to the **Author** tab and open `cdc_ingestion_pipeline`.
2. Click **Add trigger** (at the top of the canvas) -> **New/Edit**.
3. In the dropdown, select **+ New**.
4. Name the trigger `15Min_CDC_Trigger`.
5. Set the **Type** to **Schedule** and configure it to run every **15 Minutes**. Click OK.
6. **The Parameter Handoff**: ADF will immediately prompt you with a "Trigger Run Parameters" window. This is where you inject the values that will be used for every scheduled run:
   - `DatabricksWorkspaceUrl`: Paste your Databricks workspace URL (e.g., `https://adb-<workspace-id>.azuredatabricks.net`).
   - `DltPipelineId`: Paste the Pipeline ID you received after deploying your Databricks Asset Bundle.
7. Click **OK**, then click **Publish all** to make the trigger live. ADF is now your master orchestrator!

---

# Databricks Deployment Guide

This guide walks you through the exact steps required to provision the Azure Databricks environment, configure security, deploy the pipeline code, and retrieve the required IDs to link it to Azure Data Factory.

## Phase 1: Provision the Azure Databricks Environment

Before deploying the code, you must create the physical infrastructure in Azure.

1. **Create the Azure Databricks Workspace**
   - Log into the Azure Portal.
   - Search for **Azure Databricks** and click **Create**.
   - **Pricing Tier**: You *must* select **Premium**. Unity Catalog (governance) and Delta Live Tables (our pipeline framework) require Premium compute.
   - Click **Review + create** and wait for the workspace to provision.

2. **Create the Access Connector for Azure Databricks**
   - *Note: Yes, this must be explicitly created in Azure!* It acts as the secure Managed Identity for Unity Catalog.
   - In the Azure Portal, search for **Access Connector for Azure Databricks** and click **Create**.
   - Name it (e.g., `databricks-access-connector`) and deploy it to your Resource Group.
   - Once deployed, copy its **Resource ID** from the properties page.

3. **Configure Unity Catalog (Data Governance)**
   - Unity Catalog manages access to all your files and tables.
   - Go to the Databricks Account Console at `manage.databricks.com`.
   - Click **Data** -> **Create Metastore**.
   - You will need to provide the ADLS Gen2 path for managed tables (e.g., `abfss://metastore@<your-storage-account>.dfs.core.windows.net/`) and paste the **Resource ID** of the Access Connector you just created.
   - Once created, assign the Metastore to your newly created Databricks workspace.

4. **Grant ADLS Storage Permissions to Databricks**
   - Databricks needs permission to read the Parquet files that ADF drops.
   - In the Azure Portal, go to your ADLS Gen2 Storage Account.
   - Go to **Access Control (IAM)** -> **Add role assignment**.
   - Assign the **Storage Blob Data Contributor** role to the **Access Connector for Azure Databricks** you created in Step 2.

## Phase 2: Configure Deployment Authentication

Enterprise pipelines should never be tied to a single developer's account. We use a Service Principal.

1. **Create the Service Principal (App Registration)**
   - In Azure Portal, search for **Microsoft Entra ID** (formerly Azure AD).
   - Go to **App Registrations** -> **New registration**. Name it `Databricks-Deployment-SP`.
   - Once created, copy the **Application (client) ID** and **Directory (tenant) ID**.
   - Go to **Certificates & secrets**, create a new Client Secret, and copy the **Secret Value**.
2. **Add the Service Principal to Databricks**
   - Open your Databricks Workspace UI.
   - Go to the **Admin Console** (top right dropdown) -> **Service Principals**.
   - Add the Service Principal using its Client ID. Grant it **Admin** privileges so it can deploy jobs.

## Phase 3: Setup Databricks Target Schemas

1. **Create the Target Catalog and Schema**
   - Open your Databricks Workspace UI and navigate to the **SQL Editor**.
   - Run the following commands to create the target locations defined in our `retail_bronze_pipeline.yml`:
     ```sql
     CREATE CATALOG IF NOT EXISTS main;
     CREATE SCHEMA IF NOT EXISTS main.retail_bronze_dev;
     ```

## Phase 4: Deploy and Run the Pipeline

With the infrastructure ready, you can now push the code from this repository into Databricks using the Databricks CLI.

1. **Authenticate the Databricks CLI locally**
   - Open your VS Code terminal and run:
     ```bash
     databricks auth login --host https://adb-<workspace-id>.azuredatabricks.net
     ```
   - This opens a browser window. Log in with your Databricks credentials to link your CLI.

2. **Deploy the Asset Bundle**
   - Ensure you are in the project root (`apps/retail-data-platform`).
   - Run the deployment command targeting your personal developer sandbox:
     ```bash
     databricks bundle deploy -t dev
     ```
   - This command reads `databricks.yml`, uploads `ingest_cdc_bronze.py`, and creates the Delta Live Tables pipeline in Databricks.

3. **Retrieve the Pipeline ID for ADF (Crucial Step)**
   - During deployment, the CLI will output details about the created resources. Look for the **Pipeline ID** (a long UUID string). 
   - Alternatively, open the Databricks UI, go to **Workflows -> Delta Live Tables**, click `retail_cdc_bronze_pipeline_dev`, and copy the Pipeline ID from the details pane.
   - *You will paste this Pipeline ID into the ADF Schedule Trigger parameters.*

4. **Verify the Pipeline Runs Successfully**
   - Run a manual test execution from your terminal:
     ```bash
     databricks bundle run retail_bronze_pipeline -t dev
     ```
   - Once it completes, go to the Databricks SQL Editor and verify the data arrived:
     ```sql
     SELECT * FROM main.retail_bronze_dev.bronze_orders;
     ```

## Phase 5: End-to-End Orchestration (Cost-Optimized)

In production, you do not trigger the Databricks pipeline manually. We have implemented **Pattern A (Cost-Optimized Orchestration)** directly in the ADF pipeline (`cdc_ingestion_pipeline.json`).

**CRITICAL PREREQUISITE: Grant ADF Permission in Databricks**
Before ADF can trigger Databricks, Databricks must be told to trust ADF's Managed Identity:
1. In the Azure Portal, go to your Azure Data Factory resource. Under **Properties**, copy the **Managed Identity Object ID**.
2. Open your Databricks Workspace UI and go to the **Admin Console** -> **Service Principals**.
3. Click **Add Service Principal**, select **Azure Enterprise Application**, and paste the Object ID you copied.
4. Go to your Delta Live Tables pipeline permissions, and grant this newly added ADF identity **Can Run** or **Can Manage** permissions.

Here is how the true end-to-end flow operates automatically:

1. **ADF Runs on a Schedule**: The ADF schedule trigger executes the pipeline (e.g., every 15 minutes) using the parameters (`DltPipelineId`) you injected when creating the trigger.
2. **Data Extraction**: The `ForEach` loop extracts the precise CDC delta and copies it into ADLS.
3. **Automated Databricks Trigger**: Immediately after the extraction loop succeeds, the `TriggerDatabricksBronzePipeline` Web Activity executes.
   - It hits the Databricks REST API (`/api/2.0/pipelines/{pipeline_id}/updates`).
   - It securely authenticates using ADF's **System-Assigned Managed Identity**.
   - This wakes up the Databricks DLT pipeline to process the newly dropped files, and shuts down the cluster once it finishes, saving significant compute costs!
