# Lesson 1.2: Azure Resources & Infrastructure Overview

## Table of Contents
- [1. Introduction to the Cloud Ecosystem](#1-introduction-to-the-cloud-ecosystem)
- [2. Azure Resource Groups (The Organizational Container)](#2-azure-resource-groups-the-organizational-container)
- [3. Azure Data Lake Storage (ADLS) Gen2](#3-azure-data-lake-storage-adls-gen2)
- [4. Azure Databricks Workspace (Compute & Collaboration)](#4-azure-databricks-workspace-compute--collaboration)
- [5. Senior-Level Insights: Networking & Enterprise Security](#5-senior-level-insights-networking--enterprise-security)
- [6. 🎯 Interview Preparation](#6--interview-preparation)


## 1. Introduction to the Cloud Ecosystem
Before writing a single line of PySpark code, a Data Engineer must understand the underlying infrastructure. In Azure, Databricks does not exist in a vacuum; it is part of a tightly integrated ecosystem. We will focus on the three foundational pillars required for the Mortgage Data Platform: Resource Groups, Storage, and Compute.

---

## 2. Azure Resource Groups (The Organizational Container)
A Resource Group (RG) is a logical container that holds related resources for an Azure solution. 
- **Lifecycle Management:** In an enterprise, you group resources by environment (e.g., `rg-mortgage-dev`, `rg-mortgage-prod`). If you delete the Resource Group, everything inside it is deleted. This prevents orphaned resources from silently incurring costs.
- **Access Control:** You can apply Role-Based Access Control (RBAC) at the RG level, instantly granting a data engineer access to the storage account, Key Vault, and Databricks workspace simultaneously.

---

## 3. Azure Data Lake Storage (ADLS) Gen2
ADLS Gen2 is the foundational storage layer for our Lakehouse. It is built on top of Azure Blob Storage but includes a crucial feature: a **Hierarchical Namespace (HNS)**.

### Why HNS Matters
In standard Blob Storage (flat namespace), "folders" are just illusions created by slashes in the filename (e.g., `bronze/loans/data.parquet` is just a very long file name). If you try to rename or delete the `loans/` folder, the storage engine must iterate through and rename *every single file* inside it—a massive performance bottleneck.
ADLS Gen2 with HNS creates actual directories. Renaming a directory is an instant, atomic metadata operation, making big data processing orders of magnitude faster.

### Storage Layout for the Mortgage Platform
We provision a storage account (e.g., `stmortgagedev001`) and create three containers:
1. `bronze`
2. `silver`
3. `gold`
Inside each container, data is highly partitioned (e.g., `silver/loans/year=2023/month=10/`).

---

## 4. Azure Databricks Workspace (Compute & Collaboration)
Databricks is the compute engine that mounts and processes the data residing in ADLS Gen2. 

### The Control Plane vs. Data Plane Architecture
This is a critical senior-level concept:
- **Control Plane:** Hosted in Microsoft's Azure subscription. It manages the web UI, notebook code, job scheduling, and cluster management. 
- **Data Plane:** Hosted in *your* Azure subscription (inside your Virtual Network). This is where the actual virtual machines (clusters) run and process data. **Your data never leaves your environment.**

### Clusters (Compute Nodes)
To execute PySpark code, you spin up a cluster. 
- **Driver Node:** The master node that translates your PySpark code into tasks.
- **Worker Nodes:** The machines that actually execute the tasks in parallel across the partitioned data.
- **Interactive vs. Job Clusters:** You use interactive (All-Purpose) clusters to develop code in notebooks. In production, workflows spin up isolated, ephemeral **Job Clusters** which terminate immediately after the job finishes to save costs.

---

## 5. Senior-Level Insights: Networking & Enterprise Security
- **VNET Injection:** By default, Databricks creates its own Virtual Network (VNET) in your subscription. Senior architects use **VNET Injection** to deploy the Databricks Data Plane into an *existing* corporate VNET. This allows Databricks to securely communicate with on-premise databases (e.g., an on-premise SQL server holding mortgage applications) without going over the public internet.
- **Azure Private Link:** To ensure that traffic between Databricks and ADLS Gen2 never traverses the public internet, enterprises configure Private Endpoints. 

---

## 🛠️ Action Step: Provisioning the Data Lake

In a true enterprise production environment, you never provision resources manually using the Azure Portal (a practice known as "ClickOps"). Manual provisioning leads to human error, environment drift, and security vulnerabilities. 

**Production Standard:** Enterprise environments use **Infrastructure as Code (IaC)**, such as Terraform or Azure Bicep, to provision resources. This ensures deployments are version-controlled, peer-reviewed, and completely reproducible.

However, to keep this curriculum focused strictly on Data Engineering and Databricks (and avoid a detour into Terraform state management), we will simulate the deployment using the Azure CLI. This mirrors the exact programmatic API calls Terraform would make.

### Provisioning via Azure CLI
Open your local terminal or the Azure Cloud Shell and run the following commands to provision the storage account precisely to our Enterprise Standards:

1. **Create the Resource Group:**
   ```bash
   az group create --name rg-mortgage-prod --location eastus
   ```
2. **Create the Storage Account (ADLS Gen2):**
   *Note: Storage Account names must be globally unique across all of Azure. Please replace `<your_initials>` with your actual initials (or a random number) to ensure the name is available.*
   *Note: We strictly enforce `--enable-hierarchical-namespace true` (HNS) and use standard LRS to optimize cost. We also explicitly disable public access.*
   ```bash
   az storage account create \
       --name stmortgagedata<your_initials> \
       --resource-group rg-mortgage-prod \
       --location eastus \
       --sku Standard_LRS \
       --enable-hierarchical-namespace true \
       --allow-blob-public-access false
   ```
3. **Create the Medallion Containers:**
   ```bash
   az storage fs create -n bronze --account-name stmortgagedata<your_initials> --public-access off
   az storage fs create -n silver --account-name stmortgagedata<your_initials> --public-access off
   az storage fs create -n gold --account-name stmortgagedata<your_initials> --public-access off
   ```
4. **Create the Databricks Workspace:**
   *Note: We use the `premium` SKU because Unity Catalog (which we use later) requires it.*
   ```bash
   az databricks workspace create \
       --resource-group rg-mortgage-prod \
       --name dbw-mortgage-prod \
       --location eastus \
       --sku premium
   ```

*(If you do not have the Azure CLI installed, you may manually provision this in the Azure Portal. If doing so, you must explicitly select the "Advanced" tab during creation, check the box for "Enable hierarchical namespace", and explicitly disable public blob access in the "Configuration" tab).*

---

## 6. 🎯 Interview Preparation



> [!TIP]
> **Common Interview Questions on Cloud Infrastructure**

**Q1: What is the difference between Azure Blob Storage and ADLS Gen2, and why does Databricks prefer Gen2?**
**Answer:** "The primary difference is the Hierarchical Namespace (HNS). Blob storage uses a flat namespace, meaning directory operations like renames are O(n) operations based on the number of files. ADLS Gen2 uses true directories, making renames atomic O(1) operations. Since PySpark heavily relies on directory renaming when committing output files, ADLS Gen2 provides drastically better performance for big data workloads."

**Q2: Explain the architecture of Azure Databricks regarding the Control Plane and Data Plane.**
**Answer:** "Databricks uses a split architecture. The Control Plane, which hosts the web interface, notebooks, and management services, is managed by Microsoft in their subscription. The Data Plane, which consists of the actual VM clusters processing the data, resides in my company's Azure subscription. This ensures that our sensitive mortgage data never leaves our secure Virtual Network."

**Q3: How do you optimize costs for Databricks in a production environment?**
**Answer:** "I strictly enforce the use of **Job Clusters** for automated pipelines instead of All-Purpose clusters. Job clusters are billed at a significantly lower rate and terminate automatically upon completion. Additionally, I use auto-scaling to dynamically adjust worker nodes based on workload, and leverage Azure Spot Instances for non-critical, fault-tolerant workloads to save up to 80% on VM costs."

[⬅️ Previous: Lesson 1.1: Data Analytics & Medallion Architecture](lesson-1.1-intro-architecture.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 1.3: Security Fundamentals](lesson-1.3-security-fundamentals.md)
