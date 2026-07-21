# Lesson 1.2: Azure Resources & Infrastructure Overview

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

## 6. 🎯 Interview Preparation

> [!TIP]
> **Common Interview Questions on Cloud Infrastructure**

**Q1: What is the difference between Azure Blob Storage and ADLS Gen2, and why does Databricks prefer Gen2?**
**Answer:** "The primary difference is the Hierarchical Namespace (HNS). Blob storage uses a flat namespace, meaning directory operations like renames are O(n) operations based on the number of files. ADLS Gen2 uses true directories, making renames atomic O(1) operations. Since PySpark heavily relies on directory renaming when committing output files, ADLS Gen2 provides drastically better performance for big data workloads."

**Q2: Explain the architecture of Azure Databricks regarding the Control Plane and Data Plane.**
**Answer:** "Databricks uses a split architecture. The Control Plane, which hosts the web interface, notebooks, and management services, is managed by Microsoft in their subscription. The Data Plane, which consists of the actual VM clusters processing the data, resides in my company's Azure subscription. This ensures that our sensitive mortgage data never leaves our secure Virtual Network."

**Q3: How do you optimize costs for Databricks in a production environment?**
**Answer:** "I strictly enforce the use of **Job Clusters** for automated pipelines instead of All-Purpose clusters. Job clusters are billed at a significantly lower rate and terminate automatically upon completion. Additionally, I use auto-scaling to dynamically adjust worker nodes based on workload, and leverage Azure Spot Instances for non-critical, fault-tolerant workloads to save up to 80% on VM costs."
