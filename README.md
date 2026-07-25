# Azure Databricks Knowledge Repository

Welcome to the Azure Databricks Knowledge Repository! This repository contains a comprehensive, hands-on curriculum for mastering Databricks, Data Engineering, and Lakehouse architectures using a real-world enterprise use case: the **Mortgage Data Platform (MDP)**.

## 📚 Curriculum Overview

The course is structured into 7 core modules, blending theory with incremental, practical project tasks.

- **[Module 1: Cloud Fundamentals & Azure Setup](docs/module-01-fundamentals/README.md)**
  - Architecture, Medallion framework, and initial Azure resource provisioning.
- **[Module 2: PySpark & Comprehensive Data Ingestion](docs/module-02-ingestion/README.md)**
  - Reading from Files, JDBC (Databases), and HTTP (REST APIs).
- **[Module 3: Delta Lake & Batch Medallion Architecture](docs/module-03-medallion-and-ml/README.md)**
  - Building Bronze, Silver, Gold layers; Fraud API integration and ML modeling.
- **[Module 4: Advanced Ingestion & Delta Live Tables (DLT)](docs/module-04-dlt/README.md)**
  - Auto Loader, Change Data Capture (SCD Type 1/2), and declarative pipelines.
- **[Module 5: Performance Tuning & Optimization](docs/module-05-performance/README.md)**
  - Liquid Clustering, Z-ORDER, AQE, and handling data skew.
- **[Module 6: Data Governance with Unity Catalog](docs/module-06-governance/README.md)**
  - Metastore setup, RBAC, Data Lineage, and PII masking.
- [Module 7: Orchestration, Monitoring & CI/CD](docs/module-07-ops-cicd/README.md)
  - Databricks Workflows, Azure Monitor, Git integration, and GitHub Actions.

## 🚀 The Project: Mortgage Data Platform (MDP)

Throughout these modules, you will incrementally build the **Mortgage Data Platform**.
The codebase and architectural blueprint are located in the **[apps/mortgage-data-platform](apps/mortgage-data-platform/README.md)** directory.

### Business Case
A national mortgage lender needs a scalable data lakehouse to process daily loan applications. You will:
1. Ingest loan data from simulated databases and file systems.
2. Cross-reference applicants with a Fraud Detection API (Blacklist).
3. Apply an AI prediction model to determine loan default risk.
4. Securely expose this aggregated risk data to downstream analysts using Unity Catalog.

### 🛠️ Data Generation Utility
To build a realistic Lakehouse, you need massive amounts of data. Instead of bloating this repository with gigabytes of CSV files, we have provided a dynamic **[Data Generator Utility](apps/data-generator/README.md)**. 
Before starting the modules, you can use this utility to locally generate millions of rows of mock loan applications, fraud blacklists, and CDC servicing events directly into your simulated Data Lake!

### Repository Structure
```text
.
├── docs/                   # Markdown lessons and task instructions
│   ├── module-01-fundamentals/
│   ├── module-02-ingestion/
│   └── ...
└── apps/                   # Project Codebase
    ├── data-generator/         # Python utility to mock massive datasets
    └── mortgage-data-platform/
        ├── src/            # PySpark/SQL source code
        │   ├── bronze/     # Raw Ingestion (Auto Loader, API Calls)
        │   ├── silver/     # Cleansing, CDC, and Join logic
        │   ├── gold/       # Business aggregations and ML models
        │   └── dlt/        # Delta Live Tables pipelines
        ├── config/         # Environment configurations
        ├── tests/          # Unit and integration tests
        └── .github/        # CI/CD Workflows
```

## Getting Started
Navigate to **[Module 1](docs/module-01-fundamentals/README.md)** to begin the curriculum. Follow the lessons sequentially, and complete the **Project Task** at the end of each module to build the platform.
