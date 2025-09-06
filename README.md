# princeton-comscore-pipeline
This repo contains work from my time at Princeton to create an ETL pipeline for a massive commercial dataset sourced from comScore. [Kai Pak](https://www.linkedin.com/in/kaipak/) and [Lilly Amirjavadi](https://www.linkedin.com/in/lilly-amirjavadi-415512215/) also contributed to code that was re-used here.

## Table of Contents
- [Overview](#overview)
- [Key Technologies](#key-technologies)
- [Repository Structure](#repository-structure)
- [Deployment Process](#deployment-process)
- [Acknowledgement](#acknowledgement)

---
## Overview
At Princeton we structured our pipelines according to a medallion architecture. [Chris Etheridge](https://www.linkedin.com/in/christopher-etheridge-msda/) did the bulk of the work on the gold layer, but for this pipeline I owned the development of the bronze and silver layers. Medallion architecture generally works as follows:

- **Bronze** → Ingests raw data
- **Silver** → Cleans and transforms the ingested data into a structured format.
- **Gold** → Aggregates and prepares data for reporting, dashboards, and analytics.

These layers are **managed using Delta Live Tables (DLT)** to enable **incremental updates, data quality enforcement, and optimized performance**.

---
## Key Technologies
The pipeline was built with:

- **Delta Live Tables (DLT)** – Automates ETL workflows.
- **Databricks Asset Bundles (DAB)** – Manages pipeline deployments via `bundle.yml`.
- **Unity Catalog** – Organizes and secures data across environments.
- **PySpark & SQL** – Handles transformations. In the case of this dataset, many tables had to be joined to enable lookups and proper formatting.

---
## Repo Structure
For simplicity in this repository I have omitted portions of the code which I did not write or which were unused in the comScore pipeline. However, when fully functioning the repository was organized as below.

```plaintext
data-pipelines/
│
├── bundles/                # Organized pipeline bundles
│   ├── bronze/             # Bronze (raw data ingestion)
│   │   ├── comscore/
│   │   │   ├── bundle.yml  # Databricks Asset Bundle config
│   │   │   ├── *_pipeline.py # DLT pipeline script
│   │   │   └── sql/        # SQL transformations
│   │   │   └── tests/      # DLT tests
│   │   ├── youtube/
│   │   ├── ...
│   ├── silver/             # Silver (data cleansing & transformation)
│   │   ├── comscore/
│   │   ├── youtube/
│   │   ├── ...
│   ├── gold/               # Gold (data aggregation & preparation)
│       ├── dashboard/
│       ├── analytics/
│       ├── ...
│
├── notebooks/              # Exploratory & debugging notebooks
│   ├── bronze_2.0_poc.ipynb
│   ├── foo.ipynb
│
├── src/                    # Shared Python utility functions
│   ├── __init__.py
│   ├── ingestion.py
│   ├── utils.py
│
└── README.md               # Documentation
```
## CI/CD Pipeline
**GitHub Actions** automated deployments, while **Databricks Asset Bundles (DAB)** handled builds and deployments.

## Acknowledgement
I stole most of this README from [Kai Pak](https://www.linkedin.com/in/kaipak/) and edited it to provide sample code once I left Princeton.

