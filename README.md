# 🏅 Athlete Performance Analytics Pipeline

## Overview
This is an end-to-end data pipeline built to clean and migrate Olympic athlete data. It started as a local migration project into **SQL Server** using **Pandas** and is currently being scaled into a production-level **Big Data** environment.

The goal was to move from basic data manipulation to a distributed architecture that handles larger datasets efficiently.

---

## The Tech Stack
* **Languages:** Python (Pandas + PySpark)
* **Database:** SQL Server (On-prem/Local)
* **Big Data:** Hadoop HDFS & Dockerized Spark Cluster
* **Visualization:** Tableau

---

## Pipeline Architecture

### Phase 1: Local ETL (Complete)
Used **Pandas** for the initial data cleaning, normalization, and schema validation. This was the "fast" way to get the data structured and moved into **SQL Server** tables.

### Phase 2: Distributed Processing (Current)
Moving the logic to **PySpark** to run on a **Docker-Hadoop** cluster. This transition handles the data as if it were a massive production dataset, leveraging HDFS for storage and Spark for distributed cleaning.



---

## Project Structure
* `docker-hadoop/` - Configurations for the Hadoop/Spark nodes
* `notebook/` - Exploration in `analysis.ipynb` and Spark development in `production.ipynb`
* `scripts/` - The main `run_pipeline.py` script
* `data/` - Raw and cleaned CSV samples

---

## Engineering Notes
* **Why the switch?** Pandas works great until the data hits your RAM limit. Using PySpark and Hadoop prepares this pipeline for actual large-scale athlete datasets.
* **SQL Integration:** Data is loaded into structured tables to make it ready for analysis in Tableau.