# Athlete Performance Analytics Pipeline

## Overview
This is an end-to-end data pipeline built to clean and migrate Olympic athlete data. It started as a local migration project into a **Dockerized SQL Server** using **Pandas** and is currently being scaled into a production-level **Big Data** environment.

The goal is to move the data to a distributed architecture that handles larger datasets efficiently by leveraging **PySpark** and **Hadoop HDFS**.

---

## The Tech Stack
* **Languages:** Python (Pandas + PySpark + SQLAlchemy)
* **Database:** SQL Server (Dockerized)
* **Big Data:** Hadoop HDFS & Dockerized Spark Cluster
* **Visualization:** Tableau

---

## Pipeline Architecture

# 1. Modular ETL Design
To ensure maintainability and professional standards, the pipeline is decoupled into specialized layers:
* `cleaning.py`: Handles raw data extraction using Regex to parse complex strings into structured formats.
* `transformations.py`: Contains the logic for feature engineering, such as age grouping and performance point mapping.
* `run_pipeline.py`: Orchestrates the flow, managing the connection between the Python environment and the SQL Server container.

## 2. Automated SQL Migration
The pipeline builds a structured data warehouse by loading multiple cleaned and summarized tables directly into SQL Server:
* `master_table`: The fully joined and cleaned primary dataset.
* `podium_appearance_age`: Aggregated medal appearance percentages by age group.
* `physical_performance`: Statistical summaries (mean/std) of athlete height and weight across disciplines.
* `yearly_total_points`: Total performance points calculated by year and demographic.


### Phase 1: Local ETL (Complete)
Used **Pandas** for the initial data cleaning, normalization, and schema validation. This was the "fast" way to get the data structured and moved into **SQL Server** tables.

### Phase 2: Distributed Processing (Current)
Moving the logic to **PySpark** to run on a **Docker-Hadoop** cluster. This transition handles the data as if it were a massive production dataset, leveraging HDFS for storage and Spark for distributed cleaning.

---

## Project Structure
* `docker-hadoop/` - Configurations for the Hadoop/Spark nodes
* `notebook/` - Exploration in `analysis.ipynb` and Spark development in `production.ipynb`
* `scripts/` - Modular Python scripts (`run_pipeline.py`, `cleaning.py`, `transformations.py`)
* `data/` - Raw and cleaned CSV samples

---
