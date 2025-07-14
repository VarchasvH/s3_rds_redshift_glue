# 🛠️ End-to-End AWS Data Pipeline with PySpark, Airflow & Redshift

This project showcases a production-grade, cloud-native ETL pipeline using **AWS services**, **PySpark**, and **Apache Airflow**. It simulates real-world batch processing, transforming raw data into analytics-ready insights.

---

## 📌 Architecture Overview

![ETL Architecture](https://github.com/user-attachments/assets/048c022f-e40f-47ee-80b9-64be802aead6)

---

## 🚀 Technologies Used

| Category          | Tools & Services                              |
|------------------|-----------------------------------------------|
| **Cloud Storage** | Amazon S3                                      |
| **Data Processing** | PySpark on AWS Glue                            |
| **Database (Staging)** | AWS RDS (MySQL/PostgreSQL)                     |
| **Data Warehouse** | Amazon Redshift                               |
| **Orchestration** | Apache Airflow (Custom DAGs)                   |
| **Visualization** | Amazon QuickSight                              |
| **Notifications** | AWS SNS (Email alerts)                         |

---

## 🔄 ETL Workflow

### 🥇 Stage 1: Raw Data → S3
- Upload raw `.csv` data files to **Amazon S3**

### 🧹 Stage 2: S3 → RDS (Cleaning & Staging)
- `scripts/s3_to_rds.py` reads from S3
- Applies data cleaning/transformation using **PySpark**
- Loads structured data into **AWS RDS**

### 📊 Stage 3: RDS → Redshift (Enrichment & Aggregation)
- `scripts/rds_to_redshift.py` fetches cleaned data from RDS
- Performs additional transformations
- Loads final dataset into **Amazon Redshift**

### 📅 Orchestration
- **Apache Airflow DAG**: `dags/etl_airflow_dag.py` manages the pipeline schedule, retries, dependencies, and task status.

### 📈 Visualization
- **Amazon QuickSight** dashboard is connected to Redshift for interactive reports and KPIs.

### 🔔 Notifications
- Pipeline success/failure is monitored via **AWS SNS** alerts to email.

---

## 🗂️ Project Structure

```bash
etl-pipeline-aws/
├── README.md
├── dags/
│   └── etl_airflow_dag.py         # Airflow DAG definition
├── scripts/
│   ├── s3_to_rds.py               # Stage 1: S3 → RDS
│   └── rds_to_redshift.py         # Stage 2: RDS → Redshift
├── data/
│   └── sample_data.csv            # Sample input data
├── diagram/
│   └── etl_pipeline_architecture.png
```
---

## 📊 Output Summary

| Component            | Description                                                    |
|---------------------|----------------------------------------------------------------|
| **Amazon RDS**       | Contains cleaned and structured staging data after ETL Stage 1 |
| **Amazon Redshift**  | Stores analytics-ready, transformed data for dashboards        |
| **Amazon QuickSight**| Visualizes key business metrics in real time                   |
| **AWS SNS**          | Sends automated email alerts on pipeline success/failure       |

---

## ✅ Features

* Modular pipeline with retry and alerting mechanisms
* Separated transformation logic for RDS and Redshift stages
* Visual KPIs using Amazon QuickSight
* Designed for scalability and real-world AWS environments

