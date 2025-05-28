# 🛠️ AWS ETL Data Pipeline with PySpark, Airflow & Redshift

This project demonstrates a robust, end-to-end data pipeline using AWS services, PySpark, and Apache Airflow. The pipeline ingests raw data from S3, processes and loads it into RDS, further transforms it, and loads it into Redshift for analytics and visualization.

---

## 📌 Architecture Overview

![ETL Architecture](diagram/etl_pipeline_architecture.png](https://github.com/user-attachments/assets/048c022f-e40f-47ee-80b9-64be802aead6)

---

## 🚀 Technologies Used

- **AWS S3** – Cloud storage for raw data
- **PySpark** – Data cleaning and transformation
- **AWS RDS** – Intermediate storage using PostgreSQL
- **AWS Redshift** – Data warehousing for analytics
- **Amazon QuickSight** – Business intelligence and dashboards
- **Apache Airflow** – Workflow orchestration and automation
- **AWS SNS** – Notification alerts via email

---

## 🔄 ETL Process Workflow

1. **Raw Data Ingestion**  
   Data is uploaded to **Amazon S3**

2. **ETL Stage 1 – S3 to RDS**  
   `scripts/s3_to_rds.py` reads and processes the data using **PySpark**, and loads it into **Amazon RDS**

3. **ETL Stage 2 – RDS to Redshift**  
   `scripts/rds_to_redshift.py` fetches data from RDS, applies further transformations, and loads it into **Amazon Redshift**

4. **Orchestration**  
   All steps are scheduled and monitored via the **Airflow DAG** in `dags/etl_airflow_dag.py`

5. **Visualization**  
   Cleaned data is visualized using **Amazon QuickSight** dashboards

6. **Notifications**  
   Success/failure notifications sent via **AWS SNS**

---

## 🗂️ Project Structure
etl-pipeline-aws/
├── README.md
├── dags/
│ └── etl_airflow_dag.py
├── scripts/
│ ├── s3_to_rds.py
│ └── rds_to_redshift.py
├── data/
│ └── sample_data.csv
├── diagram/
│ └── etl_pipeline_architecture.png

📊 Output
RDS: Cleaned, structured data

Redshift: Aggregated analytics-ready data

QuickSight Dashboards: Visual KPIs

SNS Alerts: Email notifications on success/failure

🧪 Sample Data
Located in /data/sample_data.csv – this sample represents the raw input file used for pipeline testing.
