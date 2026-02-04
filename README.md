# 🌍 Global Air Quality ETL & Analytics Project

## Overview
Air quality data is fragmented across multiple sources and often not analysis-ready.
The objective of this project is to design a batch ETL pipeline that collects global air quality data, cleans and standardizes it, stores it in an analytical database, and exposes insights through an interactive Power BI dashboard.

This project focuses on **data reliability, structure, and analytical usability, not real-time streaming.**.

---

## Scope of the Project
This project demonstrates:

-Script-driven batch ETL pipeline
-Modular extract, transform, and load layers
-Data validation and logging
-Analytical data modeling
-Business-focused visualization using Power BI

---

## Data Sources

**Air Quality Data**
-Source: OpenAQ public API
-Metrics: PM2.5, PM10, location, date

**Data Sets**
 Weather and population datasets (CSV-based)
 
---

## Architecture Block Diagram:-

<img width="500" height="500" alt="ETL" src="https://github.com/user-attachments/assets/5649bece-f10f-49e9-93f7-eff6a8053e3b" />


---

## Features

- **ETL Pipeline:** Automates extraction, transformation, and loading of data.
- **Data Cleaning:** Handles missing values, outliers, and inconsistent data formats.
- **Data Merging:** Combines air quality, weather, and population datasets.
- **Exploratory Data Analysis (EDA):** Generates insights through visualizations and statistical analysis.
- **Interactive Dashboard:** Visualizes trends, comparisons, and KPIs for global air quality.
- **Scalable & Reusable:** Scripts and workflow can be reused for new datasets.

---

## Technologies Used

- **Programming Languages:** Python (pandas, NumPy, matplotlib, seaborn)
- **Data Visualization:** Power BI
- **Database / SQL:**Postgres and pgadmin
- **Version Control:** Git and GitHub
- **Development Tools:** VS Code, Jupyter Notebook

---

## How to Run the Pipeline:-

1. **Clone the repository:**

git clone https://github.com/siddharthramgundam/Global-Air-Quality-ETL-Project.git
cd "Global Air Quality ETL Project"
Set up a virtual environment (recommended):


2️. Set up virtual environment

python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.\.venv\Scripts\activate    # Windows

3️. Install dependencies

pip install -r requirements.txt

4️. Run ETL pipeline

python pipeline/run_pipeline.py

logs/pipeline.log

## Dashboard Overview

-Time-based trends of air quality indicators (PM2.5 and PM10) to observe changes across different periods.
-Country-wise comparison of pollution levels to identify regions with relatively higher or lower particulate concentration.
-Distribution analysis of PM2.5 and PM10 values to understand overall pollution spread.
-AQI category breakdowns to classify air quality into interpretable health-related categories

## Sample Insights

Based on the available data and visual analysis:

-Urban and densely populated regions tend to exhibit higher PM2.5 concentrations compared to less populated areas.
-Seasonal variation in particulate matter levels is visible across multiple regions.
-Certain locations frequently fall into poorer AQI categories, indicating recurring air quality concerns.

## Author

**Siddharth Ramgundam**
**Email**    ramgundamsiddharth@gmail.com
**GitHub**   https://github.com/siddharthramgundam
**LinkedIn** https://www.linkedin.com/in/siddharth-ramgundam
