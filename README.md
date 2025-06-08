# E-commerce Big Data Analytics Project

This project explores large-scale e-commerce behavioral data using Apache Spark and Hive on Google Cloud Platform (GCP). It includes data processing, exploratory analysis, and machine learning classification using Random Forest algorithms.

## Dataset

- **Source**: [Kaggle – E-Commerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- **File Used**: `2019-Oct.csv` (~5.3 GB)
- **Storage**: Google Cloud Storage (GCS)

---

## Technologies Used

- Google Cloud Dataproc (Spark + Hive)
- PySpark (for data wrangling and ML)
- HiveQL (for SQL-style data analysis)
- GitHub (for version control and collaboration)

---

## Project Structure

| File Name             | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| `spark_only.py`       | Spark-based data processing and visual analysis                             |
| `hive_only.py`        | HiveQL queries and summary statistics                                       |
| `ML_RF_Final.py`      | Baseline Random Forest with basic feature engineering                       |
| `ML_RF_Improved.py`   | Random Forest with advanced feature engineering and hyperparameter tuning   |

---

## Machine Learning Details

- **Target Variable**: Purchase (binary classification)
- **Algorithm**: Random Forest (Spark MLlib)
- **Metrics Evaluated**: Accuracy, F1-Score, AUC (ROC), Precision, Recall

---

## Running the Code

All code was executed in a PySpark shell over Google Cloud Dataproc via SSH. To run locally or on another platform:

1. Install required packages:
   ```bash
   pip install pyspark
