# E-commerce Big Data Analysis with Spark, Hive, and ML

This project analyzes a multi-category e-commerce dataset (~5GB) using Apache Spark and Hive on Google Cloud Dataproc. It includes data processing, exploratory analysis, and machine learning using PySpark.

## Dataset

- Source: [E-Commerce Behavior Data from Multi Category Store on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- File used: `2019-Oct.csv`

## Tools and Technologies

- Google Cloud Platform (GCP) – Dataproc Cluster (Spark + Hive)
- Apache Spark (PySpark)
- HiveQL (via Dataproc)
- Google Cloud Storage (GCS)
- Python + scikit-learn (for ML visualization)
- Google Colab (for post-processing and export)
- GitHub

## Project Structure

| File Name               | Description                                                                 |
|-------------------------|-----------------------------------------------------------------------------|
| `spark_only.py`         | Spark-based data processing and visual analysis                             |
| `hive_only.py`          | HiveQL queries and summary statistics                                       |
| `ML_RF_Final.py`        | Baseline Random Forest with basic feature engineering                       |
| `ML_RF_Improved.py`     | Random Forest with advanced feature engineering and hyperparameter tuning   |
| `ML_Visualization.ipynb`| ROC curve, Confusion Matrix, and metrics visualization                      |
---


## Machine Learning

We used classification to predict whether a user action will lead to a purchase.

- **Model:** Random Forest Classifier (PySpark MLlib)
- **Features used:** `price`, `brand`, `category`, `hour`, `day_of_week`
- **Performance Metrics:**
  - Accuracy: ~0.65
  - AUC (ROC): ~0.69
  - Precision, Recall, F1-score calculated
- **Visuals included:**
  - Confusion Matrix
  - ROC Curve
  - Feature Importance plot

## How to Run

### If using GCP:
1. Upload `.py` files to a GCS bucket
2. SSH into your Dataproc cluster
3. Run: `exec(open("your_script.py").read())`

### Locally or on Colab (for ML visualization only):
1. Open `ML_Visualization.ipynb` in Google Colab
2. Download prediction results from GCS as CSV (`predictions_output` folder)
3. Upload CSV file to Colab environment
4. The notebook computes metrics (accuracy, precision, recall, F1), plots the ROC curve, confusion matrix, and feature importance

## Requirements

- PySpark
- pandas, numpy
- matplotlib, seaborn, scikit-learn (for visualization)

## GitHub Repository

[https://github.com/batuhan1734/Ecommerce_BigData_project](https://github.com/batuhan1734/Ecommerce_BigData_project)

---

📌 *This project was developed as part of the Big Data Analytics course at BSBI.*

