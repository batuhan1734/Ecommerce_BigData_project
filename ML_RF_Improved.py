
from pyspark.sql.functions import col, hour, dayofweek
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Step 1: Load and clean the dataset
df = spark.read.csv("gs://ecommerce-data-bucket-test/2019-Oct.csv", header=True, inferSchema=True)
df_clean = df.na.drop()

# Step 2: Feature engineering
df_ml = df_clean.withColumn("hour", hour("event_time")) \
                .withColumn("day_of_week", dayofweek("event_time")) \
                .withColumn("label", (col("event_type") == "purchase").cast("integer"))

# Step 3: Handle class imbalance by undersampling
purchase_df = df_ml.filter(df_ml.label == 1)
non_purchase_df = df_ml.filter(df_ml.label == 0).sample(False, 0.03, seed=42)
balanced_df = purchase_df.union(non_purchase_df)

# Step 4: Feature transformation
brand_indexer = StringIndexer(inputCol="brand", outputCol="brand_index", handleInvalid="skip")
category_indexer = StringIndexer(inputCol="category_code", outputCol="category_index", handleInvalid="skip")

# Updated feature vector includes additional engineered features
assembler = VectorAssembler(
    inputCols=["price", "category_index", "brand_index", "hour", "day_of_week"],
    outputCol="features"
)

# Step 5: Random Forest with tuned hyperparameters
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=10,
    maxBins=2000,
    minInstancesPerNode=5
)

pipeline = Pipeline(stages=[category_indexer, brand_indexer, assembler, rf])

# Train-test split
train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_data)

# Step 6: Evaluate the model
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
print("AUC (ROC):", evaluator.evaluate(predictions))

# Print detailed classification metrics
for metric in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
    eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
    print(f"{metric.capitalize()}: {eval.evaluate(predictions)}")

# Print feature importances
rf_model = model.stages[-1]
print("Feature Importances:", rf_model.featureImportances)
