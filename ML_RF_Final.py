
from pyspark.sql.functions import col, hour
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Step 1: Load and clean the dataset
df = spark.read.csv("gs://ecommerce-data-bucket-test/2019-Oct.csv", header=True, inferSchema=True)
df_clean = df.na.drop()

# Step 2: Feature engineering
df_ml = df_clean.withColumn("hour", hour("event_time")) \
                .withColumn("label", (col("event_type") == "purchase").cast("integer"))

# Step 3: Handle class imbalance
purchase_df = df_ml.filter(df_ml.label == 1)
non_purchase_df = df_ml.filter(df_ml.label == 0).sample(False, 0.02, seed=42)
balanced_df = purchase_df.union(non_purchase_df)

# Step 4: Preprocessing
brand_indexer = StringIndexer(inputCol="brand", outputCol="brand_index", handleInvalid="skip")
assembler = VectorAssembler(inputCols=["price", "category_id", "hour", "brand_index"], outputCol="features")

# Step 5: Random Forest with increased maxBins
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxBins=2000)
pipeline = Pipeline(stages=[brand_indexer, assembler, rf])
train_data, test_data = balanced_df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_data)

# Step 6: Prediction and Evaluation
predictions = model.transform(test_data)

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
print("AUC (ROC):", evaluator.evaluate(predictions))

for metric in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
    eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
    print(f"{metric.capitalize()}: {eval.evaluate(predictions)}")
