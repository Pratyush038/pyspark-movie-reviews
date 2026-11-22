"""DAS Module 1 - Homework 1.2: process the IMDb 50K movie review dataset with PySpark."""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, length, max as max_, min as min_, trim

INPUT = sys.argv[1] if len(sys.argv) > 1 else "data/IMDB Dataset.csv"
OUTPUT = sys.argv[2] if len(sys.argv) > 2 else "output/cleaned_reviews"

spark = SparkSession.builder.appName("IMDbMovieReviews").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the dataset (reviews contain commas, quotes and newlines -> multiLine + escape)
df = spark.read.csv(INPUT, header=True, inferSchema=True,
                    multiLine=True, escape='"')

# 2. Explore the data
print("\n=== Schema ===")
df.printSchema()
print("=== First 5 rows ===")
df.show(5, truncate=80)

# 3. Clean: remove rows with null or empty reviews
raw_count = df.count()
clean = df.filter(col("review").isNotNull() & (trim(col("review")) != ""))
print(f"Rows before cleaning: {raw_count}")

# 4. Analyze
total = clean.count()
print(f"=== Total reviews after cleaning: {total} ===")
print("=== Sentiment counts ===")
clean.groupBy("sentiment").count().show()
pos = clean.filter(col("sentiment") == "positive").count()
neg = clean.filter(col("sentiment") == "negative").count()
print(f"Positive: {pos} | Negative: {neg}")

# 5. Transform: review length (characters) and filter > 500
transformed = clean.withColumn("review_length", length(col("review")))
print("=== Review length (characters) by sentiment ===")
transformed.groupBy("sentiment").agg(
    avg("review_length").alias("avg_len"),
    min_("review_length").alias("min_len"),
    max_("review_length").alias("max_len")).show()
long_reviews = transformed.filter(col("review_length") > 500)
print(f"=== Reviews longer than 500 characters: {long_reviews.count()} ===")
long_reviews.select("review_length", "sentiment", "review").show(10, truncate=80)

# 6. Save the cleaned + transformed DataFrame as a new CSV
transformed.coalesce(1).write.mode("overwrite").csv(OUTPUT, header=True,
                                                    quote='"', escape='"')
print(f"Saved cleaned & transformed data to {OUTPUT}/")
spark.stop()
