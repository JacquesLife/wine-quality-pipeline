# Databricks notebook source

# COMMAND ----------

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, avg, round, when

spark = DatabricksSession.builder.getOrCreate()
dbutils = spark.extensions.dbutils

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/wine-quality/"))

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/wine-quality/winequality-red.csv", header=True, inferSchema=True, sep=";")

display(df)
df.printSchema()
print(f"Row count: {df.count()}")

# COMMAND ----------

result = (
    df
    .groupBy("quality")
    .agg(
        round(avg("alcohol"), 2).alias("avg_alcohol"),
        round(avg("pH"), 2).alias("avg_pH"),
        round(avg("fixed acidity"), 2).alias("avg_acidity")
    )
    .orderBy("quality")
)

display(result)

# COMMAND ----------

df_enriched = df.withColumn(
    "quality_tier",
    when(col("quality") <= 4, "low")
    .when(col("quality") <= 6, "medium")
    .otherwise("high")
)

display(df_enriched.select("quality", "quality_tier", "alcohol", "pH").limit(20))

# COMMAND ----------

cols_renamed = [c.replace(" ", "_") for c in df_enriched.columns]
df_enriched_renamed = df_enriched.toDF(*cols_renamed)

df_enriched_renamed.write.format("delta").mode("overwrite").saveAsTable("wine_enriched")

display(spark.sql("SELECT * FROM wine_enriched LIMIT 5"))
# updated
