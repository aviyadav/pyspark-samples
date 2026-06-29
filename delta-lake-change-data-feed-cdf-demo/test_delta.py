from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# 1. Define your Spark builder configuration
builder = (
    SparkSession.builder.appName("LocalLakeTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

# 2. Configure Spark to download Delta Lake JARs automatically
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 3. Create a test DataFrame and write it as a Delta table locally
data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "id"])
df.write.format("delta").mode("overwrite").save("./tmp/delta-table")

# 4. Read the Delta table and verify the data
df_read = spark.read.format("delta").load("./tmp/delta-table")
df_read.show()
