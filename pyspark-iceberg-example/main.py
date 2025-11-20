from pyspark.sql import SparkSession
from pyspark.sql.functions import col, parse_json
import sys
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
SUBMIT_ARGS = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 "
    "--repositories https://repo1.maven.org/maven2 "
    "pyspark-shell"
)

os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ["PYSPARK_PYTHON"] = sys.executable

warehouse_path = "/home/avinash/home/codebase/python-base/pyspark/pyspark-iceberg-example/warehouse"

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.dev.type", "hadoop")
    .config("spark.sql.catalog.dev.warehouse", warehouse_path)
    .config("spark.sql.catalog.dev.format-version", "3")  # Critical: Iceberg V3 for VARIANT
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .getOrCreate()
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS dev.default.test_table (
          id BIGINT,
          properties VARIANT
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '3'
    )
""")

# Complex JSON data to be inserted
json_string = '''{
    "user": {
        "name": "Alice",
        "age": 30,
        "hobbies": ["reading", "swimming", "hiking"],
        "address": {
            "city": "Wonderland",
            "zip": "12345"
        }
    },
    "status": "active",
    "tags": ["admin", "user", "editor"]
}'''

# Create DataFrame with columns id and JSON string
df = spark.createDataFrame([(1, json_string)], ["id", "json_str"])

# Convert JSON string column to VARIANT type column using parse_json
df_variant = df.withColumn("properties", parse_json(col("json_str"))).drop("json_str")

# Insert into Iceberg table
df_variant.writeTo("dev.default.test_table").append()

spark.sql("""
    SELECT
        id,
        variant_get(properties, '$.user.name', 'string') AS user_name,
        variant_get(properties, '$.user.age', 'int') AS user_age,
        variant_get(properties, '$.user.address.city', 'string') AS city,
        variant_get(properties, '$.status', 'string') AS status
    FROM dev.default.test_table
""").show(truncate=False)

# Query array elements
spark.sql("""
    SELECT
        id,
        variant_get(properties, '$.user.hobbies[0]', 'string') AS first_hobby,
        variant_get(properties, '$.tags[1]', 'string') AS second_tag
    FROM dev.default.test_table
""").show()

# Check for existence of fields
spark.sql("""
    SELECT
        id,
        CASE 
            WHEN variant_get(properties, '$.user.email', 'string') IS NOT NULL 
            THEN variant_get(properties, '$.user.email', 'string')
            ELSE 'No email provided'
        END AS email_status
    FROM dev.default.test_table
""").show()

