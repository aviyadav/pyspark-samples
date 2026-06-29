import os

from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

# Step 0: Ensure output directories exist so writes never fail on a fresh clone.
# Delta will create table/checkpoint paths on demand, but creating the base
# folders up front avoids race conditions and makes the demo self-contained.
for _path in ("delta", "delta/checkpoints"):
    os.makedirs(_path, exist_ok=True)

# Step 1: Environment Setup
builder = (
    SparkSession.builder.appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 2: Create the Source Table with CDF Enabled

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Sample customer data
data = [
    (1, "Alice", "alice@example.com", "Mumbai", "Gold", datetime(2024, 1, 1)),
    (2, "Bob", "bob@example.com", "Delhi", "Silver", datetime(2024, 1, 2)),
    (3, "Charlie", "charlie@example.com", "Bangalore", "Bronze", datetime(2024, 1, 3)),
    (4, "Diana", "diana@example.com", "Pune", "Gold", datetime(2024, 1, 4)),
    (5, "Eve", "eve@example.com", "Chennai", "Silver", datetime(2024, 1, 5)),
]

schema = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("tier", StringType(), True),
        StructField("signup_date", TimestampType(), True),
    ]
)

df = spark.createDataFrame(data, schema)

# Write with CDF enabled — this is the key property!
df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode(
    "overwrite"
).save("delta/customers")

print("Customers table created with CDF enabled.")

# Step 3: Make Some Changes
customers_table = DeltaTable.forPath(spark, "delta/customers")

# --- UPDATE: Alice got promoted to Platinum ---
customers_table.update(condition="customer_id = 1", set={"tier": "'Platinum'"})
print("Updated Alice's tier to Platinum")

# --- MERGE (Upsert): New customer + update Bob's city ---
upsert_data = [
    (
        2,
        "Bob",
        "bob@example.com",
        "Hyderabad",
        "Silver",
        datetime(2024, 1, 2),
    ),  # city changed
    (
        6,
        "Frank",
        "frank@example.com",
        "Kolkata",
        "Bronze",
        datetime(2024, 11, 1),
    ),  # new customer
]
upsert_df = spark.createDataFrame(upsert_data, schema)

customers_table.alias("target").merge(
    upsert_df.alias("source"), "target.customer_id = source.customer_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print("Merged updates — Bob relocated, Frank joined")

# --- DELETE: Eve churned ---
customers_table.delete("customer_id = 5")
print("Deleted Eve's record (churn)")


# Step 4: Read the Change Data Feed
# Read CDF changes starting from version 1 (after initial load)
changes_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("delta/customers")
)

changes_df.select(
    "customer_id",
    "name",
    "tier",
    "city",
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
).orderBy("_commit_version", "customer_id").show(truncate=False)


# Step 5: Build an Incremental Downstream Pipeline
# Simulated state: last time we synced, the table was at version 0
last_processed_version = 0


def get_latest_version(table_path):
    dt = DeltaTable.forPath(spark, table_path)
    return dt.history(1).select("version").collect()[0][0]


def run_incremental_sync(source_path, target_path, from_version):
    """
    Reads only changed rows from source since from_version,
    and applies them to the target table.
    """
    current_version = get_latest_version(source_path)

    if from_version >= current_version:
        print("No new changes to process.")
        return current_version

    print(f"Processing versions {from_version + 1} → {current_version}")

    # Read the change feed
    cdf = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version + 1)
        .load(source_path)
    )

    # We only care about the latest state of each customer
    # Keep only inserts and update_postimage (final state)
    latest_changes = cdf.filter(
        "(_change_type = 'insert') OR (_change_type = 'update_postimage')"
    ).drop("_change_type", "_commit_version", "_commit_timestamp")

    deleted_ids = cdf.filter("_change_type = 'delete'").select("customer_id").distinct()

    # Apply to target — upsert changed rows
    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)

        # Handle deletes
        if deleted_ids.count() > 0:
            deleted_list = [row.customer_id for row in deleted_ids.collect()]
            target.delete(f"customer_id IN ({','.join(map(str, deleted_list))})")
            print(f"Deleted {len(deleted_list)} records")

        # Handle upserts
        if latest_changes.count() > 0:
            target.alias("t").merge(
                latest_changes.alias("s"), "t.customer_id = s.customer_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"Upserted {latest_changes.count()} records")
    else:
        # First run — just write
        latest_changes.write.format("delta").save(target_path)
        print(f"Initial write: {latest_changes.count()} records")

    return current_version


# Run the sync
new_checkpoint = run_incremental_sync(
    source_path="delta/customers",
    target_path="delta/customer_features",
    from_version=last_processed_version,
)

print(f"Sync complete. New checkpoint version: {new_checkpoint}")

# Step 6: CDF With Streaming (Bonus!)
import threading
import time

# Streaming read of changes.
#
# NOTE: startingVersion="latest" would only emit commits that happen *after*
# the stream starts. Since all of our changes above were committed before this
# point, that would leave the sink empty and make the stream look broken.
# Starting from version 1 lets the stream replay the historical CDF events and
# then continue picking up any new commits as they arrive.
streaming_changes = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("delta/customers")
)

# Only process inserts and final update states
processed_stream = streaming_changes.filter(
    "(_change_type = 'insert') OR (_change_type = 'update_postimage')"
)

# Write to a streaming sink (another Delta table, Kafka, etc.)
query = (
    processed_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "delta/checkpoints/customer_sync")
    .trigger(processingTime="5 seconds")
    .start("delta/customer_features_stream")
)

print("Streaming CDF pipeline running")


# Simulate live changes arriving on the source table while the stream runs, so
# we can watch the CDF stream pick them up incrementally.
def generate_live_changes():
    time.sleep(8)
    live_table = DeltaTable.forPath(spark, "delta/customers")
    live_table.update(condition="customer_id = 3", set={"tier": "'Gold'"})
    print("[live] Charlie upgraded to Gold")

    time.sleep(8)
    new_customer = spark.createDataFrame(
        [(7, "Grace", "grace@example.com", "Jaipur", "Silver", datetime(2024, 12, 1))],
        schema,
    )
    new_customer.write.format("delta").mode("append").save("delta/customers")
    print("[live] New customer Grace inserted")


writer = threading.Thread(target=generate_live_changes, daemon=True)
writer.start()

# Let the stream run long enough to process the historical + live changes.
query.awaitTermination(30)
writer.join(timeout=5)

# Stop the query gracefully *before* the process exits.
# awaitTermination(timeout) only stops *waiting* — the query keeps running in a
# background thread. If we let the script exit now, the SparkSession is torn
# down while that thread is mid-microbatch, which raises:
#   [INTERNAL_ERROR] No active or default Spark session found
query.stop()
print("Streaming CDF pipeline stopped")

# Confirm the streaming sink actually received the changes.
print("\nContents of the streaming sink (delta/customer_features_stream):")
spark.read.format("delta").load("delta/customer_features_stream").orderBy(
    "customer_id"
).show(truncate=False)

spark.stop()
