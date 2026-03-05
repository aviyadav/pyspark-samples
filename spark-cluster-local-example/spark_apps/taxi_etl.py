import pyspark.sql.functions as F
from pyspark.sql import SparkSession

RAW_PATH = "/opt/spark/data/yellow_tripdata_2024-01.parquet"
ZONE_LOOKUP_PATH = "/opt/spark/data/taxi_zone_lookup.csv"

CURATED_OUT = "/opt/spark/data/results/curated_taxi_trips"
DAILY_KPIS_OUT = "/opt/spark/data/results/daily_zone_kpis"


def main():
    spark = (
        SparkSession.builder.appName(
            "NYC TLC Taxi ETL (Parquet -> Curated + KPIs)")
        .getOrCreate()
    )

    # -------------------------
    # Extract
    # -------------------------
    trips = spark.read.parquet(RAW_PATH)

    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(ZONE_LOOKUP_PATH)
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("Borough").alias("Borough"),
            F.col("Zone").alias("Zone"),
            F.col("service_zone").alias("service_zone"),
        )
    )

    # -------------------------
    # Transform
    # -------------------------
    # Yellow taxi typically uses these timestamp columns:
    # tpep_pickup_datetime, tpep_dropoff_datetime
    # (If TLC changes schemas slightly across years, these may still be present; if not, you'll see an error quickly.)
    cleaned = (
        trips
        .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("PULocationID", F.col("PULocationID").cast("int"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
        .withColumn(
            "trip_duration_sec",
            (F.col("dropoff_ts").cast("long") -
             F.col("pickup_ts").cast("long")).cast("long"),
        )
        .withColumn(
            "trip_duration_min",
            (F.col("trip_duration_sec") / F.lit(60.0)).cast("double"),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_duration_sec") > 0,
                   (F.col("trip_distance") / (F.col("trip_duration_sec") / F.lit(3600.0))))
            .otherwise(F.lit(None).cast("double")),
        )
        # Basic quality filters (typical ETL hygiene)
        .where(F.col("pickup_ts").isNotNull())
        .where(F.col("dropoff_ts").isNotNull())
        .where(F.col("trip_duration_sec") > 0)
        .where((F.col("trip_distance") >= 0) & (F.col("trip_distance") <= 200))
        .where((F.col("total_amount") >= 0) & (F.col("total_amount") <= 2000))
        .where(F.col("PULocationID").isNotNull())
        .where(F.col("DOLocationID").isNotNull())
    )

    # Enrich pickup zone
    pu_zones = zones.select(
        F.col("LocationID").alias("PU_LocationID"),
        F.col("Borough").alias("PU_Borough"),
        F.col("Zone").alias("PU_Zone"),
        F.col("service_zone").alias("PU_service_zone"),
    )

    enriched = (
        cleaned
        .join(pu_zones, cleaned.PULocationID == pu_zones.PU_LocationID, "left")
        .drop("PU_LocationID")
    )

    # -------------------------
    # Load (curated dataset)
    # -------------------------
    (
        enriched
        .select(
            "pickup_ts",
            "dropoff_ts",
            "pickup_date",
            "PULocationID",
            "DOLocationID",
            "PU_Borough",
            "PU_Zone",
            "trip_distance",
            "trip_duration_min",
            "avg_speed_mph",
            "fare_amount",
            "total_amount",
            "payment_type",
            "passenger_count",
        )
        .write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(CURATED_OUT)
    )

    # -------------------------
    # Load (daily KPIs per pickup zone)
    # -------------------------
    daily_kpis = (
        enriched
        .groupBy("pickup_date", "PULocationID", "PU_Borough", "PU_Zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
        )
        .orderBy(F.col("pickup_date").asc(), F.col("trip_count").desc())
    )

    daily_kpis.show(20, truncate=False)

    (
        daily_kpis
        .write.mode("overwrite")
        .parquet(DAILY_KPIS_OUT)
    )

    spark.stop()


if __name__ == "__main__":
    main()
