from pyspark.sql import SparkSession, Window, functions as F


def example_1(session, dataframe):
    """Calculate running total sales partitioned by store and product."""
    window_spec = (
        Window
        .partitionBy("store_code", "product_code")
        .orderBy("sales_date")
    )
    
    data_windowed = dataframe.withColumn(
        "total_sales",
        F.sum("sales_qty").over(window_spec)
    )
    
    data_windowed.filter(
        F.col("store_code") == "B1"
    ).select(
        "store_code",
        "product_code",
        "sales_date",
        "sales_qty",
        "total_sales"
    ).show(50)

    return window_spec


def example_2(session, dataframe, window):
    """Calculate maximum price per store and product."""
    data2 = dataframe.withColumn(
        "max_price",
        F.max("price").over(window)
    )

    data2.filter(
        F.col("store_code") == "B1"
    ).select(
        "store_code",
        "product_code",
        "sales_date",
        "price",
        "max_price"
    ).show(15)


def example_3(session, dataframe, window):
    """Calculate lag and lead values for sales quantity."""
    data3 = dataframe.withColumn(
        "prev_day_sales_lag",
        F.lag("sales_qty", 1).over(window)
    ).withColumn(
        "prev_day_sales_lead",
        F.lead("sales_qty", -1).over(window)
    )

    data3.filter(
        (F.col("store_code") == "A1") & (F.col("product_code") == "95955")
    ).select(
        "sales_date",
        "sales_qty",
        "prev_day_sales_lag",
        "prev_day_sales_lead"
    ).show(15)


def example_4(session, dataframe):
    """Calculate rolling 3-day average for sales quantity."""
    window = (
        Window
        .partitionBy("store_code", "product_code")
        .orderBy("sales_date")
        .rowsBetween(-3, -1)
    )

    data4 = dataframe.withColumn(
        "last_3_day_avg",
        F.mean("sales_qty").over(window)
    )

    data4.filter(
        (F.col("store_code") == "A1") & (F.col("product_code") == "95955")
    ).select(
        "sales_date",
        "sales_qty",
        "last_3_day_avg"
    ).show()


def example_5(session, dataframe):
    """Calculate cumulative mean for sales quantity."""
    window = (
        Window
        .partitionBy("store_code", "product_code")
        .orderBy("sales_date")
        .rowsBetween(Window.unboundedPreceding, -1)
        # Alternative: .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    data5 = dataframe.withColumn(
        "cumulative_mean",
        F.mean("sales_qty").over(window)
    )

    data5.filter(
        (F.col("store_code") == "A1") & (F.col("product_code") == "95955")
    ).select(
        "sales_date",
        "sales_qty",
        "cumulative_mean"
    ).show()


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    
    spark_df = spark_session.read.csv(
        "dataset/sample_sales_pyspark.csv",
        header=True,
        inferSchema=True
    )
    
    # Run examples
    window = example_1(spark_session, spark_df)
    example_2(spark_session, spark_df, window)
    example_3(spark_session, spark_df, window)
    example_4(spark_session, spark_df)
    example_5(spark_session, spark_df)
