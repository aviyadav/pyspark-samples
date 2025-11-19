import pyspark
from pyspark.sql import SparkSession, DataFrame
from typing import List
import pyspark.sql.functions as F
import pyspark.sql.types as T



def play_with_df(df: DataFrame) -> None:
    """Perform various DataFrame operations on the COVID-19 dataset."""
    df.printSchema()

    date_col_df = df.select(F.to_date(df.date).alias("date"))
    print(date_col_df)

    df.describe().show()

    df.filter(F.col("location") == "United States") \
        .orderBy(F.col("date").desc()) \
        .show(5)

    df.groupBy("location") \
        .sum("new_cases") \
        .orderBy(F.col("sum(new_cases)").desc()) \
        .show(truncate=False)


def df_sql(df: DataFrame, spark: SparkSession) -> None:
    df.createOrReplaceTempView("covid_data")

    df_view = spark.sql("""
        SELECT * FROM covid_data
    """)
    df_view.printSchema()
    df_view.show()

    groupDF = spark.sql("SELECT location, count(*) from covid_data group by location")
    groupDF.show()

def california_housing_train(spark: SparkSession):
    """Load the California housing training dataset."""
    df = spark.read.csv("dataset/california_housing_train.csv", header=True, inferSchema=True)
    
    df.printSchema()

    df.show(5)

    print(f"Total rows: {df.count()}")

    df.select("housingMedianAge","totalRooms").show(5)

    df.describe().show()

    df.select('totalRooms').distinct().show()
    test = df.groupBy('totalRooms').agg(F.sum('housingMedianAge'))

    test.toPandas()

    df.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df.columns]).show()
    
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CovidDataAnalysis") \
        .getOrCreate()
    
    df = spark.read.csv("dataset/owid-covid-data.csv", header=True, inferSchema=True)

    play_with_df(df)
    df_sql(df, spark)

    california_housing_train(spark)