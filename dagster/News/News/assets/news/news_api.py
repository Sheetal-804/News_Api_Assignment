from typing import Any
from dagster import asset, OpExecutionContext, ResourceParam
from pyspark.sql import SparkSession  # type: ignore
import subprocess
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import (
    regexp_replace,
    to_date,
    expr,
)

KEY_PREFIX = ["news_api", "bbc"]

mongo_connector_jar = (
    "C:/spark/spark-3.5.1-bin-hadoop3/jars/mongo-spark-connector_2.12-3.0.1.jar"
)
# Initialize the Spark session outside the transformation logic
spark = (
    SparkSession.builder.appName("MongoDBIntegration")
    .config(
        "spark.mongodb.input.uri",
        "mongodb://localhost:27017/scrapy_db.scraped_articles",
    )
    .config(
        "spark.mongodb.output.uri",
        "mongodb://localhost:27017/scrapy_db.scraped_articles",
    )
    .config("spark.jars", mongo_connector_jar)
    .getOrCreate()
)


@asset(
    key_prefix=KEY_PREFIX,
    compute_kind="Scrapy",
)
def scrapy_news_spider(context: OpExecutionContext):
    # Command to run the Scrapy spider
    command = ["scrapy", "crawl", "news_spider"]

    # Use subprocess to run the Scrapy spider from the specific directory
    result = subprocess.run(
        command, cwd="../../scrapy_spider/News/News", capture_output=True, text=True
    )

    # Check for errors
    if result.returncode != 0:
        raise Exception(f"Scrapy spider failed: {result.stderr}")

    context.log.info(f"Scrapy spider finished with output: {result.stdout}")
    return result.stdout


@asset(
    key_prefix=KEY_PREFIX,
    deps=["scrapy_news_spider"],
    compute_kind="Spark",
)
def mongo_data():
    # Spark logic should be executed here, not inside parallelized code
    df = spark.read.format("mongo").load()
    df.show()
    return df


@asset(
    key_prefix=KEY_PREFIX,
    deps=["mongo_data"],
    compute_kind="Spark",
)
def news_cleaned_data(
    df: DataFrame, pyspark_step_launcher: ResourceParam[Any]
) -> DataFrame:
    df = (
        df.withColumn("title", regexp_replace("title", r"\s+", " "))
        .withColumn("description", regexp_replace("description", r"\s+", " "))
        .withColumn(
            "content",
            regexp_replace("content", r"\s+", " "),
        )
    )

    df = df.withColumn("publishedAt", to_date(expr("substring(publishedAt, 1, 10)")))

    df = df.select(
        "author",
        "title",
        "description",
        "publishedAt",
        "content",
        "url",
    )

    return df


spark = (
    SparkSession.builder.appName("DeltaInsertWithParquet")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)


@asset(
    key_prefix=KEY_PREFIX,
    deps=["news_cleaned_data"],
    compute_kind="Spark",
)
def insert_data_into_delta(
    df: DataFrame, pyspark_step_launcher: ResourceParam[Any]
) -> DataFrame:
    # Define the path for the Delta Table (which stores data in Parquet format internally)
    delta_table_path = "/path/to/delta/table"

    # Write the cleaned data into the Delta Table (which uses Parquet files under the hood)
    df.write.format("delta").mode("append").save(delta_table_path)

    return df
