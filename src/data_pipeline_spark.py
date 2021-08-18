from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as f, SparkSession

# initiate spark session locally
spark = SparkSession.builder.appName('local').getOrCreate()


def data_pipeline(input_path, output_path):
    # read data with schema
    schema = StructType() \
        .add("transaction_unique_id", StringType(), True) \
        .add("price", IntegerType(), True) \
        .add("date_of_transfer", StringType(), True) \
        .add("postcode", StringType(), True) \
        .add("property_type", StringType(), True) \
        .add("old_new", StringType(), True) \
        .add("duration", StringType(), True) \
        .add("PAON", StringType(), True) \
        .add("SAON", StringType(), True) \
        .add("street", StringType(), True) \
        .add("locality", StringType(), True) \
        .add("town_city", StringType(), True) \
        .add("district", StringType(), True) \
        .add("country", StringType(), True) \
        .add("PPD_category_type", StringType(), True) \
        .add("record_status", StringType(), True)

    df_with_schema = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load(input_path) # input can be either a local csv file or an S3 path

    # input_df = spark.read.option("header", True).csv(input_s3_path)

    # construct new identifier 'property_id' for each property
    df = df_with_schema.withColumn('property_id',
                                   f.hash('postcode', 'property_type', 'old_new', 'PAON', 'SAON'))

    # move 'property_id' to the front column
    # and remove the last column since it's the same for each transaction in the testing file
    df = df.select([df.columns[-1]] + df.columns[:-2])

    # group data
    grouped_df = df.select(f.col("property_id"),
                           f.struct(f.col("transaction_unique_id"),
                                    f.col("price"),
                                    f.col("date_of_transfer"),
                                    f.col("postcode"),
                                    f.col("property_type"),
                                    f.col("old_new"),
                                    f.col("duration"),
                                    f.col("PAON"),
                                    f.col("SAON"),
                                    f.col("street"),
                                    f.col("locality"),
                                    f.col("town_city"),
                                    f.col("district"),
                                    f.col("country"),
                                    f.col("PPD_category_type")
                                    ).alias("transactions")) \
        .groupby("property_id") \
        .agg(f.collect_list("transactions").alias("transactions"))

    # write to json file locally
    # downside for this is we can't overwrite existing file with the same file name
    grouped_df.coalesce(1).write.format('json').save(output_path)


if __name__ == "__main__":
    data_pipeline('pp-2021-part1.csv', 'spark-test-output')
