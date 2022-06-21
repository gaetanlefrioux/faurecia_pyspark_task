from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, json_tuple, explode
import argparse

def transform(inputFile, nestedField):
    spark = SparkSession.builder \
            .master("local") \
            .appName("myApp") \
            .getOrCreate()

    df = spark.read.format("csv").option("header", "true").load(inputFile)

    # Inferring name of nested columns from the data
    nested_cols = df.select(
                    from_json(nestedField, "MAP<String, String>").alias("jsonData")) \
                .select(explode("jsonData")) \
                .select("key").distinct().collect()

    nested_col_list = [r["key"] for r in nested_cols]
    keep_cols = set(df.columns) - set([nestedField])
    all_cols = list(keep_cols) + nested_col_list

    # Unpack Combined_Dict columns
    unpacked_df = df.select(*keep_cols, json_tuple(nestedField, *nested_col_list)).toDF(*all_cols)

    # Clean Empty Values
    cleaned_df = unpacked_df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in unpacked_df.columns])

    return cleaned_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Path to the input file to be transformed", required=True)
    parser.add_argument("-o", "--output", help="Path to the output directory where to write the transformed data", required=True)
    nestedField = "Combined_Dict"

    args = parser.parse_args()
    transformed_df = transform(args.input, nestedField)

    # Writing result
    transformed_df.write.option("header","true").csv(args.output)

