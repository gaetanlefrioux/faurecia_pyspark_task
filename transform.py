from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, json_tuple, explode
import argparse

"""
    Transform the content of the input CSV file by unpacking the given JSON field
    arguments:
        - inputFile [str] : path to the input file
        - jsonField [str] : json field to be unpacked
    return:
        - Spark Dataframe with the transformation applied
"""
def transform(inputFile, jsonField):
    spark = SparkSession.builder \
            .master("local") \
            .appName("unpack") \
            .getOrCreate()

    df = spark.read.format("csv").option("header", "true").load(inputFile)

    # Inferring name of nested columns from the data
    nestedCols = df.select(
                    from_json(jsonField, "MAP<String, String>").alias("jsonData")) \
                .select(explode("jsonData")) \
                .select("key").distinct().collect()

    nestedColList = [r["key"] for r in nestedCols]
    keepCols = set(df.columns) - set([jsonField])
    allCols = list(keepCols) + nestedColList

    # Unpack Combined_Dict columns
    unpackedDf = df.select(*keepCols, json_tuple(jsonField, *nestedColList)).toDF(*allCols)

    # Clean Empty Values
    cleanedDf = unpackedDf.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in unpackedDf.columns])

    return cleanedDf

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Path to the input file to be transformed", required=True)
    parser.add_argument("-o", "--output", help="Path to the output directory where to write the transformed data", required=True)
    jsonField = "Combined_Dict"

    args = parser.parse_args()
    transformed_df = transform(args.input, jsonField)

    # Writing result
    transformed_df.write.option("header","true").csv(args.output)

