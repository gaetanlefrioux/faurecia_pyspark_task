from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, json_tuple, explode



inputFile = "./data/pyspark_task1_source.csv"
outputFile = "./data/pyspark_task1_mytransformed.csv"
nestedField = "Combined_Dict"


spark = SparkSession.builder \
        .master("local") \
        .appName("myApp") \
        .getOrCreate()

df = spark.read.format("csv").option("header", "true").load(inputFile)

# Inferring name of nested columns from the data
nested_cols = df.select(
                from_json("Combined_Dict", "MAP<String, String>").alias("jsonData")) \
              .select(explode("jsonData")) \
              .select("key").distinct().collect()

nested_col_list = [r["key"] for r in nested_cols]
keep_cols = set(df.columns) - set(["Combined_Dict"])
all_cols = list(keep_cols) + nested_col_list

# Unpack Combined_Dict columns
unpacked_df = df.select(*keep_cols, json_tuple("Combined_Dict", *nested_col_list)).toDF(*all_cols)

# Clean Empty Values
cleaned_df = unpacked_df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in unpacked_df.columns])

# Writing result
cleaned_df.write.option("header","true").csv(outputFile)

