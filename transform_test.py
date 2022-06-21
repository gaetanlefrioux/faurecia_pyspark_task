import pytest
from pyspark.sql import SparkSession
from transform import transform
from pyspark_test import assert_pyspark_df_equal

class TestTransform:

    def get_dataframes_to_compare(self):
        input = "./data/pyspark_task1_source.csv"
        expected_output = "./data/pyspark_task1_transformed.csv"
        self.spark = SparkSession.builder \
            .master("local") \
            .appName("myApp") \
            .getOrCreate()

        input_df = self.spark.read.format("csv").option("header", "true").load(input)
        my_output_df = transform(input, "Combined_Dict")

        expected_df = self.spark.read.format("csv").option("header", "true").load(expected_output)

        return (my_output_df, expected_df)

    def test_dataframes(self):
        my_output_df, expected_df = self.get_dataframes_to_compare()

        assert_pyspark_df_equal(my_output_df, expected_df)
    
