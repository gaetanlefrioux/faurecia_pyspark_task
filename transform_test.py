import pytest
from pyspark.sql import SparkSession
from transform import transform
from pyspark_test import assert_pyspark_df_equal

"""
    Class to test that the transformation is working as expected
"""
class TestTransform:
    
    """
        Setup the test by applying the transformation and loading the example output
        arguments:
            
        return:
            - Spark dataframe created by the transformation
            - Spark dataframe expected as result of the transformation
    """
    def get_dataframes_to_compare(self):
        input = "./data/pyspark_task1_source.csv"
        expectedOutput = "./data/pyspark_task1_transformed.csv"
        self.spark = SparkSession.builder \
            .master("local") \
            .appName("myApp") \
            .getOrCreate()

        inputDf = self.spark.read.format("csv").option("header", "true").load(input)
        myOutputDf = transform(input, "Combined_Dict")

        expectedDf = self.spark.read.format("csv").option("header", "true").load(expectedOutput)

        return (myOutputDf, expectedDf)

    """
        Test that the dataframe given by the transformation is the same as the one expected
    """
    def test_dataframes(self):
        myOutputDf, expectedDf = self.get_dataframes_to_compare()

        assert_pyspark_df_equal(myOutputDf, expectedDf)
    
