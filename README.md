# faurecia_pyspark_task

Example of pyspark code to unpack a JSON field from a CSV file.

## Usage

The pyspark transformation is wrapped in a python script. This script can be used as follow:


usage: transform.py [-h] -i INPUT -o OUTPUT

optional arguments:
- -h, --help            show this help message and exit
- -i INPUT, --input INPUT
                        Path to the input file to be transformed
- -o OUTPUT, --output OUTPUT
                        Path to the output directory where to write the transformed data

## Tests

A test is implemented using the pytest library.
This test is making sure that the dataframe resulting from the transformation is the same as the one expected.

Tests can be run using the `pytest` command

### Test results

![image](https://user-images.githubusercontent.com/15312613/174862259-06f73797-bd7b-4665-bdf3-b74ad33df1f9.png)


## Remarks and limitations

With the choosen implementation, the transform script is inferring the columns provided in the JSON field directly by checking the provided data.
This makes the transformation more flexible allowing to handle JSON value with missing keys.
The transform script will only unpack the first level of the JSON field. 

In some use cases it might be more suitable to specify directly the Schema and make the transformation less flexible to identify malformed data at an early stage.

For testing purposes the transform script is using Spark with local mode
