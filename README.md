# price_paid_data_pipeline

This repo is a simple data pipeline using Pyspark. 
It takes a CSV file, construct a new identifier `property_id` and group transactions by this new identifier.
Then output a newline delimited JSON formatted file.

Data Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

Data Definition Information: https://www.gov.uk/guidance/about-the-price-paid-data#download-option

The input dataset used by this repo is the Part 1 yearly file for 2021.

The output file `part-00000-3bbeb519-143c-47a9-8aa1-46e6407fd6dc-c000.json` can be found under `spark-test-output` folder.


Prerequisites
---------
```
Spark 2.3.3
Hadoop 2.7.1
Python 3.6
```
(Note that newer version of Spark are not very stable with Python 3.7+, the versions above work relatively stable.)

Please note that if you run the script in PyCharm, please do the following:

In `Project structure` -> `Add content root` -> add the path of the Spark python executable -> restart PyCharm.

