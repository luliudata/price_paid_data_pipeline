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
To run pyspark script locally:
```
Spark 2.3.3
Hadoop 2.7.1
Python 3.6
```
(Note that newer version of Spark are not very stable with Python 3.7+, the versions above work relatively stable.)

Please note that if you run the script in PyCharm, please do the following:

In `Project structure` -> `Add content root` -> add the path of the Spark python executable -> restart PyCharm.

To run Jupyter notebook script (Python 3.8):
```
pandas==1.2.4
numpy==1.20.1
```

Deployment
---------
I have written an AWS Glue driver script, and deployed it to AWS Glue (the deployment to AWS is done manually due to time constraints).

Glue provides a managed ETL service that runs on a serverless Apache Spark environment. 
So it is ideal for our case, it allows us to focus on our ETL job and not worry about configuring and managing the underlying compute resources.

I have manually created a Glue job with the least-privilege policies from AWS console, the Glue script, input and output files are all stored
in S3 buckets. The test execution looks good, and the newline delimited JSON formatted outputs are uploaded to S3 bucket as expected.

Below is a part of the job logs:
```
......
LogType:stdout
Log Upload Time:Wed Aug 18 15:04:40 +0000 2021
LogLength:3155
Log Contents:
2021-08-18 15:04:24,118 - root - INFO - Starting the data pipeline
2021-08-18 15:04:24,118 - root - INFO - Reading csv files from s3 path s3://ppd-paid-data-pipeline-input
2021-08-18 15:04:25,763 - root - INFO - Constructing a new identifier 'property_id'
2021-08-18 15:04:25,948 - root - INFO - Grouping transaction records based on 'property_id'
2021-08-18 15:04:26,053 - root - INFO - Writing json files to s3 path s3://ppd-paid-data-pipeline-output/output
End of LogType:stdout
```
Below is screenshots for the Glue job:
 
![image](https://user-images.githubusercontent.com/44141273/129926325-f843bf7a-2a44-47b5-b50e-a2b32f004b0d.png)

Two screenshots for the output bucket:
(Note that the number pf partitions can be configured in the code.)
![image](https://user-images.githubusercontent.com/44141273/129926575-cbe2d4b3-793a-47e8-b10d-d2c749a18173.png)
![image](https://user-images.githubusercontent.com/44141273/129926718-356f8a6d-4a4c-4967-aaeb-66055ea69000.png)

An example output file downloaded from output bucket can be found under folder 'example_output_from_aws'.

Future Work
---------
Due to the time constraints, the deployment for data pipeline has not been automated.
All the AWS resources used here can be created and managed via Terraform (for testing purpose, those resources are created manually via AWS console).
