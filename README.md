# txn-total-beam-vm

 Sample apache beam pipeline to read a csv file from a gcs bucket filter the data on certain conditions and generate a summarised report.


# Required python modules

execute below command to install apache-beam and apache-beam[gcp] modules.

 1. pip install apache-beam 
 2. pip install apache-beam[gcp]

## Pipeline detials

Whole logic to read the csv file and generate a summary report is done through the file txn_total.py.
This file is made up of below classes/functions:
Class: **SplitTransaction** to split the data from the source csv file
Function: **txn_cost_filter** to filter the transaction data based on the transaction amount.
Function: **txn_period_filter** to filter the transaction data based on the transaction date
Class: **GetTotal** to get the aggregated sum of transaction amount based on the transaction date
Function: **format_result** to format the output file

## Executing the pipeline

 - In Windows python -m txn_total 
 - In Linux python3 -m txn_total

## Unit test case
Unit test case is written using beam tooling ***assert_that***
The unit test case reads a known input file located at Tests/source_files/sample_file.csv and generates actual output file into Tests/actual_output folder. Then this output is compared with the expected output using assert_that.

## Executing unit test case
- In Windows python -m test_txn_total
- In Linux python3 -m test_txn_total