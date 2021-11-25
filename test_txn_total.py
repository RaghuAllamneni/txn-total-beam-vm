import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import Map
import txn_total
import csv
from datetime import datetime

OUTPUT_TRANSACTION_DATA = [['date', ' total_amount'], ['2010-01-01', ' 100.4']]

dt = datetime.now().strftime("%Y%m%d_%H%M%S")
txn_total.generate_txn_report("Tests/source_files/sample_file.csv","Tests/actual_output/results",dt)


with open('Tests/actual_output/results-00000-of-00001_'+dt+'.csv', newline='') as f:
    Reader = csv.reader(f)
    Data = list(Reader)
#print(data)
#print(OUTPUT_TRANSACTION_DATA)
with TestPipeline() as Expected_Pipline:
        Expected = (Expected_Pipline | beam.Create(OUTPUT_TRANSACTION_DATA)
                   ) 
with TestPipeline() as Actual_Pipline:
        Actual = (Actual_Pipline | beam.Create(Data))
        
assert_that(Actual, equal_to(Expected) )