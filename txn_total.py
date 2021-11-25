from typing import overload
import apache_beam as beam
from datetime import datetime

# Class to split the input file and return the data
class SplitTransaction(beam.DoFn):
    def process(self, element):
        timestamp, origin, destination, transaction_amount = element.split(',')
        return [{"timestamp": datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %Z').date(), "origin": origin,"destination": destination,"transaction_amount": float(transaction_amount)}]

# Function to exclude the transaction less than certain amount (20 in this case)
def txn_cost_filter(transactions):
  return transactions[1] > 20.00

# function to exclude transactions made before certain year (2010 in this case)
def txn_period_filter(transactions):
  return transactions[0].year >= 2010

# Class to return sum of txn amount per each txn date
class GetTotal(beam.DoFn):
    def process(self, element):
        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]

# function to format the result
def format_result(trans_data):
    return f"{trans_data[0]}, {trans_data[1]}"

# Main function to p
def generate_txn_report(source_path: str, target_path: str, time_stamp: str):
  with beam.Pipeline() as build_report:
    data_from_source = (build_report
                        | 'Read Source csv file' >> beam.io.ReadFromText(source_path, skip_header_lines=True)
                        | 'Split the data read from source file' >> beam.ParDo(SplitTransaction())
                        | 'Select only txn timestamp and txn amount' >> beam.Map(lambda record: (record['timestamp'], record['transaction_amount']))
                        | 'Exclude the unwanted transactions based on the txn amount' >> beam.Filter(txn_cost_filter)
                        | 'Exclude the unwanted transactions based on year of txn' >> beam.Filter(txn_period_filter)
                        | 'Cast txn date as str to be used as GroupByKey' >> beam.Map(lambda record: (str(record[0]), record[1]))
                        | 'GroupBy txn date' >> beam.GroupByKey()
                        | 'Get the txn total by txn date' >> beam.ParDo(GetTotal())
                        | 'Format the results' >> beam.Map(format_result)
                        |'Write the result into a csv file with appropriate headers' >> beam.io.WriteToText( file_path_prefix=target_path,
                                              file_name_suffix='_'+time_stamp+'.csv',
                                              header='date, total_amount')
    )
                        
                        
if __name__ == "__main__":
  dt = datetime.now().strftime("%Y%m%d_%H%M%S")
  generate_txn_report("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv", "output/results", dt)