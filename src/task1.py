import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from datetime import datetime


class ParseTransaction(beam.DoFn):
    """A DoFn for parsing transaction data from CSV."""

    def process(self, element):
        # Split the CSV row
        rows = element.split('\n')
        headers = next(csv.reader([rows[0]]))  # Extract headers

        # Check if the first row contains headers
        if 'timestamp' in headers and 'transaction_amount' in headers:
            rows = rows[1:]  # Skip the first row if it contains headers
        for row in rows:
            try:
                # Parse the row
                timestamp_str, origin, destination, amount_str = row.split(',')
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S %Z')
                amount = float(amount_str)
                if timestamp.year >= 2010 and amount > 20:
                    yield (timestamp.strftime('%Y-%m-%d'), amount)

            except ValueError as e:
                # If parsing fails, log an error and continue processing other rows
                raise ValueError(f"Error parsing row: {row}. {e}")


class SumTransactions(beam.DoFn):
    """A DoFn for summing transaction amounts by date."""

    def process(self, element):
        date, amounts = element
        total_amount = sum(amounts)
        yield f"{date},{total_amount}"


def run():
    """Runs the Apache Beam pipeline to process transaction data."""
    options = PipelineOptions()
    # Define the output directory
    output_dir = 'output/'
    with beam.Pipeline(options=options) as p:
        transactions = (p | 'Read Transactions' >> beam.io.ReadFromText(
            'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
                        | 'Parse Transactions' >> beam.ParDo(ParseTransaction())
                        | 'Group by Dates' >> beam.GroupByKey()
                        | 'Sum Transactions' >> beam.ParDo(SumTransactions()))

        transactions | 'Write to JSON' >> beam.io.WriteToText(output_dir, file_name_suffix='.output1.csv',
                                                              header='date,total_amount')


if __name__ == '__main__':
    run()
