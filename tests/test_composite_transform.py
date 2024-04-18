import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from src.task2 import ProcessTransactions  # Import your composite transform


class TestProcessTransactions(unittest.TestCase):
    def test_process_transactions(self):
        with TestPipeline() as p:
            # Define the input data
            input_data = [
                'timestamp,origin,destination,transaction_amount',
                '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,29.95',
                # Add more input data as needed
            ]

            # Apply the ProcessTransactions transform to the input data
            output = (
                    p
                    | beam.Create(input_data)
                    | 'Process Transactions' >> ProcessTransactions()
            )

            # Define the expected output
            expected_output = [
                '2017-01-01,29.95',
                # Add more expected output as needed
            ]

            # Assert that the actual output matches the expected output
            assert_that(output, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
