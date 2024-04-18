import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from src.task1 import ParseTransaction, SumTransactions


class MyPipelineTest(unittest.TestCase):
    def test_parse_transaction(self):
        with TestPipeline() as p:
            # Test input data
            input_data = ['2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']
            # Expected output data after parsing
            expected_output = [('2018-02-27', 129.12)]

            # Apply the ParseTransaction transform to the input data
            parsed_transactions = p | beam.Create(input_data) | beam.ParDo(ParseTransaction())

            # Assert that the parsed transactions match the expected output
            assert_that(parsed_transactions, equal_to(expected_output))

    def test_parse_transaction_invalid_data(self):
        dofn = ParseTransaction()

        element = '202-01-03,25\n'  # Invalid date format
        with self.assertRaises(ValueError) as context:  # Assert that value error is raised
            list(dofn.process(element))
        self.assertTrue('Error parsing row' in str(context.exception))

    def test_sum_transactions(self):
        with TestPipeline() as p:
            # Test input data
            input_data = [('2023-01-01', [30.0, 25.0]), ('2023-01-02', [15.0])]
            # Expected output data after summing
            expected_output = ['2023-01-01,55.0',
                               '2023-01-02,15.0']

            # Apply the SumTransactions transform to the input data
            summed_transactions = p | beam.Create(input_data) | beam.ParDo(SumTransactions())

            # Assert that the summed transactions match the expected output
            assert_that(summed_transactions, equal_to(expected_output))

    def test_process_empty_input(self):
        """Test process method with empty input."""
        with TestPipeline() as p:
            # Test input data
            input_data = [('2023-01-01', [])]
            # Expected output data after summing
            expected_output = ['2023-01-01,0']

            # Apply the SumTransactions transform to the input data
            summed_transactions = p | beam.Create(input_data) | beam.ParDo(SumTransactions())

            # Assert that the summed transactions match the expected output
            assert_that(summed_transactions, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
