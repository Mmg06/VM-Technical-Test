import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Importing the necessary components from the main pipeline code.
# These include the composite transform ProcessTransactions, as well as helper functions like format_as_csv.
from main import ProcessTransactions, FilterTransactions, format_as_csv

class TestProcessTransactions(unittest.TestCase):

    # Testing the ProcessTransactions composite transform to ensure it filters and sums correctly.
    def test_process_transactions(self):
        # Creating sample input data: this is what would be passed to the pipeline.
        # Each line represents a transaction in CSV format (date, some other fields, and amount).
        input_data = [
            '2011-01-01,1,2,30',  # Valid: amount > 20, year >= 2010
            '2009-01-01,1,2,50',  # Invalid: year < 2010 (should be excluded)
            '2011-01-01,1,2,10',  # Invalid: amount <= 20 (should be excluded)
            '2012-01-01,1,2,25'   # Valid: amount > 20, year >= 2010
        ]

        # Defining what we expect the output to be after running the pipeline.
        # It should include only the valid transactions, grouped and summed by date.
        expected_output = [
            ('2011-01-01', 30),   # Only one valid transaction for this date
            ('2012-01-01', 25)    # Only one valid transaction for this date
        ]

        # Using TestPipeline as a context manager to create a temporary pipeline for testing.
        # This is where we run the actual test.
        with TestPipeline() as p:
            # Feed the input data into the pipeline using the 'Create' transform, which simulates the input source.
            result = (p
                      | 'CreateTestInput' >> beam.Create(input_data)  # Test input data as a PCollection
                      | 'ProcessTransactions' >> ProcessTransactions())  # Applying the composite transform

            # Using 'assert_that' to check if the pipeline output matches the expected output.
            # 'equal_to' ensures the two collections are exactly the same.
            assert_that(result, equal_to(expected_output))

# Standard entry point to run the tests.
if __name__ == '__main__':
    unittest.main()
