import apache_beam as beam

# Composite transform to process transactions: filter and sum them by date.
class ProcessTransactions(beam.PTransform):
    def expand(self, pcoll):
        # Step 1: Filter transactions using the FilterTransactions DoFn.
        # We are only interested in transactions where the amount is greater than 20 and the date is in 2010 or later.
        filtered_transactions = pcoll | 'FilterTransactions' >> beam.ParDo(FilterTransactions())
        
        # Step 2: Sum the transaction amounts by date. Using CombinePerKey to group by date and sum up amounts.
        summed_transactions = filtered_transactions | 'SumAmounts' >> beam.CombinePerKey(sum)
        
        return summed_transactions  # Returning the transformed PCollection for further processing.

# Define a custom DoFn to filter out transactions that don't meet our conditions.
class FilterTransactions(beam.DoFn):
    def process(self, row):
        # Splitting each row (CSV format) to extract date and amount fields.
        transaction = row.split(',')
        date, amount = transaction[0], float(transaction[3])  # Assuming the amount is in the 4th column.
        year = int(date.split('-')[0])  # Extract the year from the date string.

        # We're only interested in transactions after 2010 and where the amount is greater than 20.
        if amount > 20 and year >= 2010:
            # Yielding the transaction as a tuple (date, amount). This will be processed further by CombinePerKey.
            yield (date, amount)

# Utility function to format each row as a CSV string. 
# This is helpful when we're ready to write out the processed data.
def format_as_csv(row):
    date, total_amount = row
    # Formatting each record as "date,total_amount" with two decimal places for the amount.
    return f'{date},{total_amount:.2f}'

# Function to add a header row to the output. We want the output CSV to have "date,total_amount" as the first line.
def add_header(pcoll):
    # Creating a PCollection that consists of just the header string.
    header = ['date,total_amount']  # This will be the first line in our output.
    
    # Adding the header to the rest of the data using Flatten.
    header_pcoll = pcoll.pipeline | 'CreateHeader' >> beam.Create(header)  # Creating a PCollection for the header.
    return (header_pcoll, pcoll) | beam.Flatten()  # Flatten merges the header with the actual data.

# Main pipeline function where the data is ingested, processed, and written to a file.
def run():
    # PipelineOptions allows us to pass runtime arguments or options to the pipeline (e.g., runner, project, etc.).
    options = beam.options.pipeline_options.PipelineOptions()

    # Defining the pipeline.
    with beam.Pipeline(options=options) as p:
        # Read CSV file from a GCS bucket or local file.
        # The 'ReadFromText' transform reads the data line by line. We skip the header using 'skip_header_lines'.
        transactions = (p
                        | 'ReadCSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
                        | 'ProcessTransactions' >> ProcessTransactions()  # Apply our composite transform to filter and sum.
                        | 'FormatAsCSV' >> beam.Map(format_as_csv))  # Convert each record into a CSV string format.

        # Add header and data together.
        output_with_header = add_header(transactions)

        # Write the output to a compressed CSV file.
        # Using the WriteToText transform, we can specify the output path and file format (.csv.gz).
        output_with_header | 'WriteResults' >> beam.io.WriteToText('output/results', file_name_suffix='.csv.gz', shard_name_template='')

# Entry point for the pipeline.
if __name__ == '__main__':
    run()
