# Copyright 2019 Ismael Yuste
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""data_ingestion.py is a Dataflow pipeline which reads a file and writes its
contents to a BigQuery table.
This example does not do any transformation on the data.
"""

from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# This Dataflow py file reads a folder on a GCS BUCKET
# The CSV are from a BQ public table https://bigquery.cloud.google.com/table/bigquery-public-data:new_york_citibike.citibike_trips?pli=1
# with CSV files following this pattern (tripduration,starttime,stoptime,start_station_id,start_station_name,
# start_station_latitude,start_station_longitude,end_station_id,end_station_name,end_station_latitude,
# end_station_longitude,bikeid,usertype,birth_year,gender,customer_plan)
# and drop it data table at BQ

GCS_TO_PROCESS = 'gs://<bucket_name>/bikeshare.csv'
BQ_OUTPUT = 'sfo.bikeshare'
PROJECT = '<Project_id>'  # Project ID
DF_STAGING = 'gs://<bucket_name>/staging'
DF_TEMP = 'gs://<bucket_name>/temp'
DF_JOB_NAME = 'sfo-load-dayly-data'
DF_REGION = 'europe-west1'  # Your Preferred region

# This helper class contains the logic to translate the file rows to BQ


class DataIngestion:

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values in the form of
            the origin file

        Returns:
            A dict mapping BigQuery column names as keys to the corresponding
            value parsed from string_input.  In this example, the data is not
            transformed, and remains in the same format as the CSV.
            There are no date format transformations.
         """
        # Strip out return characters and quote characters.
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))

        # Maps 4 fields from the sfo citibyke dataset
        row = dict(zip(('station_id', 'bikes_available', 'docks_available', 'time'), values))

        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to load and the output table to
    # This is the final stage of the pipeline, where we define the destination
    # of the data.  In this case we are writing to BigQuery.
    parser.add_argument(
        '--input', dest='input', required=False,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.',
        default=GCS_TO_PROCESS)

    parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
                        default=BQ_OUTPUT)

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=' + PROJECT,
        '--staging_location=' + DF_STAGING,
        '--temp_location=' + DF_TEMP,
        '--job_name=' + DF_JOB_NAME,
        '--region=' + DF_REGION,
        '---save_main_session=true',
    ])

    # Maps 4 fields from the sfo citibyke dataset
    schema = ('station_id:INTEGER,bikes_available:INTEGER,docks_available:INTEGER,time:TIMESTAMP')

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file.  This is the source of the pipeline.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            known_args.output,
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            schema=schema,
            # As the table is day partitioned needs to be created beforehand
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Appends data to the table.
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()