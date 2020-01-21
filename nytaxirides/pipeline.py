'''
Author: Gábor Hraskó
Email: gabor@hrasko.com

Beam pipeline implementation:

[Main pipeline branch]
.. Read from PubSub
   Parse JSON to Dict (and drops some fields)
   Filter out enroute status
   [Runs if --raw_messages defined]
   .. Add process timestamp
      Save msg to BigQuery
   [Continue main line]
   .. Define windowing
      [Statuses count calculation]
      .. Define status as key
         Group by status field
         Count status frequency
         Add window info to counting
         Save counts to BigQuery
      [Passenger sum calculation]
      .. Filters for pickup status
         Define dummy key
         Group by dummy key
         Sum passengers
         Add window info to summing
         Save sum to BigQuery
 
Data source:
    - Pub/Sub messages: 
      Topic: projects/pubsub-public-data/topics/taxirides-realtime
      Subscription: projects/<project>/subscriptions/taxirides

Message format:
{
    "ride_id":"ffff404d-205f-4639-a9ba-e0a1c0e59ee9",
    "point_idx":156,
    "latitude":40.758680000000005,
    "longitude":-73.95870000000001,
    "timestamp":"2020-01-19T09:50:56.57238-05:00",
    "meter_reading":6.486022,
    "meter_increment":0.041577064,
    "ride_status":"pickup",
    "passenger_count":5
}

Data sinks:
    - GC BigQuery tables: 
        taxirides.filtered_records (optional, if --raw_messages defined)
        taxirides.status_counts
        taxirides.passenger_counts
'''

import os
import datetime as dt

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from google.oauth2 import service_account

from nytaxirides.transform import Interpret, CountGroups, TotalGroupField
from nytaxirides.transform import AddWindowInfo, AddProcessTimestamp


def purge_pipeline( args ):
    ''' Purges the NY Taxi rides PubSub Pileline '''
    from google.cloud import pubsub_v1
    from datetime import datetime

    subscriber = pubsub_v1.SubscriberClient()
    subscription_id = 'projects/{}/subscriptions/taxirides'.format(
        args.project,
    )
    t = pubsub_v1.types.Timestamp()
    t.FromDatetime(datetime.utcnow())
    subscriber.seek(subscription_id, time=t)
    return


def run_pipeline( cfg, pipeline_args ):
    ''' The PubSub pipeline definition for NY Taxi rides analysis '''

    # Set ENV variable with Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cfg.cred_file

    if cfg.purge:
        purge_pipeline(cfg)

    with beam.Pipeline(argv=pipeline_args) as p:
        # Main branch of the pipeline
        p0 = ( p 
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(
                subscription='projects/{}/subscriptions/{}'.format(
                    cfg.project,
                    cfg.subscription),
                with_attributes=True,
                timestamp_attribute=None,
                id_label=None
            | 'Parse JSON to Dict' >> beam.ParDo(Interpret())
            | 'Filter out enroute status' >> beam.Filter(lambda e: e['ride_status'] in ['pickup', 'dropoff'])
        )

        # Saves filtered records to perform data checking via manual batch processing.
        if cfg.raw_messages:
            p_save_raw_data = ( p0
                | 'Add process timestamp' >> beam.ParDo(AddProcessTimestamp())
                | 'Save msg to BigQuery' >> beam.io.WriteToBigQuery(
                    'filtered_records', 
                    dataset='taxirides', 
                    project=cfg.project,
                    schema='ride_id:STRING, timestamp:TIMESTAMP, ride_status:STRING, passenger_count:INTEGER, processed:TIMESTAMP', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )

        # Define windowing for upcoming aggregations.
        p_window = ( p0
            | 'Define windowing' >> beam.WindowInto(window.FixedWindows(
                    cfg.window_length, 
                    0))
        )

        # Counting statuses and reporting it in BigQuery. For larger sets possibly
        # Combine.perKey() is favourable instead of the GroupByKey()/ParDo pair used here.
        p_status_count = ( p_window
            | 'Define status as key' >> beam.Map(lambda e: (e['ride_status'], e))
            | 'Group by status field' >> beam.GroupByKey()
            | 'Count status frequency' >> beam.Map(
                    CountGroups, 
                    cfg.custom_deduplication, 
                    'ride_status')
            | 'Add window info to counting' >> beam.ParDo(AddWindowInfo())
            | 'Save counts to BigQuery' >> beam.io.WriteToBigQuery(
                    'status_counts', 
                    dataset='taxirides', 
                    project=cfg.project,
                    schema='ride_status:STRING, count:INTEGER, window_start:TIMESTAMP, window_end:TIMESTAMP', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

        # Summing passengers at 'pickup' status and reporting it to BigQuery
        p_passenger_count = ( p_window
            | 'Filters for pickup status' >> beam.Filter(lambda e: e['ride_status'] == 'pickup')
            | 'Define dummy key' >> beam.Map(lambda e: (1, e))
            | 'Group by dummy key' >> beam.GroupByKey()
            | 'Sum passengers' >> beam.Map(
                    TotalGroupField, 
                    cfg.custom_deduplication, 
                    'passenger_count', 
                    'sum_passengers')
            | 'Add window info to summing' >> beam.ParDo(AddWindowInfo())
            | 'Save sum to BigQuery' >> beam.io.WriteToBigQuery(
                    'passenger_counts', 
                    dataset='taxirides', 
                    project=cfg.project,
                    schema='sum_passengers:INTEGER, window_start:TIMESTAMP, window_end:TIMESTAMP', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

    return
