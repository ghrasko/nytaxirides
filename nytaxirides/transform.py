'''
Author: Gábor Hraskó
Email: gabor@hrasko.com

This module contains the custom pipeline transformation modules if those 
are not implemented as in-line lambda functions within the pipeline itself. 
ParDo transformations are implemented as classes, Map transformations are as
functions:

class Interpret(beam.DoFn)
def CountGroups(element, deduplicate, key_name=None)
def TotalGroupField(element, deduplicate, element_key_name, col_name)
class AddWindowInfo(beam.DoFn)
class AddProcessTimestamp(beam.DoFn)
'''

import json                     # for data import
from datetime import datetime   # for timestamp
import logging

import apache_beam as beam


class Interpret(beam.DoFn):
    ''' Read streamed message into Python dictionary based on JSON schema '''
    def process(self, element):
        try:
            e = json.loads(element.data)
            e_reduced = {
                'ride_id' : e['ride_id'],
                'timestamp' : e['timestamp'],
                'ride_status' : e['ride_status'],
                'passenger_count' : e['passenger_count']
            }
            yield e_reduced
        except json.decoder.JSONDecodeError as err:
            logging.warning('Poorly-formed message, not JSON: %s\n%s', 
                element.data,
                err)
            return
        except KeyError as err:
            logging.warning('Message lacks obligatory attributes: %s\n%s', 
                element.data,
                err)
            return


def CountGroups(element, deduplicate, key_name=None):
    ''' Map function to count items in the groups created by GroupBy().
    Optionally performs deduplication before aggregation. 
    Returned record example: { 'ride_status': 'dropoff', 'count': 18176 } '''
    import logging
    (key, records) = element
    duplicates = 0
    if deduplicate:
        ids = set()
        count = 0
        for record in records:
            id = record['ride_id']
            if id in ids:
                duplicates += 1
            else:
                ids.add(id)
                count += 1
    else:
        count = len(records)

    if (key_name == None):
        key_name = 'group'

    logging.info('CountGroups() dropped %s duplicate messages', duplicates)
    e = { key_name: key, 'count' : count }
    return e


def TotalGroupField(element, deduplicate, element_key_name, col_name):
    ''' Map function to sum a field in the groups created by GroupBy().
    Optionally performs deduplication before aggregation. 
    Returned record example: { 'sum_passengers': 45007 } '''
    (key, records) = element
    duplicates = 0
    if deduplicate:
        ids = set()
        total = 0
        for record in records:
            id = record['ride_id']
            if id in ids:
                duplicates += 1
            else:
                ids.add(id)
                total += record[element_key_name]
    else:
        total = sum(record[element_key_name] for record in records)

    logging.info('TotalGroupField() dropped %s duplicate messages', duplicates)
    e = { col_name: total }
    return e


class AddWindowInfo(beam.DoFn):
    ''' Add window info to the aggregation record '''
    def process(self, element, window=beam.DoFn.WindowParam):
        import copy
        e = copy.deepcopy(element)
        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        e['window_start'] = window.start.to_utc_datetime().strftime(ts_format)
        e['window_end'] = window.end.to_utc_datetime().strftime(ts_format)
        yield e


class AddProcessTimestamp(beam.DoFn):
    ''' Add process time timestamp to the raw record '''
    def process(self, element):
        import copy
        e = copy.deepcopy(element)
        ts_format = '%Y-%m-%d %H:%M:%S UTC'
        dt = datetime.utcnow()
        e['processed'] = dt.strftime(ts_format)
        yield e
