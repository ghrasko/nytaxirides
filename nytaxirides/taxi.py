'''
Author: Gábor Hraskó
Email: gabor@hrasko.com

This is the main module of the NY Taxi Trips stream analyzer demo project.
The program reads streaming data about NY state taxi rides from GCP PubSub 
topic and calculates various aggregated metrics.
'''

import sys
import datetime as dt
import logging
import argparse

from nytaxirides.pipeline import run_pipeline


def run():
    ''' Reads configuration from command line and runs the pipeline '''

    '''
    # Uncoment if you prefer defining the parameters here instead of the command line
    # You should also change the actual parsing below in 2 lines by adding args as param.
    args = [
        "E:\\ghrasko\\Documents\\Develop\\.auth\\taxirides-gcp-keys.json",
        "--setup_file", "E:\\ghrasko\\Documents\\Develop\\ny-taxi-rides\\setup.py", 
        "--runner", "DataflowRunner",
        "--project", "ny-taxi-rides-hg", 
        "--subscription", "taxiride",
        "--window_length", "3600",
        "--region", "us-central1", 
        "--staging_location", "gs://ny-taxi-rides-hg/binaries", 
        "--temp_location", "gs://ny-taxi-rides-hg/temp", 
        "--num_workers", "1",
        "--max_num_workers", "2",
        "--job_name", "taxiride",
        "--beam_deduplication",
        "--custom_deduplication",
        "--streaming",
        "--save_main_session", 
        "--purge",
        "--raw_messages",
        "--check_duplicates",
        "--loglevel", "20"
    ]
    '''

    # The following arguments are not used by the GCP pipeline routines
    parser = argparse.ArgumentParser(description='Running the NY Taxi Trips analysis stream pipeline')
    parser.add_argument('cred_file', help='GCP credentials file name with path')
    parser.add_argument('--purge', '-p', action='store_true', help=
                            ('Purge pending messages before starting'))
    parser.add_argument('--raw_messages', action='store_true', help='Whether to save row messages.')
    parser.add_argument('--window_length', type=int, default=3600, help=
                            ('Length of the fix, non-shifting window in seconds. '
                            'The default is 3600 seconds (i.e. one hour)).'))
    parser.add_argument('--beam_deduplication', action='store_true', help=
                            ('Whether to activate Beam\'s own deduplication mechanism. '
                            ' Seems not working anyhow.'))
    parser.add_argument('--custom_deduplication', action='store_true', help=
                            ('Whether to filter duplicate messages with custom algorithm. '
                            'The algorithm is very basic and not optimized yet.'))
    parser.add_argument('--loglevel', type=int, default=20, help=
                            ('Log level as integer. Standard values are as follows: '
                            'NOTSET=0, DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50.'))
    parser.add_argument('--subscription', default='taxiride', help=
                            ('PubSup subscription name. Default = taxiride. One could '
                            'create a test topic and subscription for manual testing.'))
    # At this point we retrieve the GCP pipeline params (below) as list.
    # This argument list will be passed to the GCP pipeline creation routine.
    # _, pipeline_args = parser.parse_known_args( args )
    _, pipeline_args = parser.parse_known_args()
    # The next arguments are required both as list and as config variable
    # See: https://beam.apache.org/releases/pydoc/2.9.0/_modules/apache_beam/options/pipeline_options.html
    parser.add_argument('--project', help='Name of the Cloud project owning the Dataflow job.')
    parser.add_argument('--runner', help=('Pipeline runner used to execute the workflow. '
                            'Valid values are DirectRunner, DataflowRunner.'))
    parser.add_argument('--streaming', default=False, action='store_true', help=
                            ('Whether to enable streaming mode.'))
    parser.add_argument('--save_main_session', default=False, action='store_true', help=
                            ('Save the main session state so that pickled functions and classes '
                            'defined in __main__ (e.g. interactive session) can be unpickled. '
                            'Some workflows do not need the session state if for instance all '
                            'their functions/classes are defined in proper modules (not __main__)'
                            ' and the modules are importable in the worker. '))
    parser.add_argument('--region', default='us-central1', help=
                            ('The Google Compute Engine region for creating Dataflow job.'))
    parser.add_argument('--staging_location', default=None, help=
                            ('GCS path for staging code packages needed by workers'))
    parser.add_argument('--temp_location', default=None, help=
                            ('GCS path for saving temporary workflow jobs.'))
    parser.add_argument('--job_name', default=None, help='Name of the Cloud Dataflow job.')
    parser.add_argument('--num_workers', type=int, default=None, help=
                            ('Number of workers to use when executing the Dataflow job. If not '
                            'set, the Dataflow service will use a reasonable default.'))
    parser.add_argument('--max_num_workers', type=int, default=None, help=
                            ('Maximum number of workers to use when executing the Dataflow job.'))
    parser.add_argument('--setup_file', default=None, help=
                            ('Path to a setup Python file containing package dependencies. If '
                            'specified, the file\'s containing folder is assumed to have the '
                            'structure required for a setuptools setup package. The file must be '
                            'named setup.py. More details: '
                            'https://pythonhosted.org/an_example_pypi_project/setuptools.html '
                            'During job submission a source distribution will be built and the '
                            'worker will install the resulting package before running any custom '
                            'code.'))
    # Now we read all command line arguments to the config variable
    # cfg = parser.parse_args( args )
    cfg = parser.parse_args()

    # Add timestamp to the job name to avoid name collision. Because of the previous
    # parse checking, we could be sure the key is defined in the list with value.
    cfg.job_name = '{}-{}'.format(cfg.job_name, dt.datetime.now().strftime('%Y%m%d-%H%M%S'))
    idx = pipeline_args.index('--job_name')
    pipeline_args[idx + 1] = cfg.job_name

    logging.getLogger().setLevel(cfg.loglevel)
    run_pipeline(cfg, pipeline_args)


# For module testing...
if __name__ == '__main__':

    run()
    print('\nNormal program termination.\n')