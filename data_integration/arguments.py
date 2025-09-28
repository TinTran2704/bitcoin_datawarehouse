import argparse
import autologging
import logging


parser = argparse.ArgumentParser(
    description = 'Parser to manage pulling data.'
)

parser.add_argument(
    '--full-refresh', 
    action='store_true',
    help='''
    If specified, fully-load the target table from the source.
    '''
)

parser.add_argument(
    '--incremental-value', 
    type=str,
    help='''
    If specified, use this to load data of last year/month for developing purpose.
    '''
)

parser.add_argument(
    '--select', 
    # type=str,
    nargs="*",
    help='''
    If specified, Sync only specific tables.
    '''
)

arguments = parser.parse_args()



FULL_REFRESH = arguments.full_refresh
INCREMENTAL_VALUE = arguments.incremental_value
SELECT = arguments.select
