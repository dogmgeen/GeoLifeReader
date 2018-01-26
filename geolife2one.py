#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SYNOPSIS

	python geolife2one.py [-h,--help] [-v,--verbose]


DESCRIPTION

	Preprocess and convert Microsoft's Geolife dataset for use in the ONE
	simulator.


ARGUMENTS

	-h, --help          show this help message and exit
	-v, --verbose       verbose output


AUTHOR

	Doug McGeehan <doug.mcgeehan@mst.edu>


LICENSE

	Copyright 2015-2018 Doug McGeehan - GNU GPLv3

"""

import geolife2one.download

__appname__ = "geolife2one"
__author__ = "Doug McGeehan"
__version__ = "0.0pre0"
__license__ = "GNU GPLv3"


import argparse
from datetime import datetime
import sys
import os
import logging

logger = logging.getLogger(__appname__)


def main():
    args = get_arguments()
    setup_logger(args)

    # Figure out which argument key is the longest so that all the
    # parameters can be printed out nicely
    logger.debug('Command-line arguments:')
    length_of_longest_key = len(max(vars(args).keys(),
                                    key=lambda k: len(k)))

    # Print out arguments to the command line.
    for arg in vars(args):
        value = getattr(args, arg)
        logger.debug('\t{argument_key}:\t{value}'.format(
            argument_key=arg.rjust(length_of_longest_key, ' '),
            value=value))

    logger.debug(start_time)

    # Begin execution of the appropriate functionality based on the
    # subcommand supplied on the command line
    args.func(args)


def setup_logger(args):
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    log_file = os.path.join('/tmp', __appname__ + '.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()

    if args.verbose:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    line_numbers_and_function_name = logging.Formatter(
        "%(levelname)s [ %(pathname)s::%(funcName)s():%(lineno)s ] "
        "%(message)s")
    fh.setFormatter(line_numbers_and_function_name)
    ch.setFormatter(line_numbers_and_function_name)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Preprocess and convert Microsoft's Geolife dataset"
                    " for use in the ONE simulator."
    )
    # Verbosity of logs / console output
    parser.add_argument('-v', '--verbose', action='store_true',
                        default=False, help='verbose output')

    # Add subcommands - e.g.:
    #       $ geolife2one download
    #       $ geolife2one initdb
    #           etc.
    subparsers = parser.add_subparsers(help='Subcommands to execute.')

    # Download the Geolife dataset if not currently present
    geolife_downloader = subparsers.add_parser(
        'download', help='Download the GeoLife dataset from Microsoft if not '
                         'currently present.'
    )
    geolife_downloader.add_argument(
        '-o', '--output-directory', type=new_directory_if_not_present,
        default=new_directory_if_not_present('downloads'),
        help='Output directory into which the GeoLife dataset should be '
             'downloaded (default: ./downloads).'
    )

    geolife_downloader.set_defaults(func=geolife2one.download.main)

    # Initialize the database into which the GeoLife dataset will be loaded
    db_initializer = subparsers.add_parser(
        'initdb', help='Create the PostgreSQL database and tables into which '
                       'the GeoLife dataset will be loaded later.'
    )

    # Load the GeoLife dataset into the database
    db_loader = subparsers.add_parser(
        'load', help='Load the raw GeoLife dataset into the PostgreSQL database'
    )
    db_loader.add_argument(
        '-i', '--input-directory', type=existing_directory,
        default=existing_directory('downloads'),
        help='Directory in which the GeoLife dataset resides '
             '(default: ./downloads).'
    )

    # Preprocess the GeoLife dataset: homogenize the time between location
    # records and eliminate subtrajectories outside of a specified boundary
    preprocessor = subparsers.add_parser(
        'preprocess', help='Preprocess the GeoLife dataset to homogenize the '
                           'time intervals between consecutive location '
                           'records and eliminate subtrajectories outside of '
                           'a specified boundary.'
    )

    # Export the preprocessed GeoLife dataset into the format expected by the
    #  ONE simulator.
    exporter = subparsers.add_parser(
        'export', help='Export the preprocessed (time-homogenized and pruned) '
                       'GeoLife dataset to the format expected by the ONE '
                       'simulator.'
    )
    exporter.add_argument(
        '-o', '--output-directory', type=new_directory_if_not_present,
        default=new_directory_if_not_present('output'),
        help='Output directory into which the preprocessed GeoLife dataset '
             'should be exported.'
    )

    args = parser.parse_args()
    return args


################################################################################
# Command-line utilities for creating directories or verifying they exist
#
def new_directory_if_not_present(directory):
    """
    Create a new directory if it does not already exist at a given location.
    :param directory: the directory to create; could be a relative or absolute 
    path
    :return: absolute path of the directory
    """
    os.makedirs(directory, exist_ok=True)
    return os.path.abspath(directory)


def existing_directory(directory):
    """
    Verify a directory exists; if not, raise an assertion error.
    :param directory: the directory to verify exists
    :return: absolute path of the directory
    """
    absolute_path = os.path.abspath(directory)
    assert os.path.isdir(directory), (
        'No directory exists at "{directory}"!\n'
        'Perhaps you need to run '
        '`$ geolife2one download --output-directory "{directory}"`'
        'first?'.format(
            directory=absolute_path
    ))
    return absolute_path


if __name__ == '__main__':
    try:
        start_time = datetime.now()

        main()

        finish_time = datetime.now()
        logger.debug(finish_time)
        logger.debug('Execution time: {time}'.format(
            time=(finish_time - start_time)
        ))
        logger.debug("#" * 20 + " END EXECUTION " + "#" * 20)

        sys.exit(0)

    except KeyboardInterrupt as e:      # Ctrl-C
        raise e

    except SystemExit as e:             # sys.exit()
        raise e

    except Exception as e:
        logger.exception("Something happened and I don't know what to do D:")
        sys.exit(1)

