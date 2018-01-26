#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Download the GeoLife dataset to the provided directory.
# Available here: https://www.microsoft.com/en-us/download/details.aspx?id=52367

import logging
logger = logging.getLogger(__name__)

import os
import requests
import urllib.parse
import subprocess

GEOLIFE_DOWNLOAD_URL = 'https://download.microsoft.com/download/F/4/8/F4894AA5-FDBC-481E-9285-D5F8C4C4F039/Geolife%20Trajectories%201.3.zip'
DOWNLOAD_CHUNK_SIZE = (1024*1024)


def download_with_progress(path):
    """
    Download the GeoLife file with a progress bar. Deprecated: nice 
    functionality, but might not auto-scale to a user's bandwidth.
    :param path: the path to which the GeoLife dataset ZIP archive is to be 
                 downloaded
    :return: Nothing
    """
    import humanfriendly
    from tqdm import tqdm
    r = requests.get(GEOLIFE_DOWNLOAD_URL, stream=True)
    size = int(r.headers.get('content-length', 0))

    logger.debug('Download file size: {}'.format(
        humanfriendly.format_size(size)
    ))

    with open(path, 'wb') as f:
        for data in tqdm(r.iter_content(chunk_size=(1024 * 1024)),
                         total=size / DOWNLOAD_CHUNK_SIZE, unit='MB',
                         unit_scale=True):
            f.write(data)


def main(args):
    logger.info('Downloading the GeoLife dataset from {0} '
                'into {1.output_directory}'.format(
        GEOLIFE_DOWNLOAD_URL, args
    ))

    # Check to see if the file has already been downloaded; if so, abort
    download_file_name = os.path.basename(
        urllib.parse.unquote(
            urllib.parse.urlsplit(GEOLIFE_DOWNLOAD_URL).path
        )
    )
    download_to_path = os.path.join(args.output_directory, download_file_name)


    if os.path.isfile(download_to_path):
        import sys
        logger.warning('GeoLife archive already exists at "{}"! '
                       'Aborting.'.format(download_to_path))
        sys.exit(1)

    # Download the GeoLife dataset file and create a progress bar to plot the
    #  download's progress.
    logger.debug('Output file will be located at "{}"'.format(
        download_to_path
    ))

    # download_with_progress(path=download_to_path)

    subprocess.call([
        'wget',
        '--directory-prefix={}'.format(args.output_directory),
        GEOLIFE_DOWNLOAD_URL
    ])

    logger.debug('Download complete!')


if __name__ == '__main__':
    import sys

    class DummyArgs(object):
        output_directory = sys.argv[-1]

    main(DummyArgs())