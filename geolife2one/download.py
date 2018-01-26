#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Download the GeoLife dataset to the provided directory

import logging
logger = logging.getLogger(__name__)


def main(args):
    logger.debug('Downloading the GeoLife dataset to {}'.format(
        args.output_directory
    ))


if __name__ == '__main__':
    import sys

    class DummyArgs(object):
        output_directory = sys.argv[-1]

    main(DummyArgs())