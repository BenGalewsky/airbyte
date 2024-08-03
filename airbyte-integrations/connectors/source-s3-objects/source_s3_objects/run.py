#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceS3Objects


def run():
    source = SourceS3Objects()
    launch(source, sys.argv[1:])
