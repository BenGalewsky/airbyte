#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from destination_clowder import DestinationClowder

if __name__ == "__main__":
    DestinationClowder().run(sys.argv[1:])
