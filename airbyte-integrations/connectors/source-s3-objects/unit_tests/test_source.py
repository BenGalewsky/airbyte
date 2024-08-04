#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock, patch

import pytest

from source_s3_objects.source import SourceS3Objects


@patch('boto3.client')
def test_check_connection(mock_boto_client):
    mock_s3_client = mock_boto_client.return_value

    source = SourceS3Objects()
    logger_mock = MagicMock()
    config = {
        "aws_access_key_id": "test_access",
        "aws_secret_access_key": "test_secret",
        "region_name": "test_region",
        "endpoint": "test_endpoint",
        "bucket": "test_bucket"
    }
    assert source.check_connection(logger_mock, config) == (True, None)
    mock_boto_client.assert_called_with(aws_access_key_id= 'test_access',
                                        aws_secret_access_key='test_secret',
                                        endpoint_url='test_endpoint',
                                        region_name='test_region',
                                        service_name='s3')
    mock_s3_client.list_objects.assert_called_with(Bucket='test_bucket', MaxKeys=1)


def test_streams():
    source = SourceS3Objects()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
