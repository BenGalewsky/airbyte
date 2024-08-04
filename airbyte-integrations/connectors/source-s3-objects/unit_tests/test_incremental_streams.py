#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from datetime import datetime
from zoneinfo import ZoneInfo

from airbyte_cdk.models import SyncMode
from dateutil.tz import tzutc
from pytest import fixture

from source import Objects

@fixture
def mock_s3_client(mocker):
    return mocker.MagicMock()


def test_cursor_field(mock_s3_client):
    stream = Objects(mock_s3_client, "test_bucket")
    expected_cursor_field = "LastModified"
    assert stream.cursor_field == expected_cursor_field


def test_supports_incremental(mock_s3_client, mocker):
    mocker.patch.object(Objects, "cursor_field", "dummy_field")
    stream = Objects(mock_s3_client, "test_bucket")
    assert stream.supports_incremental


def test_source_defined_cursor(mock_s3_client):
    stream = Objects(mock_s3_client, "test_bucket")
    assert stream.source_defined_cursor

# Helper function to create UTC datetime
def utc_datetime(*args):
    return datetime(*args, tzinfo=ZoneInfo("UTC"))


@fixture
def mock_s3_pages(mocker):
    mock_paginator = mocker.Mock()
    # Mock pages
    mock_page1 = {
        "Contents": [
            {"Key": "file1.txt", "LastModified": utc_datetime(2023, 1, 1, 12, 0, 0)},
            {"Key": "file2.txt", "LastModified": utc_datetime(2023, 1, 1, 13, 0, 0)},
        ]
    }
    mock_page2 = {
        "Contents": [
            {"Key": "file3.txt", "LastModified": utc_datetime(2023, 1, 1, 14, 0, 0)},
        ]
    }
    mock_paginator.paginate = mocker.Mock(return_value=[mock_page1, mock_page2])
    return mocker.Mock(return_value=mock_paginator)


def test_full_read(mock_s3_client, mock_s3_pages):
    mock_s3_client.get_paginator = mock_s3_pages
    stream = Objects(mock_s3_client, "test_bucket")
    records = stream.read_records(SyncMode.full_refresh)
    assert len(list(records)) == 3


def test_incremental_read(mock_s3_client, mock_s3_pages):
    mock_s3_client.get_paginator = mock_s3_pages
    stream = Objects(mock_s3_client, "test_bucket")
    # Set the stream state to a date that is before the first file's LastModified
    records = list(stream.read_records(SyncMode.incremental, stream_state={"LastModified": "2023-01-01T10:00:00.000000+00:00"}))
    assert len(records) == 3

    # Now set the stream state to a date that equals first file's LastModified
    records2 = list(stream.read_records(SyncMode.incremental, stream_state={"LastModified": "2023-01-01T12:00:00.000000+00:00"}))
    assert len(records2) == 2

