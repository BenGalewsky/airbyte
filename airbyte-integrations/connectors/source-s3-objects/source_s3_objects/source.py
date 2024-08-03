#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import boto3
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import StreamData, IncrementalMixin
from airbyte_protocol.models import SyncMode

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


class Objects(Stream, IncrementalMixin):
    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return "Key"

    def __init__(self, s3_client: boto3.client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self._last_modified = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        super().__init__()

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "LastModified"

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {
            "LastModified": self._last_modified
        }

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if "LastModified" in value:
            self._last_modified = value["LastModified"]

    def read_records(self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_slice: Optional[Mapping[str, Any]] = None,
                     stream_state: Optional[Mapping[str, Any]] = None) -> Iterable[StreamData]:
        if stream_state is None:
            stream_state = {}

        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                if obj["LastModified"] < stream_state.get("LastModified"):
                    continue
                self._last_modified = max(obj["LastModified"], self._last_modified)
                yield obj


# Source
def create_s3_client(config: Mapping[str, Any]) -> boto3.client:
    client_args = {
        "service_name": "s3"
    }

    if "aws_access_key_id" in config:
        client_args["aws_access_key_id"] = config["aws_access_key_id"]

    if "aws_secret_access_key" in config:
        client_args["aws_secret_access_key"] = config["aws_secret_access_key"]

    if "region_name" in config:
        client_args["region_name"] = config["region_name"]

    if "endpoint" in config:
        client_args["endpoint_url"] = config["endpoint"]

    client = boto3.client(**client_args)
    return client


class SourceS3Objects(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            client = create_s3_client(config)
            example = client.list_objects(
                Bucket=config["bucket"],
                MaxKeys=1)
            logger.info(f"example response: {example}")
            return True, None
        except Exception as error:
            return False, f"Unable to connect to S3 with the provided configuration. Error: {error}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [Objects(s3_client=create_s3_client(config), bucket=config["bucket"])]
