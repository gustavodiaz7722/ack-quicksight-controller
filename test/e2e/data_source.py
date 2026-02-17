# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Utilities for working with DataSource resources"""

import datetime
import time
import logging

import boto3
import pytest

DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS = 90
DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS = 15
DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS = 15


def wait_until_exists(
        aws_account_id: str,
        data_source_id: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a DataSource with a supplied ID is returned from QuickSight
    DescribeDataSource API.

    Usage:
        from e2e.data_source import wait_until_exists

        wait_until_exists(aws_account_id, data_source_id)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for DataSource to exist "
                "in QuickSight API"
            )
        time.sleep(interval_seconds)

        latest = get_data_source(aws_account_id, data_source_id)
        if latest is not None:
            break


def wait_until_deleted(
        aws_account_id: str,
        data_source_id: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a DataSource with a supplied ID is no longer returned from
    the QuickSight API.

    Usage:
        from e2e.data_source import wait_until_deleted

        wait_until_deleted(aws_account_id, data_source_id)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for DataSource to be "
                "deleted in QuickSight API"
            )
        time.sleep(interval_seconds)

        latest = get_data_source(aws_account_id, data_source_id)
        if latest is None:
            break


def get_data_source(aws_account_id: str, data_source_id: str):
    """Returns a dict containing the DataSource from the QuickSight
    DescribeDataSource API.

    If no such DataSource exists, returns None.
    """
    c = boto3.client('quicksight')
    try:
        resp = c.describe_data_source(
            AwsAccountId=aws_account_id,
            DataSourceId=data_source_id
        )
        return resp['DataSource']
    except c.exceptions.ResourceNotFoundException:
        return None
    except Exception as e:
        logging.debug(e)
        return None


def get_tags(aws_account_id: str, data_source_arn: str):
    """Returns the tags for the data source with a supplied ARN.

    If no such DataSource exists, returns None.
    """
    c = boto3.client('quicksight')
    try:
        resp = c.list_tags_for_resource(ResourceArn=data_source_arn)
        return resp['Tags']
    except c.exceptions.ResourceNotFoundException:
        return None
    except Exception as e:
        logging.debug(e)
        return None
