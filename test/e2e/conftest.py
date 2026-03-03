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

import boto3
import pytest
import logging

from acktest import k8s
from acktest.aws.identity import get_account_id
from e2e.subscription import ensure_subscription
import os

def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "canary: mark test to also run in canary tests"
    )
    config.addinivalue_line(
        "markers", "service(arg): mark test associated with a given service"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow to run"
    )

def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)

# Session-scoped fixture to ensure QuickSight subscription exists before any tests run
@pytest.fixture(scope='session', autouse=True)
def quicksight_subscription():
    """Ensures QuickSight subscription exists before running tests.
    
    This fixture runs once per test session and does NOT clean up the subscription
    after tests complete, as requested.
    """
    aws_account_id = get_account_id()
    notification_email = os.environ.get('QUICKSIGHT_NOTIFICATION_EMAIL', f'quicksight-test@example.com')
    edition = os.environ.get('QUICKSIGHT_EDITION', 'ENTERPRISE')
    
    logging.info(f"Bootstrapping QuickSight subscription for account {aws_account_id}")
    try:
        subscription_info = ensure_subscription(aws_account_id, notification_email, edition)
        logging.info(f"QuickSight subscription ready: Edition={subscription_info.get('Edition', 'N/A')}")
    except Exception as e:
        pytest.skip(f"Failed to bootstrap QuickSight subscription: {e}")
    
    # Yield without cleanup - subscription persists after tests
    yield subscription_info

# Provide a k8s client to interact with the integration test cluster
@pytest.fixture(scope='class')
def k8s_client():
    return k8s._get_k8s_api_client()

@pytest.fixture(scope='module')
def quicksight_client(quicksight_subscription):
    """Returns a QuickSight client.
    
    Depends on quicksight_subscription to ensure subscription is ready before client is used.
    """
    return boto3.client('quicksight')

@pytest.fixture(scope='module')
def s3_client():
    return boto3.client('s3')
