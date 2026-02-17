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
"""Bootstraps the resources required to run the QuickSight integration tests.
"""
import logging
import os

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.aws.identity import get_account_id

from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources
from e2e.subscription import ensure_subscription

def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    # Get AWS account ID
    aws_account_id = get_account_id()
    
    # Get notification email from environment variable or use default
    notification_email = os.environ.get('QUICKSIGHT_NOTIFICATION_EMAIL', f'ack-infra+quicksight-resources@amazon.com')
    edition = os.environ.get('QUICKSIGHT_EDITION', 'ENTERPRISE')
    
    # Ensure QuickSight subscription exists
    logging.info(f"Ensuring QuickSight subscription for account {aws_account_id}")
    try:
        subscription_info = ensure_subscription(aws_account_id, notification_email, edition)
        logging.info(f"QuickSight subscription ready: {subscription_info}")
    except Exception as e:
        logging.error(f"Failed to ensure QuickSight subscription: {e}")
        raise BootstrapFailureException(f"Failed to bootstrap QuickSight subscription: {e}")

    resources = BootstrapResources(
        SubscriptionAccountId=aws_account_id,
        SubscriptionEdition=edition
    )

    try:
        resources.bootstrap()
    except BootstrapFailureException as ex:
        exit(254)

    return resources

if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
