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

"""Utilities for managing QuickSight subscription"""

import logging
import time
import datetime
import boto3
from botocore.exceptions import ClientError

# Wait configuration
DEFAULT_WAIT_TIMEOUT_SECONDS = 300  # 5 minutes
DEFAULT_WAIT_INTERVAL_SECONDS = 10  # Check every 10 seconds

def get_subscription_status(aws_account_id: str) -> dict:
    """Returns the QuickSight subscription status for the account.
    
    Returns None if no subscription exists.
    """
    client = boto3.client('quicksight')
    try:
        resp = client.describe_account_subscription(
            AwsAccountId=aws_account_id
        )
        return resp.get('AccountInfo')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return None
        logging.error(f"Error checking subscription status: {e}")
        raise


def create_subscription(aws_account_id: str, notification_email: str, edition: str = 'ENTERPRISE') -> dict:
    """Creates a QuickSight subscription for the account.
    
    Args:
        aws_account_id: AWS account ID
        notification_email: Email for QuickSight notifications
        edition: QuickSight edition (STANDARD or ENTERPRISE)
    
    Returns:
        Account subscription information
    """
    client = boto3.client('quicksight')
    try:
        resp = client.create_account_subscription(
            AwsAccountId=aws_account_id,
            AccountName=f'quicksight-test-{aws_account_id}',
            NotificationEmail=notification_email,
            Edition=edition,
            AuthenticationMethod='IAM_AND_QUICKSIGHT'
        )
        logging.info(f"Created QuickSight subscription for account {aws_account_id}")
        return resp
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceExistsException':
            logging.info(f"QuickSight subscription already exists for account {aws_account_id}")
            return get_subscription_status(aws_account_id)
        logging.error(f"Error creating subscription: {e}")
        raise


def ensure_subscription(aws_account_id: str, notification_email: str, edition: str = 'ENTERPRISE') -> dict:
    """Ensures a QuickSight subscription exists for the account.
    
    Creates the subscription if it doesn't exist, otherwise returns existing subscription info.
    Waits until the subscription is in ACTIVE state before returning.
    
    Args:
        aws_account_id: AWS account ID
        notification_email: Email for QuickSight notifications
        edition: QuickSight edition (STANDARD or ENTERPRISE)
    
    Returns:
        Account subscription information
    
    Raises:
        TimeoutError: If subscription doesn't become active within timeout period
    """
    status = get_subscription_status(aws_account_id)
    if status is not None:
        logging.info(f"QuickSight subscription already exists for account {aws_account_id}")
        # Even if it exists, wait for it to be active
        return wait_until_subscription_active(aws_account_id)
    
    logging.info(f"Creating QuickSight subscription for account {aws_account_id}")
    create_subscription(aws_account_id, notification_email, edition)
    
    # Wait for subscription to become active
    return wait_until_subscription_active(aws_account_id)


def wait_until_subscription_active(
        aws_account_id: str,
        timeout_seconds: int = DEFAULT_WAIT_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_INTERVAL_SECONDS,
    ) -> dict:
    """Waits until the QuickSight subscription is in ACTIVE state.
    
    Args:
        aws_account_id: AWS account ID
        timeout_seconds: Maximum time to wait in seconds
        interval_seconds: Time between status checks in seconds
    
    Returns:
        Account subscription information when active
    
    Raises:
        TimeoutError: If subscription doesn't become active within timeout period
        RuntimeError: If subscription enters an error state
    """
    logging.info(f"Waiting for QuickSight subscription to become active (timeout: {timeout_seconds}s)...")
    
    # Define known status values
    ACTIVE_STATUSES = ['ACCOUNT_CREATED', 'OK']  # OK is a legacy status
    IN_PROGRESS_STATUSES = ['SIGNUP_ATTEMPT_IN_PROGRESS']
    ERROR_STATUSES = ['CREATION_FAILED', 'UNSUBSCRIBED']
    
    start_time = datetime.datetime.now()
    timeout = start_time + datetime.timedelta(seconds=timeout_seconds)
    
    while datetime.datetime.now() < timeout:
        status = get_subscription_status(aws_account_id)
        
        if status is None:
            logging.warning("Subscription not found, waiting for it to be created...")
            time.sleep(interval_seconds)
            continue
        
        subscription_status = status.get('AccountSubscriptionStatus', 'UNKNOWN')
        logging.info(f"Current subscription status: {subscription_status}")
        
        # Check for active state
        if subscription_status in ACTIVE_STATUSES:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            logging.info(f"✓ QuickSight subscription is ready (status: {subscription_status}, took {elapsed:.1f}s)")
            return status
        
        # Check for error states
        if subscription_status in ERROR_STATUSES:
            raise RuntimeError(
                f"QuickSight subscription entered error state: {subscription_status}. "
                f"Please check AWS console for details."
            )
        
        # Still in progress states
        if subscription_status in IN_PROGRESS_STATUSES:
            logging.info(f"Subscription creation in progress, waiting {interval_seconds}s before next check...")
        else:
            # Unknown status - log warning but continue waiting
            logging.warning(f"Unknown subscription status '{subscription_status}', continuing to wait...")
        
        time.sleep(interval_seconds)
    
    # Timeout reached
    elapsed = (datetime.datetime.now() - start_time).total_seconds()
    current_status = status.get('AccountSubscriptionStatus', 'UNKNOWN') if status else 'NOT_FOUND'
    raise TimeoutError(
        f"Timed out waiting for QuickSight subscription to become active after {elapsed:.1f}s. "
        f"Current status: {current_status}. Expected one of: {ACTIVE_STATUSES}"
    )
