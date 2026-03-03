# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
"""Bootstraps the resources required to run the QuickSight integration tests."""

import logging
import os

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.bootstrapping.s3 import Bucket
from acktest.bootstrapping.iam import Role
from acktest.aws.identity import get_account_id

from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources
from e2e.subscription import ensure_subscription


def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    # Get AWS account ID
    aws_account_id = get_account_id()

    # Get notification email from environment variable or use default
    notification_email = os.environ.get(
        "QUICKSIGHT_NOTIFICATION_EMAIL", f"ack-infra+quicksight-resources@amazon.com"
    )
    edition = os.environ.get("QUICKSIGHT_EDITION", "ENTERPRISE")

    # Ensure QuickSight subscription exists
    logging.info(f"Ensuring QuickSight subscription for account {aws_account_id}")
    try:
        subscription_info = ensure_subscription(
            aws_account_id, notification_email, edition
        )
        logging.info(f"QuickSight subscription ready: {subscription_info}")
    except Exception as e:
        logging.error(f"Failed to ensure QuickSight subscription: {e}")
        raise BootstrapFailureException(
            f"Failed to bootstrap QuickSight subscription: {e}"
        )

    # Create S3 bucket for DataSource tests
    data_source_bucket = Bucket(name_prefix="qs-test-bucket")

    # Create IAM role for QuickSight to access S3
    quicksight_s3_role = Role(
        name_prefix="qs-test-role",
        principal_service="quicksight.amazonaws.com",
        description="Test role for QuickSight DataSource e2e tests",
        managed_policies=["arn:aws:iam::aws:policy/AdministratorAccess"],
    )

    resources = BootstrapResources(
        SubscriptionAccountId=aws_account_id,
        SubscriptionEdition=edition,
        DataSourceBucket=data_source_bucket,
        QuickSightS3Role=quicksight_s3_role,
    )

    try:
        resources.bootstrap()
    except BootstrapFailureException as ex:
        exit(254)

    # Upload sample data to the S3 bucket after bootstrap
    _upload_sample_data(data_source_bucket)

    return resources


def _upload_sample_data(bucket: Bucket):
    """Upload sample CSV data and manifest to the S3 bucket."""
    data_file_key = "data.csv"
    manifest_key = "manifest.json"

    csv_content = """id,name,value,category
        1,Item A,100,electronics
        2,Item B,200,books
        3,Item C,150,electronics
        4,Item D,300,clothing
        5,Item E,250,books
        """
    bucket.s3_client.put_object(Bucket=bucket.name, Key=data_file_key, Body=csv_content)

    manifest_content = f"""{{
        "fileLocations": [
            {{
            "URIs": [
                "s3://{bucket.name}/{data_file_key}"
            ]
            }}
        ],
        "globalUploadSettings": {{
            "format": "CSV",
            "containsHeader": true,
            "delimiter": ","
        }}
        }}"""
    bucket.s3_client.put_object(
        Bucket=bucket.name, Key=manifest_key, Body=manifest_content
    )

    logging.info(f"Uploaded sample data to s3://{bucket.name}/{data_file_key}")
    logging.info(f"Uploaded manifest to s3://{bucket.name}/{manifest_key}")


if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
