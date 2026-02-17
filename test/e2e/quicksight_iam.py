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

"""IAM bootstrapping utilities for QuickSight e2e tests."""

import logging

from dataclasses import dataclass, field
from typing import List

from acktest.bootstrapping import Bootstrappable
from acktest.bootstrapping.iam import Role


@dataclass
class QuickSightTestRole(Bootstrappable):
    """Creates a dedicated IAM role for QuickSight DataSource testing.
    
    This role is assumed by QuickSight to access S3 data sources during testing.
    """
    
    # Inputs
    name_prefix: str = "qs-test-role"
    managed_policies: List[str] = field(default_factory=lambda: [
        "arn:aws:iam::aws:policy/AdministratorAccess"
    ])
    
    # Subresources
    role: Role = field(init=False)
    
    # Outputs
    role_arn: str = field(init=False, default="")
    role_name: str = field(init=False, default="")
    
    def __post_init__(self):
        # Create the Role subresource with QuickSight as the principal service
        self.role = Role(
            name_prefix=self.name_prefix,
            principal_service="quicksight.amazonaws.com",
            description="Test role for QuickSight DataSource e2e tests",
            managed_policies=self.managed_policies
        )
    
    def bootstrap(self):
        """Create the QuickSight test role with S3 permissions."""
        super().bootstrap()
        
        self.role_arn = self.role.arn
        self.role_name = self.role.name
        logging.info(f"Created QuickSight test role: {self.role_name} ({self.role_arn})")
    
    def cleanup(self):
        """Delete the QuickSight test role."""
        super().cleanup()
        logging.info(f"Cleaned up QuickSight test role: {self.role_name}")
