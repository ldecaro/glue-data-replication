#!/usr/bin/env python3

import sys
import os

print("Starting validation...")

# Add src directory to path
src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src')
sys.path.insert(0, src_path)
print(f"Added src path: {src_path}")

# Mock modules
from unittest.mock import MagicMock
mock_modules = ['awsglue', 'pyspark', 'boto3']
for module in mock_modules:
    sys.modules[module] = MagicMock()
print("Mocked dependencies")

# Test imports
try:
    from glue_job.config import JobConfig
    print("✓ JobConfig imported")
except Exception as e:
    print(f"✗ JobConfig import failed: {e}")

try:
    from glue_job.storage import S3BookmarkStorage
    print("✓ S3BookmarkStorage imported")
except Exception as e:
    print(f"✗ S3BookmarkStorage import failed: {e}")

print("Validation complete!")