"""
Test configuration for pytest.

This module sets up the test environment by adding mock modules
to the Python path so that imports work correctly.
"""

import sys
import os
from unittest.mock import Mock

# Add the tests directory to the path so we can import mocks
tests_dir = os.path.dirname(os.path.abspath(__file__))
if tests_dir not in sys.path:
    sys.path.insert(0, tests_dir)

# Create mock modules in sys.modules so imports work
sys.modules['scripts'] = Mock()
sys.modules['scripts.glue_data_replication'] = Mock()
sys.modules['scripts.s3_parallel_operations'] = Mock()
sys.modules['glue_data_replication'] = Mock()

# Import our actual mocks and assign them
try:
    from mocks.glue_data_replication import getResolvedOptions, MockGlueContext, MockJob, logger
    from mocks.s3_parallel_operations import EnhancedS3ParallelOperations
    
    # Set up the mock modules with our implementations
    sys.modules['scripts.glue_data_replication'].getResolvedOptions = getResolvedOptions
    sys.modules['scripts.glue_data_replication'].GlueContext = MockGlueContext
    sys.modules['scripts.glue_data_replication'].Job = MockJob
    sys.modules['scripts.glue_data_replication'].logger = logger
    sys.modules['scripts.s3_parallel_operations'].EnhancedS3ParallelOperations = EnhancedS3ParallelOperations
    sys.modules['glue_data_replication'].getResolvedOptions = getResolvedOptions
    
except ImportError:
    # If mocks can't be imported, create basic mocks
    sys.modules['scripts.glue_data_replication'].getResolvedOptions = Mock()
    sys.modules['scripts.glue_data_replication'].logger = Mock()
    sys.modules['scripts.s3_parallel_operations'].EnhancedS3ParallelOperations = Mock()