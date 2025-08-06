#!/usr/bin/env python3
"""
End-to-End Test Runner for AWS Glue Data Replication

This script orchestrates the execution of comprehensive end-to-end tests including:
- Test environment setup and teardown
- Full-load and incremental load scenario testing
- Cross-database replication validation
- Test result reporting and analysis

Usage:
    python run_end_to_end_tests.py [--engines oracle,postgresql] [--scenarios full,incremental] [--report-format json]
"""

import argparse
import json
import sys
import time
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import unittest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_end_to_end import (
    EndToEndTestFramework, TestDataManager,
    TestEndToEndFullLoad, TestEndToEndIncrementalLoad,
    TestCrossDatabaseReplication, TestDataValidationFramework
)


class EndToEndTestRunner:
    """Orchestrates end-to-end test execution and reporting."""
    
    def __init__(self, engines: Optional[List[str]] = None, 
                 scenarios: Optional[List[str]] = None,
                 report_format: str = 'json'):
        """Initialize test runner with configuration."""
        self.framework = EndToEndTestFramework()
        self.engines = engines or ['oracle', 'sqlserver', 'postgresql', 'db2']
        self.scenarios = scenarios or ['full', 'incremental', 'cross-database', 'validation']
        self.report_format = report_format
        self.test_results = {}
        self.start_time = None
        self.end_time = None
        
    def setup_test_environment(self) -> Dict[str, Any]:
        """Set up the complete test environment."""
        print("Setting up end-to-end test environment...")
        
        try:
            test_env = self.framework.setup_test_environment()
            
            # Filter to requested engines only
            filtered_configs = {
                engine: config for engine, config in test_env['test_configs'].items()
                if engine in self.engines
            }
            test_env['test_configs'] = filtered_configs
            
            print(f"✓ Test environment setup completed for engines: {', '.join(self.engines)}")
            return test_env
            
        except Exception as e:
            print(f"✗ Test environment setup failed: {str(e)}")
            raise
    
    def run_full_load_tests(self, test_env: Dict[str, Any]) -> Dict[str, Any]:
        """Execute full-load replication tests."""
        if 'full' not in self.scenarios:
            return {}
        
        print("\nRunning full-load replication tests...")
        start_time = time.time()
        
        try:
            results = self.framework.run_full_load_tests(test_env['test_configs'])
            
            # Filter results to requested engines
            filtered_results = {
                key: result for key, result in results.items()
                if any(engine in key for engine in self.engines)
            }
            
            execution_time = time.time() - start_time
            
            success_count = sum(1 for result in filtered_results.values() 
                              if result.get('status') == 'success')
            total_count = len(filtered_results)
            
            print(f"✓ Full-load tests completed: {success_count}/{total_count} successful ({execution_time:.2f}s)")
            
            return {
                'results': filtered_results,
                'summary': {
                    'total_tests': total_count,
                    'successful_tests': success_count,
                    'failed_tests': total_count - success_count,
                    'execution_time_seconds': execution_time,
                    'success_rate': (success_count / total_count * 100) if total_count > 0 else 0
                }
            }
            
        except Exception as e:
            print(f"✗ Full-load tests failed: {str(e)}")
            return {'error': str(e)}
    
    def run_incremental_load_tests(self, test_env: Dict[str, Any]) -> Dict[str, Any]:
        """Execute incremental load replication tests."""
        if 'incremental' not in self.scenarios:
            return {}
        
        print("\nRunning incremental load replication tests...")
        start_time = time.time()
        
        try:
            results = self.framework.run_incremental_load_tests(test_env['test_configs'])
            
            # Filter and summarize results
            filtered_results = {}
            total_tests = 0
            successful_tests = 0
            
            for strategy, strategy_results in results.items():
                filtered_strategy_results = {
                    key: result for key, result in strategy_results.items()
                    if any(engine in key for engine in self.engines)
                }
                
                if filtered_strategy_results:
                    filtered_results[strategy] = filtered_strategy_results
                    total_tests += len(filtered_strategy_results)
                    successful_tests += sum(1 for result in filtered_strategy_results.values()
                                          if result.get('status') == 'success')
            
            execution_time = time.time() - start_time
            
            print(f"✓ Incremental load tests completed: {successful_tests}/{total_tests} successful ({execution_time:.2f}s)")
            
            return {
                'results': filtered_results,
                'summary': {
                    'total_tests': total_tests,
                    'successful_tests': successful_tests,
                    'failed_tests': total_tests - successful_tests,
                    'execution_time_seconds': execution_time,
                    'success_rate': (successful_tests / total_tests * 100) if total_tests > 0 else 0,
                    'strategies_tested': list(filtered_results.keys())
                }
            }
            
        except Exception as e:
            print(f"✗ Incremental load tests failed: {str(e)}")
            return {'error': str(e)}
    
    def run_cross_database_tests(self, test_env: Dict[str, Any]) -> Dict[str, Any]:
        """Execute cross-database replication tests."""
        if 'cross-database' not in self.scenarios:
            return {}
        
        print("\nRunning cross-database replication tests...")
        start_time = time.time()
        
        try:
            # Run cross-database compatibility tests
            compatibility_results = {}
            validation_results = {}
            
            for source_engine in self.engines:
                compatibility_results[source_engine] = {}
                validation_results[source_engine] = {}
                
                for target_engine in self.engines:
                    if source_engine == target_engine:
                        continue
                    
                    source_config = test_env['test_configs'][source_engine]
                    target_config = test_env['test_configs'][target_engine]
                    
                    # Test compatibility
                    table_results = {}
                    for table_name in self.framework.data_manager.test_schemas.keys():
                        validation = self.framework.data_manager.validate_replication_accuracy(
                            source_config, target_config, table_name
                        )
                        table_results[table_name] = validation
                    
                    compatibility_results[source_engine][target_engine] = table_results
                    
                    # Test actual replication
                    replication_result = self.framework._simulate_full_load_test(
                        source_config, target_config
                    )
                    validation_results[source_engine][target_engine] = replication_result
            
            execution_time = time.time() - start_time
            
            # Calculate success metrics
            total_combinations = len(self.engines) * (len(self.engines) - 1)
            successful_replications = sum(
                1 for source_results in validation_results.values()
                for result in source_results.values()
                if result.get('status') == 'success'
            )
            
            print(f"✓ Cross-database tests completed: {successful_replications}/{total_combinations} successful ({execution_time:.2f}s)")
            
            return {
                'compatibility_results': compatibility_results,
                'replication_results': validation_results,
                'summary': {
                    'total_combinations': total_combinations,
                    'successful_replications': successful_replications,
                    'failed_replications': total_combinations - successful_replications,
                    'execution_time_seconds': execution_time,
                    'success_rate': (successful_replications / total_combinations * 100) if total_combinations > 0 else 0
                }
            }
            
        except Exception as e:
            print(f"✗ Cross-database tests failed: {str(e)}")
            return {'error': str(e)}
    
    def run_validation_tests(self, test_env: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data validation and accuracy tests."""
        if 'validation' not in self.scenarios:
            return {}
        
        print("\nRunning data validation tests...")
        start_time = time.time()
        
        try:
            validation_results = {}
            
            # Test validation for each engine combination
            for source_engine in self.engines:
                validation_results[source_engine] = {}
                
                for target_engine in self.engines:
                    if source_engine == target_engine:
                        continue
                    
                    source_config = test_env['test_configs'][source_engine]
                    target_config = test_env['test_configs'][target_engine]
                    
                    table_validations = {}
                    for table_name in self.framework.data_manager.test_schemas.keys():
                        validation = self.framework.data_manager.validate_replication_accuracy(
                            source_config, target_config, table_name
                        )
                        table_validations[table_name] = validation
                    
                    validation_results[source_engine][target_engine] = table_validations
            
            execution_time = time.time() - start_time
            
            # Calculate validation metrics
            total_validations = 0
            successful_validations = 0
            
            for source_results in validation_results.values():
                for target_results in source_results.values():
                    for validation in target_results.values():
                        total_validations += 1
                        if (validation.get('row_count_match') and 
                            validation.get('data_integrity_check') and 
                            validation.get('schema_compatibility')):
                            successful_validations += 1
            
            print(f"✓ Validation tests completed: {successful_validations}/{total_validations} successful ({execution_time:.2f}s)")
            
            return {
                'results': validation_results,
                'summary': {
                    'total_validations': total_validations,
                    'successful_validations': successful_validations,
                    'failed_validations': total_validations - successful_validations,
                    'execution_time_seconds': execution_time,
                    'success_rate': (successful_validations / total_validations * 100) if total_validations > 0 else 0
                }
            }
            
        except Exception as e:
            print(f"✗ Validation tests failed: {str(e)}")
            return {'error': str(e)}
    
    def run_unittest_suite(self) -> Dict[str, Any]:
        """Run the unittest suite for additional validation."""
        print("\nRunning unittest validation suite...")
        start_time = time.time()
        
        try:
            # Create test suite with filtered test classes
            test_suite = unittest.TestSuite()
            
            test_classes = []
            if 'full' in self.scenarios:
                test_classes.append(TestEndToEndFullLoad)
            if 'incremental' in self.scenarios:
                test_classes.append(TestEndToEndIncrementalLoad)
            if 'cross-database' in self.scenarios:
                test_classes.append(TestCrossDatabaseReplication)
            if 'validation' in self.scenarios:
                test_classes.append(TestDataValidationFramework)
            
            for test_class in test_classes:
                tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
                test_suite.addTests(tests)
            
            # Run tests with custom result collector
            test_result = unittest.TestResult()
            test_suite.run(test_result)
            
            execution_time = time.time() - start_time
            
            success_count = test_result.testsRun - len(test_result.failures) - len(test_result.errors)
            
            print(f"✓ Unittest suite completed: {success_count}/{test_result.testsRun} successful ({execution_time:.2f}s)")
            
            return {
                'tests_run': test_result.testsRun,
                'successful_tests': success_count,
                'failures': len(test_result.failures),
                'errors': len(test_result.errors),
                'execution_time_seconds': execution_time,
                'success_rate': (success_count / test_result.testsRun * 100) if test_result.testsRun > 0 else 0,
                'failure_details': [
                    {'test': str(test), 'error': error} 
                    for test, error in test_result.failures
                ],
                'error_details': [
                    {'test': str(test), 'error': error} 
                    for test, error in test_result.errors
                ]
            }
            
        except Exception as e:
            print(f"✗ Unittest suite failed: {str(e)}")
            return {'error': str(e)}
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate comprehensive test report."""
        report_data = {
            'test_execution': {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                'total_execution_time_seconds': (
                    (self.end_time - self.start_time).total_seconds() 
                    if self.start_time and self.end_time else 0
                ),
                'engines_tested': self.engines,
                'scenarios_tested': self.scenarios
            },
            'test_results': results,
            'overall_summary': self._calculate_overall_summary(results)
        }
        
        if self.report_format == 'json':
            return json.dumps(report_data, indent=2, default=str)
        elif self.report_format == 'text':
            return self._generate_text_report(report_data)
        else:
            return json.dumps(report_data, indent=2, default=str)
    
    def _calculate_overall_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall test execution summary."""
        total_tests = 0
        successful_tests = 0
        total_time = 0
        
        for scenario_name, scenario_results in results.items():
            if isinstance(scenario_results, dict) and 'summary' in scenario_results:
                summary = scenario_results['summary']
                total_tests += summary.get('total_tests', 0) or summary.get('total_combinations', 0) or summary.get('total_validations', 0)
                successful_tests += summary.get('successful_tests', 0) or summary.get('successful_replications', 0) or summary.get('successful_validations', 0)
                total_time += summary.get('execution_time_seconds', 0)
        
        return {
            'total_tests_executed': total_tests,
            'total_successful_tests': successful_tests,
            'total_failed_tests': total_tests - successful_tests,
            'overall_success_rate': (successful_tests / total_tests * 100) if total_tests > 0 else 0,
            'total_execution_time_seconds': total_time
        }
    
    def _generate_text_report(self, report_data: Dict[str, Any]) -> str:
        """Generate human-readable text report."""
        lines = []
        lines.append("=" * 80)
        lines.append("AWS GLUE DATA REPLICATION - END-TO-END TEST REPORT")
        lines.append("=" * 80)
        
        # Execution summary
        exec_info = report_data['test_execution']
        lines.append(f"Test Execution Started: {exec_info['start_time']}")
        lines.append(f"Test Execution Ended: {exec_info['end_time']}")
        lines.append(f"Total Execution Time: {exec_info['total_execution_time_seconds']:.2f} seconds")
        lines.append(f"Engines Tested: {', '.join(exec_info['engines_tested'])}")
        lines.append(f"Scenarios Tested: {', '.join(exec_info['scenarios_tested'])}")
        lines.append("")
        
        # Overall summary
        overall = report_data['overall_summary']
        lines.append("OVERALL SUMMARY")
        lines.append("-" * 40)
        lines.append(f"Total Tests: {overall['total_tests_executed']}")
        lines.append(f"Successful: {overall['total_successful_tests']}")
        lines.append(f"Failed: {overall['total_failed_tests']}")
        lines.append(f"Success Rate: {overall['overall_success_rate']:.1f}%")
        lines.append("")
        
        # Scenario details
        for scenario_name, scenario_results in report_data['test_results'].items():
            if isinstance(scenario_results, dict) and 'summary' in scenario_results:
                lines.append(f"{scenario_name.upper()} TESTS")
                lines.append("-" * 40)
                summary = scenario_results['summary']
                
                for key, value in summary.items():
                    if key.endswith('_seconds'):
                        lines.append(f"{key.replace('_', ' ').title()}: {value:.2f}")
                    elif key.endswith('_rate'):
                        lines.append(f"{key.replace('_', ' ').title()}: {value:.1f}%")
                    else:
                        lines.append(f"{key.replace('_', ' ').title()}: {value}")
                lines.append("")
        
        return "\n".join(lines)
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Execute all configured test scenarios."""
        print(f"Starting end-to-end test execution for engines: {', '.join(self.engines)}")
        print(f"Test scenarios: {', '.join(self.scenarios)}")
        
        self.start_time = datetime.now()
        
        try:
            # Setup test environment
            test_env = self.setup_test_environment()
            
            # Execute test scenarios
            results = {}
            
            if 'full' in self.scenarios:
                results['full_load_tests'] = self.run_full_load_tests(test_env)
            
            if 'incremental' in self.scenarios:
                results['incremental_load_tests'] = self.run_incremental_load_tests(test_env)
            
            if 'cross-database' in self.scenarios:
                results['cross_database_tests'] = self.run_cross_database_tests(test_env)
            
            if 'validation' in self.scenarios:
                results['validation_tests'] = self.run_validation_tests(test_env)
            
            # Run unittest suite for additional validation
            results['unittest_results'] = self.run_unittest_suite()
            
            self.end_time = datetime.now()
            
            print(f"\n✓ All test scenarios completed successfully!")
            print(f"Total execution time: {(self.end_time - self.start_time).total_seconds():.2f} seconds")
            
            return results
            
        except Exception as e:
            self.end_time = datetime.now()
            print(f"\n✗ Test execution failed: {str(e)}")
            return {'error': str(e)}


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description='Run end-to-end tests for AWS Glue Data Replication'
    )
    
    parser.add_argument(
        '--engines',
        type=str,
        default='oracle,sqlserver,postgresql,db2',
        help='Comma-separated list of database engines to test (default: all)'
    )
    
    parser.add_argument(
        '--scenarios',
        type=str,
        default='full,incremental,cross-database,validation',
        help='Comma-separated list of test scenarios to run (default: all)'
    )
    
    parser.add_argument(
        '--report-format',
        type=str,
        choices=['json', 'text'],
        default='json',
        help='Output report format (default: json)'
    )
    
    parser.add_argument(
        '--output-file',
        type=str,
        help='Output file for test report (default: stdout)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Parse engine and scenario lists
    engines = [engine.strip() for engine in args.engines.split(',')]
    scenarios = [scenario.strip() for scenario in args.scenarios.split(',')]
    
    # Validate engines
    supported_engines = ['oracle', 'sqlserver', 'postgresql', 'db2']
    invalid_engines = [engine for engine in engines if engine not in supported_engines]
    if invalid_engines:
        print(f"Error: Unsupported engines: {', '.join(invalid_engines)}")
        print(f"Supported engines: {', '.join(supported_engines)}")
        sys.exit(1)
    
    # Validate scenarios
    supported_scenarios = ['full', 'incremental', 'cross-database', 'validation']
    invalid_scenarios = [scenario for scenario in scenarios if scenario not in supported_scenarios]
    if invalid_scenarios:
        print(f"Error: Unsupported scenarios: {', '.join(invalid_scenarios)}")
        print(f"Supported scenarios: {', '.join(supported_scenarios)}")
        sys.exit(1)
    
    # Create and run test runner
    runner = EndToEndTestRunner(
        engines=engines,
        scenarios=scenarios,
        report_format=args.report_format
    )
    
    try:
        # Execute tests
        results = runner.run_all_tests()
        
        # Generate report
        report = runner.generate_report(results)
        
        # Output report
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(report)
            print(f"\nTest report written to: {args.output_file}")
        else:
            print("\n" + "=" * 80)
            print("TEST REPORT")
            print("=" * 80)
            print(report)
        
        # Exit with appropriate code
        overall_summary = runner._calculate_overall_summary(results)
        success_rate = overall_summary.get('overall_success_rate', 0)
        
        if success_rate >= 90:
            print(f"\n✓ Test execution completed successfully (Success rate: {success_rate:.1f}%)")
            sys.exit(0)
        else:
            print(f"\n⚠ Test execution completed with issues (Success rate: {success_rate:.1f}%)")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nTest execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error during test execution: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()