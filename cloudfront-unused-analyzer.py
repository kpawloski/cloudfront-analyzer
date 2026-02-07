#!/usr/bin/env python3
"""
CloudFront Unused Distribution Analyzer

This script identifies potentially unused or idle CloudFront distributions by analyzing:
- Request metrics over the past 30 days
- Distribution status and configuration
- Origin accessibility
- Last modification dates

Requirements:
- boto3
- AWS credentials configured
- CloudWatch read permissions
- CloudFront read permissions
"""

import boto3
import csv
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import argparse
import sys

class CloudFrontAnalyzer:
    def __init__(self, profile_name: str = None):
        """Initialize AWS clients"""
        session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
        self.cloudfront = session.client('cloudfront')
        
        # CloudFront metrics are ONLY available in us-east-1
        # Force CloudWatch client to use us-east-1 regardless of session region
        self.cloudwatch = session.client('cloudwatch', region_name='us-east-1')
        self.cloudwatch_enabled = None
        self.region = 'us-east-1'  # Always use us-east-1 for CloudFront metrics
        
    def get_all_distributions(self) -> List[Dict[str, Any]]:
        """Retrieve all CloudFront distributions"""
        distributions = []
        paginator = self.cloudfront.get_paginator('list_distributions')
        
        try:
            for page in paginator.paginate():
                if 'Items' in page['DistributionList']:
                    distributions.extend(page['DistributionList']['Items'])
        except Exception as e:
            print(f"Error fetching distributions: {e}")
            return []
            
        return distributions
            
    def verify_cloudwatch_access(self, test_distribution_id: str = None) -> Dict[str, Any]:
        """Verify CloudWatch metrics access and availability"""
        verification = {
            'cloudwatch_accessible': False,
            'region_correct': True,  # Always true since we force us-east-1
            'metrics_available': False,
            'test_distribution_id': test_distribution_id,
            'errors': [],
            'warnings': []
        }
        
        print("✓ Using us-east-1 region for CloudFront metrics (forced)")
        
        # Test basic CloudWatch access
        try:
            # Try to list some metrics to verify access
            response = self.cloudwatch.list_metrics(
                Namespace='AWS/CloudFront'
            )
            verification['cloudwatch_accessible'] = True
            
            if response.get('Metrics'):
                verification['metrics_available'] = True
                print(f"✓ CloudWatch access verified. Found {len(response['Metrics'])} CloudFront metrics")
            else:
                verification['warnings'].append("CloudWatch accessible but no CloudFront metrics found")
                
        except Exception as e:
            verification['errors'].append(f"CloudWatch access failed: {e}")
            return verification
        
        # Test with a specific distribution if provided
        if test_distribution_id:
            try:
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=7)  # Test with shorter period
                
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/CloudFront',
                    MetricName='Requests',
                    Dimensions=[
                        {
                            'Name': 'DistributionId',
                            'Value': test_distribution_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=['Sum']
                )
                
                if response['Datapoints']:
                    verification['test_metrics_found'] = len(response['Datapoints'])
                    total_requests = sum(point['Sum'] for point in response['Datapoints'])
                    print(f"✓ Test distribution {test_distribution_id}: Found {len(response['Datapoints'])} datapoints, {int(total_requests)} requests")
                else:
                    verification['warnings'].append(f"Test distribution {test_distribution_id} has no metrics data in last 7 days")
                    
            except Exception as e:
                verification['errors'].append(f"Failed to get metrics for test distribution {test_distribution_id}: {e}")
        
        return verification
    
    def check_distribution_metrics_enabled(self, distribution_id: str) -> Dict[str, Any]:
        """Check if metrics are enabled/available for a specific distribution"""
        check_result = {
            'distribution_id': distribution_id,
            'has_recent_metrics': False,
            'has_any_metrics': False,
            'metrics_age_days': None,
            'available_metrics': [],
            'errors': []
        }
        
        try:
            # Check what metrics are available for this distribution
            response = self.cloudwatch.list_metrics(
                Namespace='AWS/CloudFront',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ]
            )
            
            if response['Metrics']:
                check_result['has_any_metrics'] = True
                check_result['available_metrics'] = [m['MetricName'] for m in response['Metrics']]
                
                # Check for recent data (last 7 days)
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=7)
                
                try:
                    metrics_response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/CloudFront',
                        MetricName='Requests',
                        Dimensions=[
                            {
                                'Name': 'DistributionId',
                                'Value': distribution_id
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum']
                    )
                    
                    if metrics_response['Datapoints']:
                        check_result['has_recent_metrics'] = True
                        # Find the most recent datapoint
                        latest_point = max(metrics_response['Datapoints'], key=lambda x: x['Timestamp'])
                        age = (datetime.utcnow().replace(tzinfo=latest_point['Timestamp'].tzinfo) - latest_point['Timestamp']).days
                        check_result['metrics_age_days'] = age
                        
                except Exception as e:
                    check_result['errors'].append(f"Error checking recent metrics: {e}")
            
        except Exception as e:
            check_result['errors'].append(f"Error listing metrics: {e}")
        
        return check_result
    
    def check_distribution_metrics_enabled(self, distribution_id: str) -> Dict[str, Any]:
        """Check if metrics are enabled/available for a specific distribution"""
        check_result = {
            'distribution_id': distribution_id,
            'has_recent_metrics': False,
            'has_any_metrics': False,
            'metrics_age_days': None,
            'available_metrics': [],
            'errors': []
        }
        
        try:
            # Check what metrics are available for this distribution
            response = self.cloudwatch.list_metrics(
                Namespace='AWS/CloudFront',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ]
            )
            
            if response['Metrics']:
                check_result['has_any_metrics'] = True
                check_result['available_metrics'] = [m['MetricName'] for m in response['Metrics']]
                
                # Check for recent data (last 7 days)
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=7)
                
                try:
                    metrics_response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/CloudFront',
                        MetricName='Requests',
                        Dimensions=[
                            {
                                'Name': 'DistributionId',
                                'Value': distribution_id
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum']
                    )
                    
                    if metrics_response['Datapoints']:
                        check_result['has_recent_metrics'] = True
                        # Find the most recent datapoint
                        latest_point = max(metrics_response['Datapoints'], key=lambda x: x['Timestamp'])
                        age = (datetime.utcnow().replace(tzinfo=latest_point['Timestamp'].tzinfo) - latest_point['Timestamp']).days
                        check_result['metrics_age_days'] = age
                        
                except Exception as e:
                    check_result['errors'].append(f"Error checking recent metrics: {e}")
            
        except Exception as e:
            check_result['errors'].append(f"Error listing metrics: {e}")
        
        return check_result
    
    
    def get_distribution_metrics(self, distribution_id: str, days: int = 30, debug: bool = False) -> Dict[str, Any]:
        """Get CloudWatch metrics for a distribution over specified days"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        metrics = {
            'requests': 0,
            'bytes_downloaded': 0,
            'bytes_uploaded': 0,
            '4xx_errors': 0,
            '5xx_errors': 0,
            'total_error_rate': 0,
            'has_data': False,
            'datapoints_found': 0,
            'errors': []
        }
        
        if debug:
            print(f"  Querying metrics from {start_time} to {end_time}")
        
        # First, get the exact dimensions that CloudWatch knows about
        try:
            list_response = self.cloudwatch.list_metrics(
                Namespace='AWS/CloudFront',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ]
            )
            
            if not list_response.get('Metrics'):
                if debug:
                    print(f"  No metrics found for distribution {distribution_id}")
                return metrics
                
            # Find the Requests metric and use its exact dimensions
            requests_metric = None
            for metric in list_response['Metrics']:
                if metric['MetricName'] == 'Requests':
                    requests_metric = metric
                    break
            
            if not requests_metric:
                if debug:
                    print(f"  No Requests metric found for distribution {distribution_id}")
                return metrics
            
            if debug:
                print(f"  Found Requests metric with dimensions: {requests_metric['Dimensions']}")
            
        except Exception as e:
            error_msg = f"Could not list metrics for {distribution_id}: {e}"
            metrics['errors'].append(error_msg)
            if debug:
                print(f"  Error: {error_msg}")
            return metrics
        
        # Now get the actual metric data using the exact dimensions
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName='Requests',
                Dimensions=requests_metric['Dimensions'],  # Use exact dimensions from list_metrics
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                metrics['has_data'] = True
                metrics['datapoints_found'] = len(response['Datapoints'])
                metrics['requests'] = sum(point['Sum'] for point in response['Datapoints'])
                if debug:
                    print(f"  Found {len(response['Datapoints'])} datapoints for requests: {metrics['requests']}")
            else:
                if debug:
                    print(f"  No datapoints found for requests metric")
                    
        except Exception as e:
            error_msg = f"Could not fetch Requests for {distribution_id}: {e}"
            metrics['errors'].append(error_msg)
            if debug:
                print(f"  Error: {error_msg}")
        
        # Try BytesDownloaded with the same approach
        try:
            bytes_metric = None
            for metric in list_response['Metrics']:
                if metric['MetricName'] == 'BytesDownloaded':
                    bytes_metric = metric
                    break
            
            if bytes_metric:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/CloudFront',
                    MetricName='BytesDownloaded',
                    Dimensions=bytes_metric['Dimensions'],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=['Sum']
                )
                
                if response['Datapoints']:
                    metrics['bytes_downloaded'] = sum(point['Sum'] for point in response['Datapoints'])
                    if debug:
                        print(f"  BytesDownloaded: {metrics['bytes_downloaded']}")
                        
        except Exception as e:
            error_msg = f"Could not fetch BytesDownloaded for {distribution_id}: {e}"
            metrics['errors'].append(error_msg)
            if debug:
                print(f"  Error: {error_msg}")
        
        # Try error rate metrics with the same approach
        error_metrics = ['4xxErrorRate', '5xxErrorRate', 'TotalErrorRate']
        
        for error_metric_name in error_metrics:
            try:
                error_metric = None
                for metric in list_response['Metrics']:
                    if metric['MetricName'] == error_metric_name:
                        error_metric = metric
                        break
                
                if error_metric:
                    response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/CloudFront',
                        MetricName=error_metric_name,
                        Dimensions=error_metric['Dimensions'],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Average']
                    )
                    
                    if response['Datapoints']:
                        avg_error_rate = sum(point['Average'] for point in response['Datapoints']) / len(response['Datapoints'])
                        
                        if error_metric_name == '4xxErrorRate':
                            metrics['4xx_errors'] = avg_error_rate
                        elif error_metric_name == '5xxErrorRate':
                            metrics['5xx_errors'] = avg_error_rate
                        elif error_metric_name == 'TotalErrorRate':
                            metrics['total_error_rate'] = avg_error_rate
                            
                        if debug:
                            print(f"  {error_metric_name}: {avg_error_rate:.2f}%")
                            
            except Exception as e:
                error_msg = f"Could not fetch {error_metric_name} for {distribution_id}: {e}"
                metrics['errors'].append(error_msg)
                if debug:
                    print(f"  Error: {error_msg}")
                
        return metrics
    
    def analyze_distribution(self, distribution: Dict[str, Any], debug: bool = False) -> Dict[str, Any]:
        """Analyze a single distribution for usage patterns"""
        dist_id = distribution['Id']
        domain_name = distribution['DomainName']
        
        if debug:
            print(f"\nAnalyzing {dist_id} ({domain_name})")
        
        # Get detailed distribution config
        try:
            config_response = self.cloudfront.get_distribution(Id=dist_id)
            config = config_response['Distribution']['DistributionConfig']
            enabled = config.get('Enabled', True)
            comment = config.get('Comment', '')
        except Exception as e:
            if debug:
                print(f"  Error getting config for {dist_id}: {e}")
            config = {}
            enabled = distribution.get('Enabled', True)
            comment = distribution.get('Comment', '')
        
        # Get metrics
        metrics = self.get_distribution_metrics(dist_id, debug=debug)
        
        # Analyze usage patterns
        analysis = {
            'id': dist_id,
            'domain_name': domain_name,
            'status': distribution['Status'],
            'enabled': enabled,
            'last_modified': distribution['LastModifiedTime'].isoformat(),
            'metrics': metrics,
            'origins_count': len(distribution.get('Origins', {}).get('Items', [])),
            'price_class': distribution.get('PriceClass', 'Unknown'),
            'comment': comment,
            'unused_indicators': []
        }
        
        if debug:
            print(f"  Status: {analysis['status']}, Enabled: {analysis['enabled']}")
            print(f"  Requests: {metrics['requests']}, Has data: {metrics.get('has_data', False)}")
            print(f"  Datapoints found: {metrics.get('datapoints_found', 0)}")
        
        # Check for unused indicators with more aggressive detection
        if not analysis['enabled']:
            analysis['unused_indicators'].append('Distribution is disabled')
            if debug:
                print(f"  -> UNUSED: Distribution is disabled")
            
        # If we have no CloudWatch data at all, it might be unused
        if not metrics.get('has_data', False):
            analysis['unused_indicators'].append('No CloudWatch metrics data available (likely no traffic)')
            if debug:
                print(f"  -> UNUSED: No CloudWatch data")
        elif metrics['requests'] == 0:
            analysis['unused_indicators'].append('Zero requests in past 30 days')
            if debug:
                print(f"  -> UNUSED: Zero requests")
        elif metrics['requests'] < 100:
            analysis['unused_indicators'].append(f'Very low traffic: {int(metrics["requests"])} requests in 30 days')
            if debug:
                print(f"  -> UNUSED: Low traffic ({int(metrics['requests'])} requests)")
            
        if metrics['bytes_downloaded'] == 0 and metrics.get('has_data', False):
            analysis['unused_indicators'].append('Zero bytes downloaded in past 30 days')
            if debug:
                print(f"  -> UNUSED: Zero bytes downloaded")
            
        # Check if distribution is old and unused
        try:
            last_modified = datetime.fromisoformat(analysis['last_modified'].replace('Z', '+00:00'))
            days_since_modified = (datetime.now(last_modified.tzinfo) - last_modified).days
            
            if days_since_modified > 90 and metrics['requests'] < 10:
                analysis['unused_indicators'].append(f'Not modified in {days_since_modified} days with minimal traffic')
                if debug:
                    print(f"  -> UNUSED: Old and unused ({days_since_modified} days, {int(metrics['requests'])} requests)")
        except Exception as e:
            if debug:
                print(f"  Error parsing date: {e}")
            
        # Check for test/staging patterns in domain or comment
        test_patterns = ['test', 'staging', 'dev', 'demo', 'temp', 'poc', 'prototype', 'sandbox']
        domain_lower = domain_name.lower()
        comment_lower = analysis['comment'].lower()
        
        for pattern in test_patterns:
            if pattern in domain_lower or pattern in comment_lower:
                analysis['unused_indicators'].append(f'Appears to be test/staging environment (contains "{pattern}")')
                if debug:
                    print(f"  -> UNUSED: Test environment pattern '{pattern}' found")
                break
        
        # Additional checks for potentially unused distributions
        if analysis['status'] != 'Deployed':
            analysis['unused_indicators'].append(f'Distribution status is {analysis["status"]} (not Deployed)')
            if debug:
                print(f"  -> UNUSED: Status is {analysis['status']}")
        
        # Check for high error rates (strong indicator of unused/broken distributions)
        total_error_rate = metrics.get('total_error_rate', 0)
        error_4xx = metrics.get('4xx_errors', 0)
        error_5xx = metrics.get('5xx_errors', 0)
        
        if debug:
            print(f"  Error rates - Total: {total_error_rate:.1f}%, 4xx: {error_4xx:.1f}%, 5xx: {error_5xx:.1f}%")
        
        # High error rate thresholds
        if total_error_rate >= 100:
            analysis['unused_indicators'].append(f'100% error rate - distribution is completely broken')
            if debug:
                print(f"  -> UNUSED: 100% error rate")
        elif total_error_rate >= 90:
            analysis['unused_indicators'].append(f'Very high error rate: {total_error_rate:.1f}% - likely unused or broken')
            if debug:
                print(f"  -> UNUSED: Very high error rate ({total_error_rate:.1f}%)")
        elif total_error_rate >= 50:
            analysis['unused_indicators'].append(f'High error rate: {total_error_rate:.1f}% - possibly unused or misconfigured')
            if debug:
                print(f"  -> UNUSED: High error rate ({total_error_rate:.1f}%)")
        
        # Check for specific error patterns
        if error_5xx >= 50:
            analysis['unused_indicators'].append(f'High 5xx error rate: {error_5xx:.1f}% - server errors indicate broken origin')
            if debug:
                print(f"  -> UNUSED: High 5xx errors ({error_5xx:.1f}%)")
        
        # Check for distributions with only errors (no successful requests)
        if metrics['requests'] > 0 and total_error_rate > 0:
            # Calculate approximate successful requests
            error_rate_decimal = total_error_rate / 100
            estimated_errors = metrics['requests'] * error_rate_decimal
            estimated_success = metrics['requests'] - estimated_errors
            
            if estimated_success < 10 and metrics['requests'] > 100:
                analysis['unused_indicators'].append(f'Almost all requests are errors - only ~{estimated_success:.0f} successful requests out of {int(metrics["requests"])}')
                if debug:
                    print(f"  -> UNUSED: Almost all requests are errors")
        
        if debug:
            print(f"  Total unused indicators: {len(analysis['unused_indicators'])}")
                
        return analysis
    
    def categorize_distribution(self, analysis: Dict[str, Any]) -> str:
        """Categorize a distribution based on its usage patterns"""
        metrics = analysis['metrics']
        
        # Check if disabled
        if not analysis['enabled']:
            return 'disabled'
        
        # Check for completely broken (100% error rate)
        if metrics.get('total_error_rate', 0) >= 100:
            return 'completely_broken'
        
        # Check for high error rate (50-99%)
        if metrics.get('total_error_rate', 0) >= 50:
            return 'high_error_rate'
        
        # Check for zero traffic
        if metrics['requests'] == 0:
            return 'zero_traffic'
        
        # Check for low traffic
        if metrics['requests'] < 100:
            return 'low_traffic'
        
        # Check for test/staging environment
        test_patterns = ['test', 'staging', 'dev', 'demo', 'temp', 'poc', 'prototype', 'sandbox']
        domain_lower = analysis['domain_name'].lower()
        comment_lower = analysis['comment'].lower()
        
        for pattern in test_patterns:
            if pattern in domain_lower or pattern in comment_lower:
                return 'test_environment'
        
        # Check if it has unused indicators but doesn't fit other categories
        if analysis['unused_indicators']:
            return 'other_unused'
        
        # Otherwise it's active
        return 'active'
    
    def generate_report(self, analyses: List[Dict[str, Any]], output_format: str = 'json') -> str:
        """Generate a report of the analysis"""
        # Separate unused from active distributions
        unused_distributions = [a for a in analyses if a['unused_indicators']]
        active_distributions = [a for a in analyses if not a['unused_indicators']]
        
        # Categorize all distributions
        categorized = {}
        for analysis in analyses:
            category = self.categorize_distribution(analysis)
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(analysis)
        
        report = {
            'analysis_date': datetime.utcnow().isoformat(),
            'total_distributions': len(analyses),
            'potentially_unused': len(unused_distributions),
            'active_distributions': len(active_distributions),
            'unused_distributions': unused_distributions,
            'categorized_distributions': categorized,
            'summary': {
                'disabled_count': len([a for a in analyses if not a['enabled']]),
                'zero_traffic_count': len([a for a in analyses if a['metrics']['requests'] == 0]),
                'low_traffic_count': len([a for a in analyses if 0 < a['metrics']['requests'] < 100]),
                'high_error_rate_count': len([a for a in analyses if 50 <= a['metrics'].get('total_error_rate', 0) < 100]),
                'completely_broken_count': len([a for a in analyses if a['metrics'].get('total_error_rate', 0) >= 100]),
                'test_env_count': len([a for a in analyses if any('test' in indicator.lower() or 'staging' in indicator.lower() for indicator in a['unused_indicators'])]),
                'categories': {category: len(distributions) for category, distributions in categorized.items()}
            }
        }
        
        if output_format == 'json':
            return json.dumps(report, indent=2, default=str)
        else:
            text_report = f"""
CloudFront Distribution Usage Analysis
=====================================
Analysis Date: {report['analysis_date']}
Total Distributions: {report['total_distributions']}
Potentially Unused: {report['potentially_unused']}
Active Distributions: {report['active_distributions']}

Summary:
- Disabled: {report['summary']['disabled_count']}
- Zero Traffic (30 days): {report['summary']['zero_traffic_count']}
- Low Traffic (<100 requests): {report['summary']['low_traffic_count']}
- High Error Rate (50-99%): {report['summary']['high_error_rate_count']}
- Completely Broken (100% errors): {report['summary']['completely_broken_count']}
- Test/Staging Environments: {report['summary']['test_env_count']}

Distribution Categories:
"""
            
            # Add category breakdown
            for category, count in report['summary']['categories'].items():
                category_name = category.replace('_', ' ').title()
                text_report += f"- {category_name}: {count}\n"
            
            text_report += f"""
Potentially Unused Distributions:
================================
"""
            
            for dist in unused_distributions:
                text_report += f"""
Distribution ID: {dist['id']}
Domain: {dist['domain_name']}
Status: {dist['status']} | Enabled: {dist['enabled']}
Last Modified: {dist['last_modified']}
Requests (30d): {dist['metrics']['requests']:,.0f}
Data Downloaded (30d): {dist['metrics']['bytes_downloaded']:,.0f} bytes
Error Rates: Total: {dist['metrics'].get('total_error_rate', 0):.1f}%, 4xx: {dist['metrics'].get('4xx_errors', 0):.1f}%, 5xx: {dist['metrics'].get('5xx_errors', 0):.1f}%
Category: {self.categorize_distribution(dist).replace('_', ' ').title()}
Unused Indicators:
"""
                for indicator in dist['unused_indicators']:
                    text_report += f"  - {indicator}\n"
                text_report += "-" * 50 + "\n"
                
            return text_report

    def save_csv_report(self, analyses: List[Dict[str, Any]], filename: str):
        """Save analysis results to a CSV file."""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Distribution ID', 'Domain Name', 'Status', 'Enabled',
                'Last Modified', 'Comment', 'Price Class', 'Origins Count',
                'Requests (30d)', 'Bytes Downloaded (30d)',
                'Total Error Rate %', '4xx Error Rate %', '5xx Error Rate %',
                'Category', 'Unused Indicators'
            ])
            for a in analyses:
                writer.writerow([
                    a['id'],
                    a['domain_name'],
                    a['status'],
                    'Yes' if a['enabled'] else 'No',
                    a['last_modified'],
                    a.get('comment', ''),
                    a.get('price_class', ''),
                    a.get('origins_count', 0),
                    int(a['metrics']['requests']),
                    int(a['metrics']['bytes_downloaded']),
                    f"{a['metrics'].get('total_error_rate', 0):.1f}",
                    f"{a['metrics'].get('4xx_errors', 0):.1f}",
                    f"{a['metrics'].get('5xx_errors', 0):.1f}",
                    self.categorize_distribution(a).replace('_', ' ').title(),
                    '; '.join(a['unused_indicators']) if a['unused_indicators'] else ''
                ])
        print(f"CSV report saved to {filename}")

def main():
    parser = argparse.ArgumentParser(description='Analyze CloudFront distributions for unused/idle instances')
    parser.add_argument('--profile', help='AWS profile name')
    parser.add_argument('--output', choices=['json', 'text'], default='text', help='Output format')
    parser.add_argument('--output-file', help='Output file path (default: stdout)')
    parser.add_argument('--csv', help='Output CSV report to file')
    parser.add_argument('--days', type=int, default=30, help='Number of days to analyze metrics (default: 30)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--sample', type=int, help='Only analyze first N distributions (for testing)')
    parser.add_argument('--broken', action='store_true', help='Only output distribution IDs with 100%% error rates (one per line)')
    
    args = parser.parse_args()
    
    try:
        analyzer = CloudFrontAnalyzer(profile_name=args.profile)
        
        # Skip CloudWatch verification if we're just looking for broken distributions
        if not args.broken:
            print("Verifying CloudWatch access...")
            
            # First, get a sample distribution for testing
            print("Fetching CloudFront distributions...")
            distributions = analyzer.get_all_distributions()
            
            if not distributions:
                print("No CloudFront distributions found or unable to access them.")
                sys.exit(1)
            
            # Verify CloudWatch access with a known distribution that has traffic
            test_dist_id = "E2SG8ZFUZOH8GZ"  # Known distribution with traffic
            print(f"Testing CloudWatch access with known active distribution: {test_dist_id}")
            verification = analyzer.verify_cloudwatch_access(test_dist_id)
            
            # Print verification results
            if verification['errors']:
                print("❌ CloudWatch Verification Issues:")
                for error in verification['errors']:
                    print(f"  - {error}")
            
            if verification['warnings']:
                print("⚠️  CloudWatch Warnings:")
                for warning in verification['warnings']:
                    print(f"  - {warning}")
            
            if not verification['cloudwatch_accessible']:
                print("❌ Cannot access CloudWatch metrics. Analysis may be incomplete.")
                print("Please check your AWS permissions and region settings.")
        else:
            # For --broken mode, just get distributions without verification
            distributions = analyzer.get_all_distributions()
            
            if not distributions:
                print("No CloudFront distributions found or unable to access them.", file=sys.stderr)
                sys.exit(1)
        
        if args.sample:
            distributions = distributions[:args.sample]
            if not args.broken:
                print(f"Analyzing first {len(distributions)} distributions (sample mode)")
        else:
            if not args.broken:
                print(f"Found {len(distributions)} distributions. Analyzing...")
        
        analyses = []
        broken_distribution_ids = []
        
        for i, dist in enumerate(distributions, 1):
            if not args.broken:
                print(f"Analyzing distribution {i}/{len(distributions)}: {dist['Id']}")
            
            # Check metrics availability for this distribution if in debug mode
            if args.debug and not args.broken:
                metrics_check = analyzer.check_distribution_metrics_enabled(dist['Id'])
                if not metrics_check['has_any_metrics']:
                    print(f"  ⚠️  No CloudWatch metrics found for this distribution")
                elif not metrics_check['has_recent_metrics']:
                    print(f"  ⚠️  No recent metrics (last 7 days) for this distribution")
            
            analysis = analyzer.analyze_distribution(dist, debug=args.debug and not args.broken)
            analyses.append(analysis)
            
            # Check if this distribution is completely broken (100% error rate)
            if analysis['metrics'].get('total_error_rate', 0) >= 100:
                broken_distribution_ids.append(dist['Id'])
            
            if args.debug and not args.broken and analysis['unused_indicators']:
                print(f"  FOUND UNUSED: {len(analysis['unused_indicators'])} indicators")
        
        # Handle --broken flag output
        if args.broken:
            if broken_distribution_ids:
                for dist_id in broken_distribution_ids:
                    print(dist_id)
            # Exit without generating full report
            sys.exit(0)
        
        print("Generating report...")
        report = analyzer.generate_report(analyses, args.output)
        
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(report)
            print(f"Report saved to {args.output_file}")
        else:
            print(report)
        
        if args.csv:
            analyzer.save_csv_report(analyses, args.csv)
            
    except Exception as e:
        print(f"Error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()