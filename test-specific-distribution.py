#!/usr/bin/env python3
"""
Test script for specific CloudFront distribution E2SG8ZFUZOH8GZ
"""

import boto3
from datetime import datetime, timedelta
import json
import argparse

def test_distribution_metrics(distribution_id="E2SG8ZFUZOH8GZ", profile_name=None, days=30):
    """Test metrics for the specific distribution"""
    
    print(f"Testing CloudWatch metrics for distribution: {distribution_id}")
    print(f"Time period: {days} days")
    print("=" * 60)
    
    # Initialize session
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
        print(f"Using AWS profile: {profile_name}")
    else:
        session = boto3.Session()
        print("Using default AWS credentials")
    
    # Force us-east-1 for CloudWatch (CloudFront metrics only exist there)
    cloudwatch = session.client('cloudwatch', region_name='us-east-1')
    print("✓ Using us-east-1 region for CloudWatch")
    
    # Test time range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    
    print(f"\nTime range:")
    print(f"  Start: {start_time}")
    print(f"  End: {end_time}")
    
    # Test 1: Check if metrics exist for this distribution
    print(f"\n{'='*60}")
    print("TEST 1: Check available metrics for this distribution")
    print(f"{'='*60}")
    
    try:
        response = cloudwatch.list_metrics(
            Namespace='AWS/CloudFront',
            Dimensions=[
                {
                    'Name': 'DistributionId',
                    'Value': distribution_id
                }
            ]
        )
        
        if response['Metrics']:
            print(f"✓ Found {len(response['Metrics'])} metric types for this distribution:")
            for metric in response['Metrics']:
                print(f"  - {metric['MetricName']}")
        else:
            print("❌ No metrics found for this distribution")
            print("This means the distribution has never generated any CloudWatch data")
            return
            
    except Exception as e:
        print(f"❌ Error listing metrics: {e}")
        return
    
    # Test 2: Get Requests metric with different time periods
    print(f"\n{'='*60}")
    print("TEST 2: Get Requests metric data")
    print(f"{'='*60}")
    
    time_periods = [
        (7, "7 days"),
        (30, "30 days"),
        (90, "90 days")
    ]
    
    for period_days, period_name in time_periods:
        print(f"\nTesting {period_name}:")
        test_end = datetime.utcnow()
        test_start = test_end - timedelta(days=period_days)
        
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName='Requests',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ],
                StartTime=test_start,
                EndTime=test_end,
                Period=86400,  # Daily
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                total_requests = sum(point['Sum'] for point in response['Datapoints'])
                print(f"  ✓ Found {len(response['Datapoints'])} datapoints")
                print(f"  ✓ Total requests: {int(total_requests):,}")
                
                # Show recent datapoints
                sorted_points = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)
                print(f"  Recent activity:")
                for point in sorted_points[:5]:
                    print(f"    {point['Timestamp'].strftime('%Y-%m-%d')}: {int(point['Sum']):,} requests")
            else:
                print(f"  ❌ No datapoints found for {period_name}")
                
        except Exception as e:
            print(f"  ❌ Error getting {period_name} data: {e}")
    
    # Test 3: Try different periods and statistics
    print(f"\n{'='*60}")
    print("TEST 3: Try different periods and statistics")
    print(f"{'='*60}")
    
    periods = [
        (3600, "1 hour"),
        (86400, "1 day"),
        (604800, "1 week")
    ]
    
    test_end = datetime.utcnow()
    test_start = test_end - timedelta(days=7)  # Last 7 days
    
    for period_seconds, period_name in periods:
        print(f"\nTesting with {period_name} periods:")
        
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName='Requests',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ],
                StartTime=test_start,
                EndTime=test_end,
                Period=period_seconds,
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                total_requests = sum(point['Sum'] for point in response['Datapoints'])
                print(f"  ✓ Found {len(response['Datapoints'])} datapoints")
                print(f"  ✓ Total requests: {int(total_requests):,}")
            else:
                print(f"  ❌ No datapoints with {period_name} periods")
                
        except Exception as e:
            print(f"  ❌ Error with {period_name} periods: {e}")
    
    # Test 4: Raw API response inspection
    print(f"\n{'='*60}")
    print("TEST 4: Raw API response inspection")
    print(f"{'='*60}")
    
    try:
        test_end = datetime.utcnow()
        test_start = test_end - timedelta(days=30)
        
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/CloudFront',
            MetricName='Requests',
            Dimensions=[
                {
                    'Name': 'DistributionId',
                    'Value': distribution_id
                }
            ],
            StartTime=test_start,
            EndTime=test_end,
            Period=86400,
            Statistics=['Sum']
        )
        
        print("Raw response structure:")
        print(f"  Response keys: {list(response.keys())}")
        print(f"  Datapoints type: {type(response.get('Datapoints', []))}")
        print(f"  Datapoints length: {len(response.get('Datapoints', []))}")
        
        if response.get('Datapoints'):
            print(f"  First datapoint structure: {list(response['Datapoints'][0].keys())}")
            print(f"  Sample datapoint: {response['Datapoints'][0]}")
        else:
            print("  No datapoints in response")
            
        # Print full response for debugging
        print(f"\nFull response (truncated):")
        print(json.dumps(response, indent=2, default=str)[:1000] + "...")
        
    except Exception as e:
        print(f"❌ Error in raw response test: {e}")
        import traceback
        traceback.print_exc()

    # Test 5: Try much longer time ranges and different approaches
    print(f"\n{'='*60}")
    print("TEST 5: Extended time ranges and different approaches")
    print(f"{'='*60}")
    
    # Try much longer time ranges
    extended_periods = [
        (180, "6 months"),
        (365, "1 year"),
        (730, "2 years")
    ]
    
    for period_days, period_name in extended_periods:
        print(f"\nTesting {period_name}:")
        test_end = datetime.utcnow()
        test_start = test_end - timedelta(days=period_days)
        
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName='Requests',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ],
                StartTime=test_start,
                EndTime=test_end,
                Period=86400,
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                total_requests = sum(point['Sum'] for point in response['Datapoints'])
                print(f"  ✓ Found {len(response['Datapoints'])} datapoints")
                print(f"  ✓ Total requests: {int(total_requests):,}")
                
                # Show oldest and newest datapoints
                sorted_points = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
                if sorted_points:
                    print(f"  Oldest data: {sorted_points[0]['Timestamp'].strftime('%Y-%m-%d')}: {int(sorted_points[0]['Sum']):,}")
                    print(f"  Newest data: {sorted_points[-1]['Timestamp'].strftime('%Y-%m-%d')}: {int(sorted_points[-1]['Sum']):,}")
                break  # Found data, no need to try longer periods
            else:
                print(f"  ❌ No datapoints found for {period_name}")
                
        except Exception as e:
            print(f"  ❌ Error getting {period_name} data: {e}")
    
    # Test 6: Try different statistics
    print(f"\n{'='*60}")
    print("TEST 6: Try different statistics and metrics")
    print(f"{'='*60}")
    
    test_end = datetime.utcnow()
    test_start = test_end - timedelta(days=90)
    
    stats_to_try = ['Sum', 'Average', 'Maximum', 'Minimum', 'SampleCount']
    
    for stat in stats_to_try:
        print(f"\nTrying {stat} statistic:")
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName='Requests',
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ],
                StartTime=test_start,
                EndTime=test_end,
                Period=86400,
                Statistics=[stat]
            )
            
            if response['Datapoints']:
                print(f"  ✓ Found {len(response['Datapoints'])} datapoints with {stat}")
                if response['Datapoints']:
                    sample_point = response['Datapoints'][0]
                    print(f"  Sample value: {sample_point.get(stat, 'N/A')}")
            else:
                print(f"  ❌ No datapoints with {stat}")
                
        except Exception as e:
            print(f"  ❌ Error with {stat}: {e}")
    
    # Test 7: Try other metrics that might have data
    print(f"\n{'='*60}")
    print("TEST 7: Try other metrics")
    print(f"{'='*60}")
    
    other_metrics = ['BytesDownloaded', 'BytesUploaded', '4xxErrorRate', '5xxErrorRate']
    
    for metric_name in other_metrics:
        print(f"\nTrying {metric_name}:")
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/CloudFront',
                MetricName=metric_name,
                Dimensions=[
                    {
                        'Name': 'DistributionId',
                        'Value': distribution_id
                    }
                ],
                StartTime=test_start,
                EndTime=test_end,
                Period=86400,
                Statistics=['Sum'] if 'Bytes' in metric_name else ['Average']
            )
            
            if response['Datapoints']:
                print(f"  ✓ Found {len(response['Datapoints'])} datapoints for {metric_name}")
                if response['Datapoints']:
                    stat_key = 'Sum' if 'Bytes' in metric_name else 'Average'
                    total = sum(point[stat_key] for point in response['Datapoints'])
                    print(f"  Total/Average: {total}")
            else:
                print(f"  ❌ No datapoints for {metric_name}")
                
        except Exception as e:
            print(f"  ❌ Error with {metric_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Test specific CloudFront distribution metrics')
    parser.add_argument('--profile', help='AWS profile name')
    parser.add_argument('--distribution-id', default='E2SG8ZFUZOH8GZ', help='Distribution ID to test')
    parser.add_argument('--days', type=int, default=30, help='Number of days to test')
    
    args = parser.parse_args()
    
    test_distribution_metrics(args.distribution_id, args.profile, args.days)

if __name__ == "__main__":
    main()