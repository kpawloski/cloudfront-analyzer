#!/usr/bin/env python3
"""
Test CloudWatch time handling and timezone issues
"""

import boto3
from datetime import datetime, timedelta, timezone
import argparse

def test_cloudwatch_time(distribution_id="E2SG8ZFUZOH8GZ", profile_name=None):
    """Test different time approaches with CloudWatch"""
    
    print("Testing CloudWatch time handling")
    print("=" * 50)
    
    # Initialize session
    session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
    cloudwatch = session.client('cloudwatch', region_name='us-east-1')
    
    # Test different time approaches
    now_utc = datetime.utcnow()
    now_with_tz = datetime.now(timezone.utc)
    
    print(f"Current time (utcnow): {now_utc}")
    print(f"Current time (with UTC tz): {now_with_tz}")
    
    # Test 1: Try with explicit UTC timezone
    print(f"\n{'='*50}")
    print("TEST 1: Using explicit UTC timezone")
    print(f"{'='*50}")
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=30)
    
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    
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
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=['Sum']
        )
        
        print(f"Datapoints found: {len(response['Datapoints'])}")
        if response['Datapoints']:
            total = sum(point['Sum'] for point in response['Datapoints'])
            print(f"Total requests: {int(total):,}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: Try going back much further
    print(f"\n{'='*50}")
    print("TEST 2: Going back 2 years")
    print(f"{'='*50}")
    
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=730)  # 2 years
    
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    
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
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=['Sum']
        )
        
        print(f"Datapoints found: {len(response['Datapoints'])}")
        if response['Datapoints']:
            total = sum(point['Sum'] for point in response['Datapoints'])
            print(f"Total requests: {int(total):,}")
            
            # Show date range of actual data
            sorted_points = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            print(f"Oldest data: {sorted_points[0]['Timestamp']}")
            print(f"Newest data: {sorted_points[-1]['Timestamp']}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 3: Check what metrics are actually available with timestamps
    print(f"\n{'='*50}")
    print("TEST 3: Check metric availability with list_metrics")
    print(f"{'='*50}")
    
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
        
        print(f"Available metrics: {len(response['Metrics'])}")
        for metric in response['Metrics']:
            print(f"  - {metric['MetricName']}")
            
        # For each metric, try to get at least one datapoint
        print(f"\nTesting each metric for any data (2 year window):")
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=730)
        
        for metric in response['Metrics']:
            metric_name = metric['MetricName']
            try:
                stat = 'Sum' if metric_name in ['Requests', 'BytesDownloaded', 'BytesUploaded'] else 'Average'
                
                metric_response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/CloudFront',
                    MetricName=metric_name,
                    Dimensions=metric['Dimensions'],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=[stat]
                )
                
                if metric_response['Datapoints']:
                    print(f"  ✓ {metric_name}: {len(metric_response['Datapoints'])} datapoints")
                    # Show date range
                    sorted_points = sorted(metric_response['Datapoints'], key=lambda x: x['Timestamp'])
                    print(f"    Range: {sorted_points[0]['Timestamp'].strftime('%Y-%m-%d')} to {sorted_points[-1]['Timestamp'].strftime('%Y-%m-%d')}")
                else:
                    print(f"  ❌ {metric_name}: No datapoints")
                    
            except Exception as e:
                print(f"  ❌ {metric_name}: Error - {e}")
        
    except Exception as e:
        print(f"Error listing metrics: {e}")

def main():
    parser = argparse.ArgumentParser(description='Test CloudWatch time handling')
    parser.add_argument('--profile', help='AWS profile name')
    parser.add_argument('--distribution-id', default='E2SG8ZFUZOH8GZ', help='Distribution ID to test')
    
    args = parser.parse_args()
    
    test_cloudwatch_time(args.distribution_id, args.profile)

if __name__ == "__main__":
    main()