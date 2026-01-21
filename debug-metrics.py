#!/usr/bin/env python3
"""
Debug script to test CloudWatch metrics access for CloudFront
"""

import boto3
from datetime import datetime, timedelta
import argparse

def test_cloudwatch_access(profile_name=None, distribution_id=None):
    """Test CloudWatch access and metrics availability"""
    session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
    cloudfront = session.client('cloudfront')
    
    # Check current region
    current_region = session.region_name or 'us-east-1'
    print(f"Current AWS session region: {current_region}")
    
    # CloudFront metrics are only available in us-east-1 - ALWAYS use us-east-1
    print("🔧 Forcing CloudWatch client to use us-east-1 region for CloudFront metrics")
    cloudwatch = session.client('cloudwatch', region_name='us-east-1')
    print("✓ Using us-east-1 region for CloudFront metrics")
    
    print("\nTesting CloudWatch access...")
    
    # Test basic CloudWatch permissions
    try:
        response = cloudwatch.list_metrics(Namespace='AWS/CloudFront')
        print("✓ Basic CloudWatch access confirmed")
    except Exception as e:
        print(f"❌ Basic CloudWatch access failed: {e}")
        return
    
    # Get a distribution ID if not provided
    if not distribution_id:
        try:
            response = cloudfront.list_distributions()
            if response['DistributionList'].get('Items'):
                distribution_id = response['DistributionList']['Items'][0]['Id']
                print(f"✓ Using first distribution for testing: {distribution_id}")
            else:
                print("❌ No distributions found")
                return
        except Exception as e:
            print(f"❌ Error listing distributions: {e}")
            return
    
    # Test CloudFront metrics namespace access
    try:
        response = cloudwatch.list_metrics(
            Namespace='AWS/CloudFront'
        )
        
        if response['Metrics']:
            print(f"✓ CloudFront metrics namespace accessible - found {len(response['Metrics'])} metrics")
        else:
            print("⚠️  CloudFront metrics namespace accessible but no metrics found")
            print("   This could mean:")
            print("   - No CloudFront distributions have generated metrics yet")
            print("   - All distributions are very new (< 24 hours)")
            print("   - No traffic to any distributions")
            
    except Exception as e:
        print(f"❌ Cannot access CloudFront metrics namespace: {e}")
        print("   Possible causes:")
        print("   - Insufficient CloudWatch permissions")
        print("   - Wrong region (must be us-east-1)")
        return
    
    # Test metrics for specific distribution
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=30)
    
    print(f"\nTesting metrics for distribution {distribution_id}")
    print(f"Time range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    
    # Check if this distribution has any metrics at all
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
            print(f"✓ Distribution has {len(response['Metrics'])} available metric types:")
            for metric in response['Metrics']:
                print(f"   - {metric['MetricName']}")
        else:
            print("⚠️  No metrics found for this distribution")
            print("   This means:")
            print("   - Distribution has never received traffic")
            print("   - Distribution is very new (< 24-48 hours)")
            print("   - Distribution is disabled")
            return
            
    except Exception as e:
        print(f"❌ Error checking distribution metrics: {e}")
        return
    
    # Test actual metric data retrieval
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
        
        if response['Datapoints']:
            total_requests = sum(point['Sum'] for point in response['Datapoints'])
            print(f"✓ Found {len(response['Datapoints'])} days of request data")
            print(f"✓ Total requests in 30 days: {int(total_requests):,}")
            
            # Show recent activity
            sorted_points = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)
            print("   Recent activity:")
            for point in sorted_points[:5]:
                print(f"     {point['Timestamp'].strftime('%Y-%m-%d')}: {int(point['Sum']):,} requests")
                
        else:
            print("⚠️  Distribution has metric definitions but no actual data")
            print("   This indicates:")
            print("   - Distribution exists but has received no traffic")
            print("   - All traffic occurred outside the 30-day window")
            
    except Exception as e:
        print(f"❌ Error retrieving metric data: {e}")
    
    # Test permissions summary
    print(f"\n{'='*50}")
    print("CLOUDWATCH VERIFICATION SUMMARY")
    print(f"{'='*50}")
    
    try:
        # Test if we can access CloudWatch at all
        cloudwatch.list_metrics(Namespace='AWS/CloudFront')
        print("✓ CloudWatch access: WORKING")
    except:
        print("❌ CloudWatch access: FAILED")
    
    try:
        # Test CloudFront namespace
        response = cloudwatch.list_metrics(Namespace='AWS/CloudFront')
        print("✓ CloudFront metrics namespace: ACCESSIBLE")
    except:
        print("❌ CloudFront metrics namespace: BLOCKED")
    
    try:
        # Test specific distribution metrics
        response = cloudwatch.list_metrics(
            Namespace='AWS/CloudFront',
            Dimensions=[{'Name': 'DistributionId', 'Value': distribution_id}]
        )
        if response['Metrics']:
            print("✓ Distribution metrics: AVAILABLE")
        else:
            print("⚠️  Distribution metrics: NO DATA")
    except:
        print("❌ Distribution metrics: ACCESS FAILED")

def main():
    parser = argparse.ArgumentParser(description='Debug CloudWatch metrics access')
    parser.add_argument('--profile', help='AWS profile name')
    parser.add_argument('--distribution-id', help='Specific distribution ID to test')
    
    args = parser.parse_args()
    
    test_cloudwatch_access(args.profile, args.distribution_id)

if __name__ == "__main__":
    main()