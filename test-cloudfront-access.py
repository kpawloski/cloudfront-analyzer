#!/usr/bin/env python3
"""
Simple test script to diagnose CloudFront API access issues
"""

import boto3
import json
import argparse
from datetime import datetime

def test_cloudfront_access(profile_name=None):
    """Test basic CloudFront API access"""
    print("Testing CloudFront API access...")
    
    try:
        # Initialize session and client
        if profile_name:
            print(f"Using AWS profile: {profile_name}")
            session = boto3.Session(profile_name=profile_name)
        else:
            print("Using default AWS credentials")
            session = boto3.Session()
        
        # Get region info
        region = session.region_name or 'default'
        print(f"Session region: {region}")
        
        # Test credentials
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print(f"✓ AWS Identity confirmed:")
        print(f"  Account: {identity.get('Account', 'Unknown')}")
        print(f"  User/Role: {identity.get('Arn', 'Unknown')}")
        
    except Exception as e:
        print(f"❌ AWS credentials/session error: {e}")
        return False
    
    try:
        # Test CloudFront client creation
        cloudfront = session.client('cloudfront')
        print("✓ CloudFront client created successfully")
        
    except Exception as e:
        print(f"❌ CloudFront client creation failed: {e}")
        return False
    
    try:
        # Test basic CloudFront API call
        print("\nTesting CloudFront list_distributions API call...")
        response = cloudfront.list_distributions()
        
        print("✓ list_distributions API call successful")
        print(f"Response keys: {list(response.keys())}")
        
        # Check distribution list structure
        dist_list = response.get('DistributionList', {})
        print(f"DistributionList keys: {list(dist_list.keys())}")
        
        # Check for distributions
        items = dist_list.get('Items', [])
        print(f"Number of distributions found: {len(items)}")
        
        if len(items) > 0:
            print("✓ Distributions found!")
            print("First few distributions:")
            for i, dist in enumerate(items[:3]):
                print(f"  {i+1}. ID: {dist.get('Id', 'Unknown')}")
                print(f"     Domain: {dist.get('DomainName', 'Unknown')}")
                print(f"     Status: {dist.get('Status', 'Unknown')}")
                print(f"     Enabled: {dist.get('Enabled', 'Unknown')}")
        else:
            print("⚠️  No distributions found in response")
            print("This could mean:")
            print("  - No CloudFront distributions exist in this account")
            print("  - Insufficient permissions to list distributions")
            print("  - API response structure changed")
        
        return len(items) > 0
        
    except Exception as e:
        print(f"❌ CloudFront API call failed: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Check for common permission errors
        error_str = str(e).lower()
        if 'access denied' in error_str or 'unauthorized' in error_str:
            print("\n🔍 This looks like a permissions issue.")
            print("Required permission: cloudfront:ListDistributions")
        elif 'invalid' in error_str:
            print("\n🔍 This might be a credential or region issue.")
        
        return False

def test_paginator():
    """Test the paginator approach used in the main script"""
    print(f"\n{'='*50}")
    print("TESTING PAGINATOR APPROACH")
    print(f"{'='*50}")
    
    try:
        session = boto3.Session()
        cloudfront = session.client('cloudfront')
        
        print("Creating paginator...")
        paginator = cloudfront.get_paginator('list_distributions')
        print("✓ Paginator created successfully")
        
        print("Testing paginator iteration...")
        distributions = []
        page_count = 0
        
        for page in paginator.paginate():
            page_count += 1
            print(f"Processing page {page_count}")
            print(f"Page keys: {list(page.keys())}")
            
            if 'DistributionList' in page:
                dist_list = page['DistributionList']
                print(f"DistributionList keys: {list(dist_list.keys())}")
                
                if 'Items' in dist_list:
                    items = dist_list['Items']
                    print(f"Found {len(items)} distributions in this page")
                    distributions.extend(items)
                else:
                    print("No 'Items' key in DistributionList")
            else:
                print("No 'DistributionList' key in page")
        
        print(f"\nPaginator results:")
        print(f"Total pages processed: {page_count}")
        print(f"Total distributions found: {len(distributions)}")
        
        return len(distributions) > 0
        
    except Exception as e:
        print(f"❌ Paginator test failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Test CloudFront API access')
    parser.add_argument('--profile', help='AWS profile name')
    
    args = parser.parse_args()
    
    print("CloudFront API Access Diagnostic")
    print("=" * 40)
    
    # Test basic access
    basic_success = test_cloudfront_access(args.profile)
    
    if basic_success:
        # Test paginator if basic access works
        paginator_success = test_paginator()
        
        if not paginator_success:
            print("\n❌ Paginator approach failed but basic API works")
            print("This suggests an issue with the pagination logic")
    else:
        print("\n❌ Basic CloudFront access failed")
        print("Fix the basic access issue before testing pagination")
    
    print(f"\n{'='*50}")
    print("DIAGNOSTIC SUMMARY")
    print(f"{'='*50}")
    print(f"Basic CloudFront access: {'✓ WORKING' if basic_success else '❌ FAILED'}")
    if basic_success:
        print(f"Paginator approach: {'✓ WORKING' if paginator_success else '❌ FAILED'}")

if __name__ == "__main__":
    main()