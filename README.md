# CloudFront Unused Distribution Analyzer

This script helps identify potentially unused or idle CloudFront distributions by analyzing their traffic patterns, configuration, and other indicators.

## Features

- **Traffic Analysis**: Examines CloudWatch metrics over the past 30 days (configurable)
- **Configuration Review**: Checks distribution status, origins, and settings
- **Pattern Detection**: Identifies test/staging environments based on naming patterns
- **Comprehensive Reporting**: Provides detailed analysis in JSON or text format

## Usage Indicators

The script identifies distributions as potentially unused based on:

- **Zero Traffic**: No requests in the specified time period
- **Low Traffic**: Very few requests (< 100 in 30 days)
- **Disabled Status**: Distribution is disabled
- **Stale Distributions**: Not modified in 90+ days with minimal traffic
- **Test Environments**: Contains keywords like 'test', 'staging', 'dev', 'demo'

## Prerequisites

```bash
pip install -r requirements.txt
```

**Required AWS Permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "cloudfront:ListDistributions",
        "cloudfront:GetDistribution",
        "cloudwatch:GetMetricStatistics"
      ],
      "Resource": "*"
    }
  ]
}
```

## Usage Examples

**Basic analysis:**
```bash
python cloudfront-unused-analyzer.py
```

**With specific AWS profile:**
```bash
python cloudfront-unused-analyzer.py --profile production
```

**JSON output to file:**
```bash
python cloudfront-unused-analyzer.py --output json --output-file unused-distributions.json
```

**Analyze last 60 days:**
```bash
python cloudfront-unused-analyzer.py --days 60
```

**Get only completely broken distribution IDs (100% error rate):**
```bash
python cloudfront-unused-analyzer.py --broken
```

**Save broken distribution IDs to file for automation:**
```bash
python cloudfront-unused-analyzer.py --broken > broken-distributions.txt
```

**Use broken distributions in a script:**
```bash
# Get broken distribution IDs and disable them
python cloudfront-unused-analyzer.py --broken | while read dist_id; do
    echo "Disabling broken distribution: $dist_id"
    # aws cloudfront get-distribution-config --id $dist_id > config.json
    # ... modify config to set Enabled=false ...
    # aws cloudfront update-distribution --id $dist_id --distribution-config file://config.json
done
```

## Sample Output

```
CloudFront Distribution Usage Analysis
=====================================
Analysis Date: 2026-01-08T10:30:00
Total Distributions: 15
Potentially Unused: 4
Active Distributions: 11

Summary:
- Disabled: 1
- Zero Traffic (30 days): 2
- Low Traffic (<100 requests): 1
- Test/Staging Environments: 2

Potentially Unused Distributions:
================================

Distribution ID: E1234567890ABC
Domain: d1234567890abc.cloudfront.net
Status: Deployed | Enabled: False
Last Modified: 2025-08-15T14:22:00
Requests (30d): 0
Data Downloaded (30d): 0 bytes
Unused Indicators:
  - Distribution is disabled
  - Zero requests in past 30 days
```

## Cost Optimization

Use this script to:
- Identify distributions that can be safely deleted
- Find test environments that should be cleaned up
- Locate distributions with minimal traffic that might be consolidated
- Review disabled distributions that are still incurring costs

## Limitations

- Requires CloudWatch metrics (may have delays)
- Cannot detect internal/private usage patterns
- Test environment detection is based on naming patterns only
- Metrics availability depends on AWS retention policies

## Safety Notes

- Always verify findings before deleting distributions
- Consider business requirements and seasonal traffic patterns
- Test environments might be needed for specific projects
- Some distributions might be used for disaster recovery scenarios