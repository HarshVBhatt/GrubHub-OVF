# Step 2: S3 Buckets Setup Guide

## Overview
Create S3 buckets for data storage with proper structure and permissions.

## Prerequisites
- AWS Console access with S3 permissions
- Completed IAM roles setup (Step 1)

## Steps

### 1. Create Data Lake Bucket

1. **Navigate to S3 Console**
   - Go to AWS Console → S3
   - Click "Create bucket"

2. **Configure bucket**
   - Bucket name: `grubhub-ovf-data-lake`
   - Region: Choose your preferred region
   - Copy settings from: Leave empty

3. **Configure options**
   - Versioning: Disable (to save storage)
   - Encryption: Enable (SSE-S3)
   - Tags: 
     - Key: `Project`, Value: `OVF`
     - Key: `Environment`, Value: `Development`

4. **Set permissions**
   - Block all public access: Keep enabled (checked)
   - Click "Create bucket"

### 2. Create Features Bucket

Repeat the above process with:
- Bucket name: `grubhub-ovf-features`
- Same region as data lake bucket
- Same tags and settings

### 3. Create Forecasts Bucket

Repeat the above process with:
- Bucket name: `grubhub-ovf-forecasts`
- Same region as data lake bucket
- Same tags and settings

### 4. Create Folder Structure

For each bucket, create the necessary folders:

1. **grubhub-ovf-features bucket:**
   - Click on the bucket name
   - Click "Create folder"
   - Create these folders:
     - `raw/` - For raw data files
     - `scripts/` - For EMR bootstrap scripts
     - `logs/` - For EMR logs
     - `config/` - For configuration files
     - `hive-metastore/` - For PrestoDB metadata

2. **grubhub-ovf-features bucket:**
   - Create these folders:
     - `features/` - For processed features
     - `models/` - For trained models
     - `models/prophet/`
     - `models/sarimax/`

3. **grubhub-ovf-forecasts bucket:**
   - Create folder:
     - `forecasts/` - For prediction outputs

### 5. Configure Bucket Policies (Optional)

For additional security, add bucket policies:

1. **Select bucket** → Properties → Permissions → Bucket Policy
2. **Add policy** (replace YOUR-ACCOUNT-ID):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEMRAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::YOUR-ACCOUNT-ID:role/EMR_EC2_DefaultRole"
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::ovf-data-lake-demo-*/*",
        "arn:aws:s3:::ovf-data-lake-demo-*"
      ]
    }
  ]
}
```

### 7. (Optional - cost optimization) Set up Lifecycle Rules

1. **Select bucket** → Management → Lifecycle rules
2. **Create rule:**
   - Rule name: `delete-old-logs`
   - Scope: `logs/`
   - Transitions: None
   - Expiration: Delete after 7 days

3. **For forecasts bucket:**
   - Rule name: `archive-old-forecasts`
   - Scope: `forecasts/`
   - Transitions: Move to Glacier after 30 days
   - Expiration: Delete after 90 days

## Verification Checklist

- [ ] All three buckets created in same region
- [ ] Bucket names are globally unique
- [ ] Folder structure created in each bucket
- [ ] Encryption enabled
- [ ] Public access blocked
- [ ] Tags added for cost tracking
- [ ] Note down bucket names for configuration files


## Cost Optimization Tips
- Delete old files regularly
- Use lifecycle rules
- Monitor bucket metrics
- Consider Intelligent-Tiering for long-term data

## Next Steps

Proceed to `03_setup_local_environment.md` to set up your local development environment.