# Step 1: IAM Roles Setup Guide

## Overview
Create the necessary IAM roles for EMR to access S3 and other AWS services.

## Prerequisites
- AWS Console access with IAM permissions
- Understanding of your AWS account ID

## Steps

### 1. Create EMR Default Service Role

1. **Navigate to IAM Console**
   - Go to AWS Console → IAM → Roles
   - Click "Create role"

2. **Select trusted entity**
   - Choose "AWS service"
   - Select "EMR" from the list
   - Click "Next"

3. **Attach permissions**
   - The policy `AmazonElasticMapReduceRole` should be automatically selected
   - Click "Next"

4. **Name the role**
   - Role name: `EMR_DefaultRole`
   - Description: "Default service role for EMR clusters"
   - Click "Create role"

### 2. Create EMR EC2 Instance Profile

1. **Create another role**
   - Click "Create role" again
   - Choose "AWS service"
   - Select "EC2" (not EMR this time)
   - Under "Select your use case", choose "EC2"
   - Click "Next"

2. **Attach permissions**
   - Search and select these policies:
     - `AmazonElasticMapReduceforEC2Role`
     - `AmazonS3FullAccess` (or create custom policy for specific buckets)
   - Click "Next"

3. **Name the role**
   - Role name: `EMR_EC2_DefaultRole`
   - Description: "EC2 instance profile for EMR nodes"
   - Click "Create role"

### 3. (Optional) If familiar with AWS policies - Create Custom S3 Access Policy for more secure access
If you want to limit S3 access to specific buckets:

1. **Go to Policies**
   - IAM → Policies → Create policy

2. **Use JSON editor**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::ovf-data-lake-demo/*",
           "arn:aws:s3:::ovf-features-demo/*",
           "arn:aws:s3:::ovf-forecasts-demo/*",
           "arn:aws:s3:::ovf-data-lake-demo",
           "arn:aws:s3:::ovf-features-demo",
           "arn:aws:s3:::ovf-forecasts-demo"
         ]
       }
     ]
   }
   ```

3. **Name the policy**
   - Name: `OVF-S3-Access-Policy`
   - Attach this to `EMR_EC2_DefaultRole` instead of `AmazonS3FullAccess`

### 4. Verify Roles

1. **Check EMR_DefaultRole**
   - Go to IAM → Roles
   - Search for `EMR_DefaultRole`
   - Verify Trust relationships show:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "elasticmapreduce.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         }
       ]
     }
     ```

2. **Check EMR_EC2_DefaultRole**
   - Verify Trust relationships show:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "ec2.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         }
       ]
     }
     ```

### 5. Create EC2 Key Pair (If not exists)

1. **Go to EC2 Console**
   - EC2 → Key Pairs → Create key pair

2. **Create key**
   - Name: `grubhub-ovf-emr-key`
   - Type: RSA
   - Format: .pem (for Mac/Linux) or .ppk (for Windows)
   - Click "Create key pair"
   - **Save the downloaded file securely!**

## Verification Checklist

- [ ] EMR_DefaultRole created with correct trust policy
- [ ] EMR_EC2_DefaultRole created with correct trust policy
- [ ] S3 access permissions attached to EMR_EC2_DefaultRole
- [ ] EC2 key pair created and saved
- [ ] Roles appear in IAM → Roles list

## Next Steps

Proceed to `02_setup_s3_buckets.md` to create the S3 buckets.

## Troubleshooting

### Missing policies
- Some policies might have different names in different regions
- Search for "EMR" or "ElasticMapReduce" in policy search
