# Step 5: Code Files Setup Guide

## Overview
Copy all Python code files to your project and configure them with your settings.

## Prerequisites
- Completed security group setup (Step 4)
- Local project structure created
- Your S3 bucket names and AWS settings

## Steps

### 1. Copy Core Python Files

Copy these files from the artifacts to your `src/` directory:

2. **feature_engineering.py** → `src/feature_engineering.py`
5. **model_training.py** → `src/model_parallel_training.py`
6. **ensemble_forecasting.py** → `src/ensemble_forecasting.py`


### 2. Update Configuration in Each File

Since we're using a centralized config.py file, most files will automatically use your settings once you create the config.py file. However, you should verify a few things:

#### A. Create config.py from template

```bash
# Copy the template
cp config.py.template config.py

# Edit with your values
nano config.py  # or vim, code, etc.
```

Update these values in config.py:
```python
# Your AWS Account ID
AWS_ACCOUNT_ID = '123456789012'  # Replace with your actual account ID

# Your EMR settings
EMR_KEY_NAME = 'ovf-emr-key'  # Your EC2 key pair name
EMR_SUBNET_ID = 'subnet-xxxxxxxx'  # From Step 4
EMR_MASTER_SG = 'sg-xxxxxxxx'  # Your master security group
EMR_SLAVE_SG = 'sg-xxxxxxxx'   # Your slave security group
```

#### B. Verify imports in Python files

Each Python file should have imports that look like this:
```python
try:
    from config import S3_BUCKET_DATA, EMR_KEY_NAME
    # Uses your config values
except ImportError:
    # Falls back to defaults if config.py not found
    print("Warning: Using default values. Create config.py")
```

No manual changes needed if you created config.py correctly!

### 3. Verify Configuration Module

Your config.py should look like this (with your actual values):
```python
# config.py
"""Central configuration for OVF system"""
import os
from datetime import datetime

# AWS Settings
AWS_REGION = 'us-east-1'
AWS_ACCOUNT_ID = '123456789012'  # Your actual account ID

# S3 Buckets (your actual bucket names)
S3_BUCKET_DATA = 'ovf-data-lake-demo-johndoe'
S3_BUCKET_FEATURES = 'ovf-features-demo-johndoe'
S3_BUCKET_FORECASTS = 'ovf-forecasts-demo-johndoe'

# EMR Settings (your actual values)
EMR_KEY_NAME = 'ovf-emr-key'
EMR_SUBNET_ID = 'subnet-1234abcd'
EMR_MASTER_SG = 'sg-1234efgh'
EMR_SLAVE_SG = 'sg-5678ijkl'
EMR_LOG_URI = f's3://{S3_BUCKET_DATA}/logs/'

# Model Settings
FORECAST_HORIZON_HOURS = 72
TRAIN_TEST_SPLIT = 0.8
N_REGIONS = 30

# File Paths
DATA_FILE = 'all_data_cleaned.csv'

# Feature Engineering
WEATHER_MAPPING = {
    'Clear': 0, 'Cloudy': 1, 'Fog': 2,
    'Sandstorms': 3, 'Stormy': 4, 'Windy': 5
}

TRAFFIC_MAPPING = {
    'Low': 0, 'Medium': 1, 'High': 2, 'Jam': 3
}

# Cost Monitoring
MAX_MONTHLY_HOURS = 750
HOUR_WARNING_THRESHOLD = 600
STORAGE_WARNING_GB = 4

# Timestamps
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
```

### 4. (Optional) Create Utility Functions

Create `src/utils/aws_utils.py`:
```python
"""AWS utility functions"""
import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_s3_bucket_exists(bucket_name):
    """Check if S3 bucket exists and is accessible"""
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"✓ Bucket {bucket_name} exists and is accessible")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.error(f"✗ Bucket {bucket_name} does not exist")
        else:
            logger.error(f"✗ Error accessing bucket {bucket_name}: {error_code}")
        return False

def check_iam_role_exists(role_name):
    """Check if IAM role exists"""
    iam = boto3.client('iam')
    try:
        iam.get_role(RoleName=role_name)
        logger.info(f"✓ IAM role {role_name} exists")
        return True
    except ClientError:
        logger.error(f"✗ IAM role {role_name} does not exist")
        return False

def get_ec2_key_pairs():
    """List available EC2 key pairs"""
    ec2 = boto3.client('ec2')
    response = ec2.describe_key_pairs()
    return [kp['KeyName'] for kp in response['KeyPairs']]

def validate_setup():
    """Validate AWS setup"""
    from config import (S3_BUCKET_DATA, S3_BUCKET_FEATURES, 
                       S3_BUCKET_FORECASTS, EMR_KEY_NAME)
    
    print("Validating AWS setup...")
    
    # Check S3 buckets
    buckets = [S3_BUCKET_DATA, S3_BUCKET_FEATURES, S3_BUCKET_FORECASTS]
    for bucket in buckets:
        check_s3_bucket_exists(bucket)
    
    # Check IAM roles
    roles = ['EMR_DefaultRole', 'EMR_EC2_DefaultRole']
    for role in roles:
        check_iam_role_exists(role)
    
    # Check key pair
    key_pairs = get_ec2_key_pairs()
    if EMR_KEY_NAME in key_pairs:
        logger.info(f"✓ Key pair {EMR_KEY_NAME} exists")
    else:
        logger.error(f"✗ Key pair {EMR_KEY_NAME} not found")
        logger.info(f"Available key pairs: {key_pairs}")

if __name__ == "__main__":
    validate_setup()
```

### 5. (Optional) Create Test Script

Create `test_code_setup.py`:
```python
#!/usr/bin/env python3
"""Test that all code files are properly set up"""

import os
import sys
sys.path.append('src')

def test_imports():
    """Test all imports work"""
    try:
        import feature_engineering
        import model_training
        import ensemble_forecasting
        from utils import aws_utils
        print("✓ All modules import successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_config():
    """Test configuration is set up"""
    try:
        from config import S3_BUCKET_DATA, EMR_KEY_NAME
        print(f"✓ Configuration loaded")
        print(f"  Data bucket: {S3_BUCKET_DATA}")
        print(f"  EMR key: {EMR_KEY_NAME}")
        return True
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return False

def test_aws_connection():
    """Test AWS utilities"""
    try:
        from utils.aws_utils import validate_setup
        validate_setup()
        return True
    except Exception as e:
        print(f"✗ AWS validation error: {e}")
        return False

def check_data_file():
    """Check if data file exists"""
    if os.path.exists('all_data_cleaned.csv'):
        print("✓ Data file found")
        return True
    else:
        print("✗ Data file 'all_data_cleaned.csv' not found")
        print("  Please copy your data file to the project root")
        return False

if __name__ == "__main__":
    print("Testing OVF code setup...")
    print("=" * 50)
    
    all_good = True
    all_good &= test_imports()
    all_good &= test_config()
    all_good &= check_data_file()
    all_good &= test_aws_connection()
    
    print("=" * 50)
    if all_good:
        print("✅ All tests passed! Ready to run pipeline.")
    else:
        print("❌ Some tests failed. Please fix issues above.")
```

## File Structure Check

Your project should now look like:
```
grubhub-ovf/
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── feature_engineering.py
│   ├── model_training.py
│   ├── ensemble_forecasting.py
│   └── utils/
│       ├── __init__.py
│       └── aws_utils.py
├── raw_data_setup.ipynb
├── all_data_cleaned.csv  # Your data file
├── requirements.txt
├── test_code_setup.py
└── venv/  # Virtual environment
```

## Running Tests

```bash
# Activate virtual environment
source venv/bin/activate  # Mac/Linux
# or
venv\Scripts\activate  # Windows

# Run tests
python test_code_setup.py
```

## Verification Checklist

- [ ] All Python files copied to correct locations
- [ ] Bucket names updated in all files
- [ ] EMR key name and subnet configured
- [ ] Security group IDs added to config
- [ ] Import statements updated
- [ ] Config.py created with your settings
- [ ] Test script runs successfully
- [ ] Data file present in project root

## NOTE

This setup for code files is a very loose guide, feel free to change things around as needed

## Next Steps

Proceed to `06_run_pipeline.md` to run the complete pipeline.
