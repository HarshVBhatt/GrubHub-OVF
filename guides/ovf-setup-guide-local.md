# Step 3: Local Environment Setup Guide

## Overview
Set up your local development environment with Python, AWS CLI, and project structure.

## Prerequisites
- Computer with Python 3.8+ installed
- Administrator/sudo access
- Completed S3 buckets setup (Step 2)

## Steps

### 1. Install Python (If not already installed)

### 2. Install AWS CLI

**All Platforms:**
1. Visit https://aws.amazon.com/cli/
2. Download installer for your OS
3. Run installer

**Verify installation:**
```bash
aws --version
python3 --version
pip3 --version
```

### 3. Configure AWS Credentials

1. **Get your credentials:**
   - AWS Console → Your username (top right) → Security credentials
   - Create new access key
   - Download credentials

2. **Configure AWS CLI:**
```bash
aws configure
```
Enter:
- AWS Access Key ID: [your-access-key]
- AWS Secret Access Key: [your-secret-key]
- Default region name: [same region as S3 buckets]
- Default output format: json

3. **Test connection:**
```bash
aws s3 ls
```
You should see your buckets listed.

### 4. Create Project Structure

```bash
# Create main directory
mkdir grubhub-ovf
cd grubhub-ovf

# Create subdirectories
mkdir -p src/{models,features,utils}
mkdir -p config

# Create empty __init__.py files
touch src/__init__.py
touch src/models/__init__.py
touch src/features/__init__.py
touch src/utils/__init__.py
```

### 5. Set Up Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On Mac/Linux:
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip
```

### 6. Install Python Dependencies

Install dependencies:
```bash
pip install -r requirements.txt
```

### 7. Create Configuration File

Create `config/config.py`:

Create `.env` file:
```bash
AWS_REGION="<region-name>"
S3_BUCKET_DATA="grubhub-ovf-data-lake"
S3_BUCKET_FEATURES="grubhub-ovf-features"
S3_BUCKET_FORECASTS="grubhub-ovf-forecasts"
EMR_KEY_NAME=grubhub-ovf-emr-key
```

### 9. (Optional - test setup) Create Utility Scripts

Create `test_setup.py`:
```python
#!/usr/bin/env python3
"""Test the local setup"""

import sys
import boto3

def test_python():
    print(f"✓ Python {sys.version}")

def test_aws():
    try:
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        print(f"✓ AWS configured, found {len(buckets['Buckets'])} buckets")
    except Exception as e:
        print(f"✗ AWS error: {e}")

def test_imports():
    try:
        import pandas
        import prophet
        import dask
        print("✓ All key packages installed")
    except ImportError as e:
        print(f"✗ Import error: {e}")

if __name__ == "__main__":
    print("Testing local setup...")
    test_python()
    test_aws()
    test_imports()
    print("\nSetup test complete!")
```

Run test:
```bash
python test_setup.py
```

### 10. (Optional) Create Git Repository


## Verification Checklist

- [ ] Python 3.8+ installed
- [ ] AWS CLI installed and configured
- [ ] Can list S3 buckets with `aws s3 ls`
- [ ] Project directories created
- [ ] Virtual environment activated
- [ ] All Python packages installed

## Next Steps

Proceed to `04_setup_emr_security.md` to configure security groups for EMR.

## Troubleshooting

### "pip: command not found"
```bash
python3 -m pip install --upgrade pip
```
