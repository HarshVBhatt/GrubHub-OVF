# Step 6: Running the Pipeline Guide

## Overview
Execute the OVF pipeline step-by-step.

## Prerequisites
- All previous setup steps completed
- Data file (`all_data_cleaned.csv`) in place
- AWS services configured
- Python environment activated

## Step 1: Setting up the raw data

Download the dataset from: https://www.kaggle.com/datasets/gauravmalik26/food-delivery-dataset/

Copy it to grubhub-ovf directory

Run the raw_data_setup.ipynb. It is meant to be interactive to give you a very detailed look at how we are transforming the data to make it ready for usage.

Make sure the data and metadata is correctly set up in s3 before proceeding.

## Step 2: Create and Start EMR Cluster

Follow the instructions in setup-guide-iam-emr to correctly set up your iam roles and emr clusters

Wait for the EMR cluster status to change to "Waiting" before proceeding

## Step 3: Setup dask

Execute the following commands in terminal

### 1. SSH to master

```bash
ssh -i grubhub-ovf-emr-key.pem hadoop@<master-dns>
```

### 2. Download and run startup script in master node

```bash
aws s3 cp s3://grubhub-ovf-data-lake/scripts/startup_dask_cluster.sh .
chmod +x startup_dask_cluster.sh
./startup_dask_cluster.sh
exit
```

### 3. SSH to workers
```bash
ssh -i grubhub-ovf-emr-key.pem hadoop@<worker1-dns>
nohup dask-worker <scheduler-ip> --nthreads 4 --memory-limit 14GB > ~/dask-worker.log 2>&1 &
exit
```

```bash
ssh -i grubhub-ovf-emr-key.pem hadoop@<worker2-dns>
nohup dask-worker <scheduler-ip> --nthreads 4 --memory-limit 14GB > ~/dask-worker.log 2>&1 &
exit
```

Your Dask should now be setup and ready to run distributed training and forecasting

## Step 4. Staging code files

Execute the following commands in terminal

### 1. SSH to master

```bash
ssh -i grubhub-ovf-emr-key.pem hadoop@<master-dns>
```

### 2. Download code files

```bash
aws s3 cp s3://grubhub-ovf-data-lake/src/feature_engineering.py .
aws s3 cp s3://grubhub-ovf-data-lake/src/model_training.py .
aws s3 cp s3://grubhub-ovf-data-lake/src/ensemble_forecasting.py .
```

### 3. Stage the files

```bash
chmod +x feature_engineering.py
chmod +x model_training.py
chmod +x ensemble_forecasting.py
```
Thats your setup completed!

## Step 3: Create Features

```bash
# Wait for cluster to be fully ready (2-3 minutes after previous step)
python feature_engineering.py
```

Expected output:
```
Loading data from S3...
Connected to PrestoDB at ec2-xx-xx-xx-xx.compute-1.amazonaws.com
Creating features with PrestoDB...
Created 75 features for 3,456 records
✅ Features saved to S3
```

## Step 4: Train Models in Parallel

```bash
python model_training.py

# If auto-detection fails
python model_training.py <master-dns>:8786
```

Expected output:
```
Connected to Dask cluster at ec2-xx-xx-xx-xx:8786
Workers available: 3
Submitting Prophet training to Worker 1...
Submitting SARIMAX training to Worker 2...

✅ Model training complete!
Total models trained: 8 (4 regions × 2 model types)
```

## Step 5: Generate Forecasts

```bash
python ensemble_forecasting.py
```

Expected output:
```
Generating 72-hour forecasts...
Worker 1: Generated Prophet forecasts
Worker 2: Generated SARIMAX forecasts

✅ Forecasting complete!
Generated 2,160 predictions (30 regions × 72 hours)
```

## Step 6: Terminate EMR Cluster (CRITICAL!)


## Step 7. (Optional)  Viewing Results

### Check Forecast Quality

Create `view_results.py`:
```python
import pandas as pd
import matplotlib.pyplot as plt
import boto3

# Download latest forecast
s3 = boto3.client('s3')
s3.download_file('ovf-forecasts-demo-username', 
                 'forecasts/latest_forecast.parquet',
                 'forecast.parquet')

# Load and visualize
df = pd.read_parquet('forecast.parquet')

# Plot sample region
region_1 = df[df['Region'] == 1]
plt.figure(figsize=(12, 6))
plt.plot(region_1['timestamp'], region_1['ensemble_forecast'])
plt.title('72-Hour Forecast for Region 1')
plt.xlabel('Time')
plt.ylabel('Order Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('forecast_sample.png')
print("Forecast plot saved as forecast_sample.png")

# Summary statistics
print("\nForecast Summary:")
print(f"Total predictions: {len(df)}")
print(f"Average hourly orders: {df['ensemble_forecast'].mean():.1f}")
print(f"Peak hour: {df.loc[df['ensemble_forecast'].idxmax(), 'timestamp']}")
print(f"Peak orders: {df['ensemble_forecast'].max()}")
```

## Troubleshooting

### EMR Cluster Issues

**"Cluster failed to start"**
- Check EMR_DefaultRole exists
- Verify subnet and security groups
- Check EC2 key pair name

**"Cannot connect to Dask"**
- Wait 2-3 minutes after cluster ready
- Check security group allows port 8786
- Try master private DNS instead

### Model Training Failures

**"No module named prophet"**
- Check EMR bootstrap script ran
- SSH to cluster and verify packages

**"Workers not found"**
- Ensure all 3 worker nodes are running
- Check Dask scheduler logs


### Quick Fixes

```bash
# Force terminate all EMR clusters
aws emr list-clusters --active --query 'Clusters[*].Id' --output text | \
  xargs -n 1 aws emr terminate-clusters --cluster-ids

# Clean up old S3 data
aws s3 rm s3://grubhub-ovf-data-lake/logs/ --recursive
aws s3 rm s3://grubhub-ovf-data-lake/forecasts/ --recursive \
  --exclude "*" --include "*.parquet" --exclude "*latest*"

# PrestoDB table drop access denied
aws s3 cp s3://grubhub-ovf-data-lake/scripts/disable_hive_auth.sh
chmod +x disable_hive_auth.sh
./disable_hive_auth.sh
```

## Success Checklist

- [ ] Data successfully loaded to S3
- [ ] EMR cluster started and Dask connected
- [ ] Features created
- [ ] Forecasts generated for all regions
- [ ] EMR cluster terminated
- [ ] Results available in S3

## Next Steps

- Schedule daily runs with Airflow
- Analyze forecast accuracy
- Tune model parameters
- Add more regions or features
- Set up monitoring dashboards