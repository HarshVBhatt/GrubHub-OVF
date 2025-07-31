# Grubhub's Order Volume Forecasting System

## Overview

This is a minimal implementation of Grubhub's Order Volume Forecasting system designed to run entirely on AWS Free Tier. The system uses model parallelization where different workers train different model types (Prophet, SARIMAX) and ensembling is used to get the final volume forecast.

## Architecture

### Components
- **Data Storage**: S3 (unified data lake)
- **Query Engine**: PrestoDB
- **Compute**: EMR with 3 m5.xlarge instances (1 master + 2 workers)
- **Orchestration**: Apache Airflow
- **Distributed Computing**: Dask
- **Caching**: SQLite on each worker

### Model Distribution
- **Worker 1**: Prophet models for all regions
- **Worker 2**: SARIMAX models for all regions  

## Prerequisites

- Python 3.8+
- AWS Account with free tier eligibility
- AWS CLI configured with credentials
- Your data file: `all_data_cleaned.csv`

## Configuration

1. **Copy the configuration template:**
   ```bash
   cp config.py.template config.py
   ```

2. **Edit `config.py` with your values:**
   - AWS account ID
   - S3 bucket names (must be globally unique)
   - EMR key pair name
   - Subnet and security group IDs


## Quick Start

1. **Set up IAM, S3, local environment and security groups:**
   - Check out the setup guides for each:
      - IAM
      - S3
      - local
      - security
   - Make sure these are setup correctly

2. **Download data from Kaggle and set it up in S3:**
   - Download data from https://www.kaggle.com/datasets/gauravmalik26/food-delivery-dataset/
   - Run raw_data-setup.ipynb to push data to the s3 data lake

3. **Set up EMR cluster**
   - Check out the setup guide for EMR to start the cluster
   - This cluster is most likely to incur some amount of charge ($$), so please make sure you are ready for that

4. **Run the pipeline:**
   - Run the feature engineering, model training and ensemble forecasting inside the cluster
   - Check out the setup for run for explicit details

5. **Run the pipeline:**
   ```bash
   # Option 1: Via Airflow UI
   # Option 2: Manually
   ./run_pipeline.sh
   ```

## Cost Management

### Free Tier Usage
- **EC2**: 8 hours/day (4 instances × 2 hours) = 240 hours/month
- **S3**: < 1GB storage
- **Data Transfer**: < 1GB/day


### Auto-termination
EMR clusters automatically terminate after 2 hours to prevent overcharges.

## Data Schema

### Input Data
- `Region`: Integer - Region identifier
- `Order_Date`: String - Date of orders
- `order_hour`: Integer - Hour of day (0-23)
- `Weatherconditions`: String - Weather category (Clear, Cloudy, Fog, etc.)
- `Road_traffic_density`: String - Traffic level (Low, Medium, High, Jam)
- `Order Count`: Integer - Number of orders

### Feature Engineering
Categorical encoding is performed during feature engineering:
- Weather conditions → Numeric encoding (0-5) + One-hot encoding
- Traffic density → Numeric encoding (0-3) + One-hot encoding
- Additional 50+ engineered features including lags, moving averages, and interactions

## Pipeline Steps

1. **Data Loading**: Raw CSV → S3 Parquet (preserves categorical variables)
2. **EMR Cluster**: Spin up 3-node cluster with Dask
3. **Feature Engineering**: PrestoDB processes and encodes features
4. **Model Training**: Parallel training for 2 models across 2 workers
5. **Ensemble Forecasting**: Parallel forecasting with 2 models on 2 workers. Combine predictions from all models.
6. **Cleanup**: Terminate cluster and archive old data

## File Structure

```
ovf-system/
├── src/
│   ├── feature_engineering.py   # Feature creation with encoding
│   ├── model_parallel_training.py # Distributed model training
│   ├── ensemble_forecasting.py  # Generate predictions
├── requirements.txt            # Python dependencies
├── guides/           # detailed guides for each step
├── raw_data_setup.ipynb            # Setup data to S3
```

## Extending the System

### Add New Models
1. Create new training function in `model_parallel_training.py`
2. Add to worker task distribution
3. Update ensemble weights in `ensemble_forecasting.py`

### Scale to More Regions
1. Increase EMR cluster size (will exceed free tier)
2. Implement region batching
3. Use spot instances for workers

### Production Deployment
1. Use managed services (Redshift, SageMaker)
2. Implement model versioning
3. Add A/B testing framework
4. Set up monitoring dashboards

## Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review Airflow UI for task failures
3. Monitor AWS CloudWatch for EMR logs
4. Ensure all services are within free tier limits

## License

This implementation is for educational purposes. Make sure to give due credits when using/replicating
