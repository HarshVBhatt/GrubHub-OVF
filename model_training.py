# model_parallel_training.py
import pandas as pd
import numpy as np
from dask.distributed import Client, as_completed
import boto3
import json
import joblib
from datetime import datetime
import sqlite3
import os
import time

class ModelParallelTrainer:
    def __init__(self, scheduler_address):
        """Initialize connection to Dask cluster"""
        self.client = Client(scheduler_address)
        self.s3 = boto3.client('s3')
        
        # Import bucket name from config
        try:
            from config import S3_BUCKET_DATA, S3_BUCKET_FEATURES
            self.data_lake_bucket = S3_BUCKET_DATA
            self.features_bucket = S3_BUCKET_FEATURES
        except ImportError:
            self.data_lake_bucket = 'grubhub-ovf-data-lake'
            self.features_bucket = 'grubhub-ovf-features'
            print("Warning: Using default bucket name. Create config.py with your settings.")
        
        print(f"Connected to Dask cluster at {scheduler_address}")
        print(f"Workers available: {len(self.client.scheduler_info()['workers'])}")
        
        # Wait for workers
        while len(self.client.scheduler_info()['workers']) < 2:
            print("Waiting for 2 workers...")
            time.sleep(10)
        
        self.workers = list(self.client.scheduler_info()['workers'].keys())
        print(f"Workers ready: {self.workers}")

    def distribute_model_training(self):
        """
        Distribute different model types across workers:
        - Worker 1: Prophet models for all regions
        - Worker 2: SARIMAX models for all regions  
        """
        
        # Download features to local first
        print("\nDownloading features from S3...")
        self.s3.download_file(
            self.features_bucket,
            'features/features_all_regions.parquet',
            '/tmp/features_all_regions.parquet'
        )
        
        # Get split info
        split_obj = self.s3.get_object(Bucket=self.data_lake_bucket, Key='features/split_info.json')
        split_info = json.loads(split_obj['Body'].read())

        # Get metadata to pass regions info
        metadata_obj = self.s3.get_object(Bucket=self.data_lake_bucket, Key='features/feature_metadata.json')
        metadata = json.loads(metadata_obj['Body'].read())
        
        # Submit training tasks to specific workers
        futures = []
        
        # Worker 1: Prophet models
        print("\nSubmitting Prophet training to Worker 1...")
        prophet_future = self.client.submit(
            train_prophet_models,
            self.features_bucket,
            split_info,
            metadata['regions'],
            workers=[self.workers[0]]
        )
        futures.append(('prophet', prophet_future))
        
        # Worker 2: SARIMAX models
        print("Submitting SARIMAX training to Worker 2...")
        sarimax_future = self.client.submit(
            train_sarimax_models,
            self.features_bucket,
            split_info,
            metadata['regions'],
            workers=[self.workers[1]] if len(self.workers) > 1 else [self.workers[0]]
        )
        futures.append(('sarimax', sarimax_future))
        
        # Collect results
        results = {}
        for model_type, future in futures:
            print(f"\nWaiting for {model_type} training to complete...")
            result = future.result()
            results[model_type] = result
            print(f"✅ {model_type} training complete: {result['models_trained']} models")
        
        # Save training summary
        summary = {
            'training_time': datetime.now().isoformat(),
            'workers_used': int(len(self.workers)),
            'model_types': list(results.keys()),
            'total_models': int(sum(r['models_trained'] for r in results.values())),
            'results': results
        }
        
        # self.s3.put_object(
        #     Bucket=self.features_bucket,
        #     Key='models/training_summary.json',
        #     Body=json.dumps(summary, indent=2)
        # )
        
        return summary

# Model training functions to run on workers
def train_prophet_models(features_bucket, split_info, regions):
    """Train Prophet models for all regions on a single worker"""
    from prophet import Prophet
    import pandas as pd
    import joblib
    import boto3
    import sqlite3
    from pandas.api.types import is_datetime64_any_dtype
    
    print(f"Prophet worker starting training...")

    # Initialize S3 client on worker
    s3 = boto3.client('s3')
    
    # Download features from S3 to worker's local storage
    print("Downloading features from S3...")
    features_path = '/tmp/features_prophet_worker.parquet'
    s3.download_file(
        features_bucket,
        'features/features_all_regions.parquet',
        features_path
    )

    # Load features
    features_df = pd.read_parquet(features_path)
    if not is_datetime64_any_dtype(features_df["timestamp"]):
        features_df['timestamp'] = pd.to_datetime(features_df['timestamp'])
    
    # Filter training data
    try:
        # Try to parse the split timestamp
        split_timestamp = pd.to_datetime(split_info['split_timestamp'])
    except:
        # If parsing fails, use the train_end timestamp instead
        print(f"Warning: Could not parse split_timestamp: {split_info.get('split_timestamp', 'None')}")
        if 'train_end' in split_info:
            split_timestamp = pd.to_datetime(split_info['train_end'])
        else:
            # Fallback: use 80% of the data
            split_idx = int(len(features_df) * 0.8)
            split_timestamp = features_df.iloc[split_idx]['timestamp']
        print(f"Using split timestamp: {split_timestamp}")

    train_df = features_df[features_df['timestamp'] < split_timestamp]
    
    print(f"Training Prophet models for {len(regions)} regions")
    
    # Setup local cache
    cache_db = '/tmp/prophet_cache.db'
    conn = sqlite3.connect(cache_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS models (
            region_id INTEGER PRIMARY KEY,
            model_type TEXT,
            model_path TEXT,
            metrics TEXT,
            trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    trained_models = []
    
    for i, region in enumerate(regions):
        if i % 5 == 0:
            print(f"Prophet: Training region {i+1}/{len(regions)}")

        # Get region data
        region_data = train_df[train_df['region'] == region].copy()
        
        # Prepare data for Prophet
        prophet_df = pd.DataFrame({
            'ds': region_data['timestamp'],
            'y': region_data['order_count']
        })
        
        # Add regressors
        for col in ['order_hour', 'day_of_week', 'weather_encoded', 'traffic_encoded']:
            prophet_df[col] = region_data[col].values
        
        # Initialize Prophet with custom parameters
        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,  # Not enough data
            seasonality_mode='multiplicative',
            interval_width=0.95,
            n_changepoints=10
        )
        
        # Add regressors
        model.add_regressor('order_hour')
        model.add_regressor('day_of_week')
        model.add_regressor('weather_encoded')
        model.add_regressor('traffic_encoded')
        
        # Fit model
        model.fit(prophet_df)
        
        # Save model locally
        model_path = f'/tmp/prophet_region_{region}.pkl'
        joblib.dump(model, model_path)
        
        # Upload to S3
        s3_key = f'models/prophet/region_{region}.pkl'
        s3.upload_file(model_path, 'grubhub-ovf-features', s3_key)
        
        # Cache model info
        conn.execute("""
            INSERT OR REPLACE INTO models (region_id, model_type, model_path, metrics)
            VALUES (?, ?, ?, ?)
        """, (region, 'prophet', s3_key, json.dumps({'status': 'trained'})))
        
        trained_models.append({
            'region': region,
            'model_type': 'prophet',
            's3_path': s3_key
        })
        
        print(f"Trained Prophet model for region {region}")
        os.remove(model_path)
    
    conn.commit()
    conn.close()

    if os.path.exists(features_path):
        os.remove(features_path)
    
    return {
        'model_type': 'prophet',
        'models_trained': len(trained_models),
        'regions': regions,
        'cache_db': cache_db
    }

def train_sarimax_models(features_bucket, split_info, regions):
    """Train SARIMAX models for all regions on a single worker"""
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    import pandas as pd
    import joblib
    import boto3
    import warnings
    from pandas.api.types import is_datetime64_any_dtype
    warnings.filterwarnings('ignore')
    
    print(f"SARIMAX worker starting training...")

    # Initialize S3 client on worker
    s3 = boto3.client('s3')
    
    # Download features from S3 to worker's local storage
    print("Downloading features from S3...")
    features_path = '/tmp/features_prophet_worker.parquet'
    s3.download_file(
        features_bucket,
        'features/features_all_regions.parquet',
        features_path
    )

    # Load features
    features_df = pd.read_parquet(features_path)
    if not is_datetime64_any_dtype(features_df["timestamp"]):
        features_df['timestamp'] = pd.to_datetime(features_df['timestamp'])
    
    # Filter training data
    try:
        # Try to parse the split timestamp
        split_timestamp = pd.to_datetime(split_info['split_timestamp'])
    except:
        # If parsing fails, use the train_end timestamp instead
        print(f"Warning: Could not parse split_timestamp: {split_info.get('split_timestamp', 'None')}")
        if 'train_end' in split_info:
            split_timestamp = pd.to_datetime(split_info['train_end'])
        else:
            # Fallback: use 80% of the data
            split_idx = int(len(features_df) * 0.8)
            split_timestamp = features_df.iloc[split_idx]['timestamp']
        print(f"Using split timestamp: {split_timestamp}")

    train_df = features_df[features_df['timestamp'] < split_timestamp]
    
    # Get unique regions
    regions = sorted(train_df['region'].unique())
    print(f"Training SARIMAX models for {len(regions)} regions")
  
    trained_models = []
    
    for i, region in enumerate(regions):
        if i % 5 == 0:
            print(f"Prophet: Training region {i+1}/{len(regions)}")

        # Get region data
        region_data = train_df[train_df['region'] == region].copy()
        region_data = region_data.set_index('timestamp').sort_index()
        
        # Prepare exogenous variables
        exog_vars = ['order_hour', 'day_of_week', 'weather_encoded', 'traffic_encoded']
        exog_data = region_data[exog_vars]
        
        try:
            # Simple SARIMAX model (keeping it simple for performance)
            model = SARIMAX(
                region_data['order_count'],
                exog=exog_data,
                order=(1, 0, 1),  # ARIMA order
                seasonal_order=(1, 0, 1, 24),  # Seasonal order (24-hour cycle)
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            
            # Fit model
            fitted_model = model.fit(disp=False, maxiter=100)
            
            # Save model
            model_path = f'/tmp/sarimax_region_{region}.pkl'
            joblib.dump(fitted_model, model_path)
            
            # Upload to S3
            s3_key = f'models/sarimax/region_{region}.pkl'
            s3.upload_file(model_path, 'grubhub-ovf-features', s3_key)
            
            trained_models.append({
                'region': region,
                'model_type': 'sarimax',
                's3_path': s3_key,
                'aic': fitted_model.aic
            })
            
            os.remove(model_path)
            
        except Exception as e:
            print(f"Failed to train SARIMAX for region {region}: {str(e)}")
            # Fall back to simpler model
            try:
                simple_model = SARIMAX(
                    region_data['order_count'],
                    order=(1, 0, 0),
                    enforce_stationarity=False
                )
                fitted_model = simple_model.fit(disp=False)
                
                model_path = f'/tmp/sarimax_region_{region}_simple.pkl'
                joblib.dump(fitted_model, model_path)
                
                s3_key = f'models/sarimax/region_{region}_simple.pkl'
                s3.upload_file(model_path, 'grubhub-ovf-features', s3_key)
                
                trained_models.append({
                    'region': region,
                    'model_type': 'sarimax_simple',
                    's3_path': s3_key
                })

                os.remove(model_path)
            except:
                pass
    
    if os.path.exists(features_path):
        os.remove(features_path)
    
    return {
        'model_type': 'sarimax',
        'models_trained': len(trained_models),
        'regions': [m['region'] for m in trained_models]
    }

# Main execution
if __name__ == "__main__":
    import sys
    
    # Try to get bucket from config first
    try:
        from config import S3_BUCKET_DATA
        data_lake_bucket = S3_BUCKET_DATA
    except ImportError:
        data_lake_bucket = 'grubhub-ovf-data-lake'
    
    if len(sys.argv) > 1:
        scheduler_address = sys.argv[1]
    else:
        # Try to read from S3
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(Bucket=data_lake_bucket, Key='cluster/current_cluster_info.json')
            cluster_info = json.loads(obj['Body'].read())
            scheduler_address = cluster_info['dask_scheduler']
        except:
            scheduler_address = 'localhost:8786'
    
    print(f"Connecting to Dask scheduler at {scheduler_address}")
    
    trainer = ModelParallelTrainer(scheduler_address)
    summary = trainer.distribute_model_training()
    
    print("\n✅ Model training complete!")
    print(f"Total models trained: {summary['total_models']}")
    print(f"Model types: {summary['model_types']}")