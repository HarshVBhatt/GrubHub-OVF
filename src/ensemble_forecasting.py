import pandas as pd
import numpy as np
from dask.distributed import Client, as_completed
import boto3
import json
import joblib
from datetime import datetime, timedelta
import warnings
from pandas.api.types import is_datetime64_any_dtype
warnings.filterwarnings('ignore')

class EnsembleForecaster:
    def __init__(self, scheduler_address):
        """Initialize connection to Dask cluster"""
        self.client = Client(scheduler_address)
        self.s3 = boto3.client('s3')
        
        # Import bucket name from config
        try:
            from config import S3_BUCKET_DATA, S3_BUCKET_FORECASTS, S3_BUCKET_FEATURES
            self.data_bucket = S3_BUCKET_DATA
            self.forecast_bucket = S3_BUCKET_FORECASTS
            self.feature_bucket = S3_BUCKET_FEATURES
        except ImportError:
            self.bucket = 'grubhub-ovf-data-lake'
            self.forecast_bucket = 'grubhub-ovf-forecasts'
            self.feature_bucket = 'grubhub-ovf-features'
            print("Warning: Using default bucket names. Create config.py with your settings.")
        
        print(f"Connected to Dask cluster for forecasting")
        print(f"Workers available: {len(self.client.scheduler_info()['workers'])}")

    def generate_ensemble_forecasts(self, horizon_hours=72):
        """
        Generate forecasts using Prophet and SARIMAX models with model-split approach
        Default horizon: 72 hours (3 days)
        """
        
        print(f"\nGenerating {horizon_hours}-hour forecasts...")
        
        # Get metadata
        metadata_obj = self.s3.get_object(Bucket=self.feature_bucket, Key='feature_metadata.json')
        metadata = json.loads(metadata_obj['Body'].read())
        regions = metadata['regions']
        
        # Download features for forecast period preparation
        self.s3.download_file(
            self.feature_bucket,
            'features/features_all_regions.parquet',
            '/tmp/features_all_regions.parquet'
        )
        
        features_df = pd.read_parquet('/tmp/features_all_regions.parquet')
        if not is_datetime64_any_dtype(features_df["timestamp"]):
            features_df['timestamp'] = pd.to_datetime(features_df['timestamp'])
        
        # Get last timestamp
        last_timestamp = features_df['timestamp'].max()
        
        # Create future timestamps
        future_timestamps = pd.date_range(
            start=last_timestamp + timedelta(hours=1),
            periods=horizon_hours,
            freq='H'
        )
        
        # Get available workers
        workers = list(self.client.scheduler_info()['workers'].keys())
        if len(workers) < 2:
            print(f"Warning: Only {len(workers)} worker(s) available. Optimal performance requires 2 workers.")
        
        # Submit model-specific forecasting tasks
        futures = []
        
        # Worker 1: All Prophet forecasts
        print("\nSubmitting Prophet forecasts to Worker 1...")
        prophet_future = self.client.submit(
            generate_prophet_forecasts,
            regions,
            features_df,
            future_timestamps,
            self.feature_bucket,
            workers=[workers[0]] if workers else None
        )
        futures.append(('prophet', prophet_future))
        
        # Worker 2: All SARIMAX forecasts
        print("Submitting SARIMAX forecasts to Worker 2...")
        sarimax_future = self.client.submit(
            generate_sarimax_forecasts,
            regions,
            features_df,
            future_timestamps,
            self.feature_bucket,
            workers=[workers[1]] if len(workers) > 1 else workers
        )
        futures.append(('sarimax', sarimax_future))
        
        # Collect results from both model types
        model_forecasts = {}
        for model_type, future in futures:
            print(f"\nWaiting for {model_type} forecasts...")
            forecasts = future.result()
            model_forecasts[model_type] = forecasts
            print(f"✅ {model_type} completed forecasts for {len(regions)} regions")
        
        # Combine forecasts from both models
        print("\nEnsembling Prophet and SARIMAX forecasts...")
        ensemble_forecasts = self._ensemble_forecasts(
            model_forecasts['prophet'],
            model_forecasts['sarimax'],
            regions,
            future_timestamps
        )
        
        # Create final forecast DataFrame
        forecast_df = pd.DataFrame(ensemble_forecasts)
        
        # Quality checks
        forecast_df['ensemble_forecast'] = forecast_df['ensemble_forecast'].clip(lower=0)
        forecast_df['ensemble_forecast'] = forecast_df['ensemble_forecast'].round().astype(int)
        
        # Save forecasts
        forecast_path = '/tmp/ensemble_forecasts.parquet'
        forecast_df.to_parquet(forecast_path, index=False)
        
        # Upload to S3
        forecast_key = f'forecasts/ensemble_forecast_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        self.s3.upload_file(forecast_path, self.forecast_bucket, forecast_key)
        
        # Create forecast summary
        summary = {
            'forecast_time': datetime.now().isoformat(),
            'horizon_hours': horizon_hours,
            'regions_forecasted': len(regions),
            'total_predictions': len(forecast_df),
            'forecast_start': str(forecast_df['timestamp'].min()),
            'forecast_end': str(forecast_df['timestamp'].max()),
            'models_used': ['prophet', 'sarimax'],
            'ensemble_weights': {'prophet': 0.5, 'sarimax': 0.5},
            'avg_forecast_by_hour': forecast_df.groupby('hour')['ensemble_forecast'].mean().to_dict(),
            's3_location': f's3://{self.forecast_bucket}/{forecast_key}'
        }
        
        self.s3.put_object(
            Bucket=self.forecast_bucket,
            Key='forecasts/latest_forecast_summary.json',
            Body=json.dumps(summary, indent=2)
        )
        
        print(f"\n✅ Forecasting complete!")
        print(f"Generated {len(forecast_df):,} predictions")
        print(f"Saved to: {summary['s3_location']}")
        
        return forecast_df, summary
    
    def _ensemble_forecasts(self, prophet_forecasts, sarimax_forecasts, regions, timestamps):
        """Efficiently ensemble Prophet and SARIMAX forecasts"""
        # Convert to DataFrames for efficient merging
        prophet_df = pd.DataFrame(prophet_forecasts)
        sarimax_df = pd.DataFrame(sarimax_forecasts)
        
        # Merge on region and timestamp
        merged = pd.merge(
            prophet_df,
            sarimax_df,
            on=['region', 'timestamp', 'hour', 'day_of_week'],
            suffixes=('_prophet', '_sarimax')
        )
        
        # Calculate ensemble forecast (simple average, can be adjusted)
        # You can implement more sophisticated weighting based on historical performance
        merged['ensemble_forecast'] = (
            0.5 * merged['forecast_prophet'] + 
            0.5 * merged['forecast_sarimax']
        )
        
        # Prepare final output
        ensemble_records = []
        for _, row in merged.iterrows():
            ensemble_records.append({
                'timestamp': row['timestamp'],
                'region': row['region'],
                'hour': row['hour'],
                'day_of_week': row['day_of_week'],
                'prophet_forecast': row['forecast_prophet'],
                'sarimax_forecast': row['forecast_sarimax'],
                'ensemble_forecast': row['ensemble_forecast']
            })
        
        return ensemble_records
    
def generate_prophet_forecasts(regions, features_df, future_timestamps, bucket):
    """Generate Prophet forecasts for all regions on a single worker"""
    import pandas as pd
    import numpy as np
    import joblib
    import boto3
    from prophet import Prophet
    
    print(f"Prophet worker: Starting forecasts for {len(regions)} regions")
    s3 = boto3.client('s3')
    forecasts = []
    successful_regions = 0
    
    for i, region in enumerate(regions):
        if i % 10 == 0:
            print(f"Prophet worker: Processing region {i+1}/{len(regions)}")
        
        try:
            # Get region's historical data
            region_data = features_df[features_df['region'] == region].copy()
            last_values = region_data.iloc[-1]
            
            # Download Prophet model
            prophet_path = f'/tmp/prophet_region_{region}.pkl'
            s3.download_file(bucket, f'models/prophet/region_{region}.pkl', prophet_path)
            prophet_model = joblib.load(prophet_path)
            
            # Prepare Prophet input
            prophet_df = pd.DataFrame({
                'ds': future_timestamps,
                'order_hour': future_timestamps.hour,
                'day_of_week': future_timestamps.dayofweek,
                'weather_encoded': last_values.get('weather_encoded', 0),
                'traffic_encoded': last_values.get('traffic_encoded', 0)
            })
            
            # Generate forecast
            prophet_pred = prophet_model.predict(prophet_df)
            prophet_forecast = prophet_pred['yhat'].values
            
            # Create forecast records
            for j, ts in enumerate(future_timestamps):
                forecasts.append({
                    'timestamp': ts,
                    'region': region,
                    'hour': ts.hour,
                    'day_of_week': ts.dayofweek,
                    'forecast': max(0, prophet_forecast[j])
                })
            
            successful_regions += 1
            
            # Clean up
            import os
            if os.path.exists(prophet_path):
                os.remove(prophet_path)
                
        except Exception as e:
            print(f"Prophet forecast failed for region {region}: {e}")
            # Use simple fallback - historical average
            historical_avg = region_data['order_count'].mean() if len(region_data) > 0 else 50
            
            for ts in future_timestamps:
                forecasts.append({
                    'timestamp': ts,
                    'region': region,
                    'hour': ts.hour,
                    'day_of_week': ts.dayofweek,
                    'forecast': historical_avg
                })
    
    print(f"Prophet worker: Completed {successful_regions}/{len(regions)} regions successfully")
    return forecasts

def generate_sarimax_forecasts(regions, features_df, future_timestamps, bucket):
    """Generate SARIMAX forecasts for all regions on a single worker"""
    import pandas as pd
    import numpy as np
    import joblib
    import boto3
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    
    print(f"SARIMAX worker: Starting forecasts for {len(regions)} regions")
    s3 = boto3.client('s3')
    forecasts = []
    successful_regions = 0
    
    for i, region in enumerate(regions):
        if i % 10 == 0:
            print(f"SARIMAX worker: Processing region {i+1}/{len(regions)}")
        
        try:
            # Get region's historical data
            region_data = features_df[features_df['region'] == region].copy()
            last_values = region_data.iloc[-1]
            
            # Try to load SARIMAX model
            sarimax_path = f'/tmp/sarimax_region_{region}.pkl'
            try:
                s3.download_file(bucket, f'models/sarimax/region_{region}.pkl', sarimax_path)
            except:
                # Try simple model fallback
                s3.download_file(bucket, f'models/sarimax/region_{region}_simple.pkl', sarimax_path)
            
            model_data = joblib.load(sarimax_path)
            
            # Extract model and metadata
            if isinstance(model_data, dict):
                sarimax_model = model_data['model']
                exog_vars = model_data.get('exog_vars', [])
            else:
                # Backward compatibility
                sarimax_model = model_data
                exog_vars = ['order_hour', 'day_of_week', 'weather_encoded', 'traffic_encoded']
            
            # Prepare exogenous variables for forecast
            if exog_vars:
                exog_future = pd.DataFrame({
                    'order_hour': future_timestamps.hour,
                    'day_of_week': future_timestamps.dayofweek,
                    'weather_encoded': last_values.get('weather_encoded', 0),
                    'traffic_encoded': last_values.get('traffic_encoded', 0)
                })
                # Select only the variables used in training
                exog_future = exog_future[exog_vars] if exog_vars else None
            else:
                exog_future = None
            
            # Generate forecast
            sarimax_pred = sarimax_model.forecast(steps=len(future_timestamps), exog=exog_future)
            
            # Convert to numpy array if needed
            if hasattr(sarimax_pred, 'values'):
                sarimax_forecast = sarimax_pred.values
            else:
                sarimax_forecast = np.array(sarimax_pred)
            
            # Create forecast records
            for j, ts in enumerate(future_timestamps):
                forecasts.append({
                    'timestamp': ts,
                    'region': region,
                    'hour': ts.hour,
                    'day_of_week': ts.dayofweek,
                    'forecast': max(0, sarimax_forecast[j])
                })
            
            successful_regions += 1
            
            # Clean up
            import os
            if os.path.exists(sarimax_path):
                os.remove(sarimax_path)
                
        except Exception as e:
            print(f"SARIMAX forecast failed for region {region}: {e}")
            # Use simple fallback - rolling average
            if len(region_data) > 24:
                fallback_value = region_data['order_count'].rolling(24).mean().iloc[-1]
            else:
                fallback_value = region_data['order_count'].mean() if len(region_data) > 0 else 50
            
            for ts in future_timestamps:
                forecasts.append({
                    'timestamp': ts,
                    'region': region,
                    'hour': ts.hour,
                    'day_of_week': ts.dayofweek,
                    'forecast': fallback_value
                })
    
    print(f"SARIMAX worker: Completed {successful_regions}/{len(regions)} regions successfully")
    return forecasts

# Main execution
if __name__ == "__main__":
    import sys
    
    # Try to get bucket from config first
    try:
        from config import S3_BUCKET_DATA
        data_bucket = S3_BUCKET_DATA
    except ImportError:
        data_bucket = 'grubhub-ovf-data-lake'
    
    if len(sys.argv) > 1:
        scheduler_address = sys.argv[1]
    else:
        # Try to read from S3
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(Bucket=data_bucket, Key='cluster/current_cluster_info.json')
            cluster_info = json.loads(obj['Body'].read())
            scheduler_address = cluster_info['dask_scheduler']
        except:
            print("Error: Could not find cluster info. Please provide scheduler address.")
            print("Usage: python ensemble_forecasting.py <scheduler-address>")
            print("Example: python ensemble_forecasting.py ec2-xx-xx-xx-xx.compute-1.amazonaws.com:8786")
            sys.exit(1)
    
    forecaster = EnsembleForecaster(scheduler_address)
    forecast_df, summary = forecaster.generate_ensemble_forecasts(horizon_hours=72)
    
    # Display sample results
    print("\nSample forecasts (first 10 rows):")
    print(forecast_df.head(10))
    
    print("\nForecast statistics:")
    print(f"Average hourly forecast: {forecast_df['ensemble_forecast'].mean():.1f} orders")
    print(f"Peak hour forecast: {forecast_df['ensemble_forecast'].max()} orders")
    print(f"Minimum hour forecast: {forecast_df['ensemble_forecast'].min()} orders")
    
    # Show model-specific statistics
    print("\nModel performance:")
    print(f"Prophet average: {forecast_df['prophet_forecast'].mean():.1f} orders")
    print(f"SARIMAX average: {forecast_df['sarimax_forecast'].mean():.1f} orders")