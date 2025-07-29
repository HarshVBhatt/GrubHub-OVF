import pandas as pd
import numpy as np
import boto3
from pyhive import presto
import json
from datetime import datetime
import time

class FeatureEngineer:
    def __init__(self):
        self.s3 = boto3.client('s3')
        
        # Import configuration
        try:
            from config import S3_BUCKET_DATA, S3_BUCKET_FEATURES
            self.bucket = S3_BUCKET_DATA
            self.features_bucket = S3_BUCKET_FEATURES
        except ImportError:
            self.bucket = 'grubhub-ovf-data-lake'
            self.features_bucket = 'grubhub-ovf-features'
            # print("Warning: Using default bucket name. Create config.py with your settings.")
        
        # Define encoding mappings for categorical variables
        self.weather_mapping = {
            'Clear': 0,
            'Sunny': 1,
            'Cloudy': 2, 
            'Fog': 3,
            'Sandstorms': 4,
            'Stormy': 5,
            'Windy': 6
        }
        
        self.traffic_mapping = {
            'Low': 0,
            'Medium': 1,
            'High': 2,
            'Jam': 3
        }
        
        # Get PrestoDB connection
        self.presto_conn = self._get_presto_connection()
    
    def _get_presto_connection(self):
        """Get PrestoDB connection from EMR master"""
        try:
            # Try to get master hostname from EMR metadata
            try:
                with open('/mnt/var/lib/info/job-flow.json', 'r') as f:
                    job_flow = json.load(f)
                    master_dns = job_flow.get('masterPrivateDnsName', 'localhost')
                    # Extract just the hostname
                    presto_host = master_dns.split('.')[0] if '.' in master_dns else master_dns
            except:
                # Fallback to localhost if on master
                presto_host = 'localhost'
            
            print(f"Connecting to PrestoDB at {presto_host}:8889...")
            
            # Connect to PrestoDB
            conn = presto.connect(
                host=presto_host,
                port=8889,
                username='hadoop',
                catalog='hive',
                schema='default'
            )
            
            # Test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            print("✅ Connected to PrestoDB successfully")
            return conn
            
        except Exception as e:
            print(f"❌ Error connecting to PrestoDB: {e}")
            print("Falling back to DuckDB")
            return None
    
    def setup_presto_tables(self):
        """Create external tables in PrestoDB pointing to S3 data"""
        if not self.presto_conn:
            print("No PrestoDB connection available")
            return False
        
        cursor = self.presto_conn.cursor()
        
        try:
            print("\nSetting up PrestoDB external table...")
            
            # First, let's check what we have
            print("Checking catalogs...")
            cursor.execute("SHOW CATALOGS")
            catalogs = cursor.fetchall()
            print(f"Available catalogs: {catalogs}")
            
            # Check schemas in hive catalog
            print("\nChecking schemas in hive catalog...")
            cursor.execute("SHOW SCHEMAS FROM hive")
            schemas = cursor.fetchall()
            print(f"Available schemas in hive: {schemas}")
            
            
            # Check existing tables in default schema
            print("\nChecking existing tables in hive.default...")
            cursor.execute("SHOW TABLES FROM hive.default")
            existing_tables = cursor.fetchall()
            print(f"Existing tables: {existing_tables}")
            
            # Drop table if exists
            if any('orders_raw' in str(table) for table in existing_tables):
                print("Dropping existing orders_raw table...")
                cursor.execute("DROP TABLE IF EXISTS hive.default.orders_raw")
                cursor.fetchone()
            
            # Create external table
            # Note: For Hive external tables in Presto, we need to use Hive DDL syntax
            print(f"\nCreating external table pointing to s3://{self.bucket}/raw/data/")
            
            # First, let's verify the S3 path has data
            s3_objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix='raw/data/')
            if 'Contents' in s3_objects:
                print(f"Found {len(s3_objects['Contents'])} objects in S3 path")
                for obj in s3_objects['Contents']:
                    print(f"  - {obj['Key']}")
            
            # Create the external table
            # Using CREATE EXTERNAL TABLE syntax for Hive
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS hive.default.orders_raw (
                timestamp_str varchar,
                region bigint,
                order_count bigint,
                order_hour bigint,
                day_of_week bigint,
                is_weekend boolean,
                month bigint,
                day bigint,
                weather_conditions varchar,
                traffic_density varchar
            )
            WITH (
                format = 'PARQUET',
                external_location = 's3://{self.bucket}/raw/data/'
            )
            """

            print("Executing CREATE TABLE query...")
            cursor.execute(create_table_query)
            cursor.fetchone()
            print("✅ Table creation query executed")
            
            # Check existing tables in default schema
            print("\nChecking existing tables in hive.default...")
            cursor.execute("SHOW TABLES FROM hive.default")
            existing_tables = cursor.fetchall()
            print(f"Existing tables: {existing_tables}")

            # Wait a moment for table to be registered
            time.sleep(2)
            
            # Verify table exists
            print("\nVerifying table creation...")
            cursor.execute("SHOW TABLES FROM hive.default LIKE 'orders_raw'")
            tables = cursor.fetchall()
            if tables:
                print(f"✅ Table 'orders_raw' exists: {tables}")
                
                # Try to get table schema
                cursor.execute("DESCRIBE hive.default.orders_raw")
                schema = cursor.fetchall()
                print("\nTable schema:")
                for col in schema:
                    print(f"  {col}")
                
                # Try to get a sample of data
                try:
                    print("\nTrying to query table...")
                    cursor.execute("SELECT COUNT(*) FROM hive.default.orders_raw")
                    count = cursor.fetchone()[0]
                    print(f"✅ Table has {count} records")
                except Exception as e:
                    print(f"Warning: Could not count records: {e}")
                    # Try a simpler query
                try:
                    cursor.execute("SELECT * FROM hive.default.orders_raw LIMIT 1")
                    sample = cursor.fetchone()
                    print(f"✅ Table is queryable. Sample record: {sample}")
                except Exception as e2:
                    print(f"Warning: Could not query table: {e2}")
            else:
                print("❌ Table creation may have failed")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error setting up PrestoDB tables: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            cursor.close()

    def create_features_with_presto(self):
        """Create features using PrestoDB for distributed SQL processing"""
        if not self.presto_conn:
            print("No PrestoDB connection - using fallback method")
            return self.create_features_with_duckdb()
        
        print("\nCreating features with PrestoDB...")
        cursor = self.presto_conn.cursor()
        
        try:
            # Setup tables first
            if not self.setup_presto_tables():
                raise Exception("Failed to setup PrestoDB tables")
            
            # Main feature engineering query
            feature_query = f"""
            WITH encoded_data AS (
                SELECT *,
                    CAST(timestamp_str AS TIMESTAMP) as timestamp,                    
                    -- Traffic encoding  
                    CASE traffic_density
                        WHEN 'Low' THEN 0
                        WHEN 'Medium' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Jam' THEN 3
                        ELSE -1
                    END as traffic_encoded,
                    
                    -- One-hot encoding for weather
                    CASE weather_conditions
                        WHEN 'Clear' THEN 0
                        WHEN 'Sunny' THEN 1
                        WHEN 'Cloudy' THEN 2
                        WHEN 'Fog' THEN 3
                        WHEN 'Sandstorms' THEN 4
                        WHEN 'Stormy' THEN 5
                        WHEN 'Windy' THEN 6
                    END as weather_encoded
                    
                FROM hive.default.orders_raw
            ),
            
            hourly_features AS (
                SELECT 
                    region,
                    timestamp,
                    order_hour,
                    day_of_week,
                    is_weekend,
                    month,
                    day,
                    weather_conditions,
                    traffic_density,
                    traffic_encoded,
                    weather_encoded,
                    order_count
                    
                FROM encoded_data
            ),
            
            final_features AS (
                SELECT 
                    *,
                    -- Lag features using window functions
                    LAG(order_count, 1) OVER (PARTITION BY region ORDER BY timestamp) AS orders_lag_1h,
                    LAG(order_count, 24) OVER (PARTITION BY region ORDER BY timestamp) AS orders_lag_24h,
                    LAG(order_count, 168) OVER (PARTITION BY region ORDER BY timestamp) AS orders_lag_1w,
                    
                    -- Moving averages
                    AVG(order_count) OVER (
                        PARTITION BY region 
                        ORDER BY timestamp 
                        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS ma_24h,
                    
                    AVG(order_count) OVER (
                        PARTITION BY region 
                        ORDER BY timestamp 
                        ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                    ) AS ma_6h,
                    
                    -- Rolling statistics
                    STDDEV(order_count) OVER (
                        PARTITION BY region 
                        ORDER BY timestamp 
                        ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
                    ) AS std_24h,
                    
                    -- Trend features
                    order_count - LAG(order_count, 24) OVER (PARTITION BY region ORDER BY timestamp) AS diff_24h,
                    
                    -- Cumulative features
                    SUM(order_count) OVER (
                        PARTITION BY region, DATE(timestamp)
                        ORDER BY timestamp
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_daily_orders
                    
                FROM hourly_features
            )
            
            SELECT 
                -- Original columns
                region,
                timestamp,
                order_hour,
                day_of_week,
                is_weekend,
                month,
                day,
                weather_conditions,
                traffic_density,
                traffic_encoded,
                weather_encoded,
                order_count,
                
                -- Fill nulls for lag features
                COALESCE(orders_lag_1h, 0) AS orders_lag_1h_filled,
                COALESCE(orders_lag_24h, CAST(AVG(order_count) OVER (PARTITION BY region) AS BIGINT)) AS orders_lag_24h_filled,
                COALESCE(orders_lag_1w, CAST(AVG(order_count) OVER (PARTITION BY region) AS BIGINT)) AS orders_lag_1w_filled,
                COALESCE(ma_24h, AVG(order_count) OVER (PARTITION BY region)) AS ma_24h_filled,
                COALESCE(ma_6h, AVG(order_count) OVER (PARTITION BY region)) AS ma_6h_filled,
                COALESCE(std_24h, 0.0) AS std_24h_filled,
                COALESCE(diff_24h, CAST(0 AS BIGINT)) AS diff_24h_filled,
                cumulative_daily_orders
                
            FROM final_features
            ORDER BY region, timestamp
            """
            
            # Execute query
            print("Executing feature engineering query (this may take a few minutes)...")
            cursor.execute(feature_query)
            cursor.fetchone()
            print("Query executed successfully")
            
            # Fetch results in batches
            batch_size = 1000
            all_data = []
            batch_num = 0
            
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                all_data.extend(batch)
                batch_num += 1
                print(f"Fetched batch {batch_num} ({len(all_data)} records total)...")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Create DataFrame
            features_df = pd.DataFrame(all_data, columns=columns)
            
            print(f"Created {len(features_df.columns)} features for {len(features_df):,} records using PrestoDB")
            
            # Save features
            features_df.to_parquet('/tmp/features_all_regions.parquet', index=False)
            
            # Upload to S3
            self.s3.upload_file(
                '/tmp/features_all_regions.parquet',
                self.features_bucket,
                'features/features_all_regions.parquet'
            )
            
            # Create metadata
            self._save_feature_metadata(features_df)
            
            return features_df, self._get_metadata_dict(features_df)
            
        except Exception as e:
            print(f"Error in PrestoDB feature engineering: {e}")
            print("Falling back to DuckDB")
            return e
        finally:
            cursor.close()

    def _get_metadata_dict(self, features_df):
        """Create metadata dictionary"""
        return {
            'n_records': len(features_df),
            'n_features': len(features_df.columns),
            'n_regions': features_df['region'].nunique(),
            'regions': sorted(features_df['region'].unique().tolist())
        }
    
    def _save_feature_metadata(self, features_df):
        """Save feature metadata to S3"""
        feature_metadata = {
            'creation_time': datetime.now().isoformat(),
            'n_records': len(features_df),
            'n_features': len(features_df.columns),
            'features': list(features_df.columns),
            'regions': sorted(features_df['region'].unique().tolist()),
            'date_range': {
                'start': str(features_df['timestamp'].min()),
                'end': str(features_df['timestamp'].max())
            },
            'encodings': {
                'weather_mapping': self.weather_mapping,
                'traffic_mapping': self.traffic_mapping
            },
            'processing_engine': 'PrestoDB' if self.presto_conn else 'No engine. Error using PrestoDB',
            'file_size_mb': features_df.memory_usage(deep=True).sum() / 1024**2
        }

        self.s3.put_object(
            Bucket=self.features_bucket,
            Key='feature_metadata.json',
            Body=json.dumps(feature_metadata, indent=2)
        )

        print(f"\n✅ Features saved to S3")
        print(f"Location: s3://{self.features_bucket}/features/features_all_regions.parquet")
        print(f"Size: {feature_metadata['file_size_mb']:.2f} MB")
        print(f"Processing engine: {feature_metadata['processing_engine']}")

    def create_train_test_split(self, features_df):
        """Create train/test split based on time"""
        # Use last 20% of data for testing
        split_point = int(len(features_df) * 0.8)
        split_timestamp = features_df.iloc[split_point]['timestamp']
        
        train_mask = features_df['timestamp'] < split_timestamp
        
        print(f"\nTrain/Test Split:")
        print(f"Split date: {split_timestamp}")
        print(f"Train records: {train_mask.sum():,}")
        print(f"Test records: {(~train_mask).sum():,}")
        
        # Save split info
        split_info = {
            'split_timestamp': str(split_timestamp),
            'train_records': int(train_mask.sum()),
            'test_records': int((~train_mask).sum()),
            'train_end': str(features_df[train_mask]['timestamp'].max()),
            'test_start': str(features_df[~train_mask]['timestamp'].min()),
            'test_end': str(features_df[~train_mask]['timestamp'].max())
        }
        
        self.s3.put_object(
            Bucket=self.features_bucket,
            Key='train_test_split.json',
            Body=json.dumps(split_info, indent=2)
        )
        
        return split_info
    
    def load_data_from_s3(self):
        """Load data info from S3"""
        obj = self.s3.get_object(Bucket=self.bucket, Key='raw/metadata/metadata.json')
        metadata = json.loads(obj['Body'].read())
        return metadata['n_records'], metadata['n_regions']
    
    def create_features(self):
        """Main method to create features - uses PrestoDB if available"""
        # Create features using appropriate engine
        if self.presto_conn:
            return self.create_features_with_presto()
        else:
            return "No PrestoDB connection established"
        
# Main execution
if __name__ == "__main__":
    engineer = FeatureEngineer()
    
    # Load data info
    n_records, n_regions = engineer.load_data_from_s3()
    print(f"Processing data: {n_records} records from {n_regions} regions")
    
    # Create features (will use PrestoDB if available, otherwise DuckDB)
    try:
        features_df, metadata = engineer.create_features()
         # Create train/test split
        # split_info = engineer.create_train_test_split(features_df)
        print("\n✅ Feature engineering complete!")
        print("Used PrestoDB for distributed SQL processing")
    except Exception as e:
        print("Error encountered")