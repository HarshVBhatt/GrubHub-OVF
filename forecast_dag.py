# ovf_forecast_limited_dag.py
"""
Airflow DAG for OVF Forecasting Pipeline - Limited Run Version
This DAG runs only 3 times (every hour) then stops.
Uses pre-trained models stored in S3 for forecasting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
import sys

# Add your project path to Python path
PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))  # Update this to your actual path
sys.path.insert(0, os.path.join(PROJECT_PATH, 'src'))

default_args = {
    'owner': 'ovf-system',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(hours=1),  # Start from 1 hour ago
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'grubhub_ovf_forecast_runs',
    default_args=default_args,
    description='OVF Forecasting Pipeline - Runs 3 times only',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    max_active_runs=1,
    tags=['ovf', 'forecasting', 'limited-run']
)

# Task 1: Check Run Count and Stop if Needed
def check_run_count(**context):
    """Check if we've already run 3 times"""
    # Get or initialize run count
    try:
        run_count = Variable.get("ovf_forecast_run_count", default_var=0)
        run_count = int(run_count)
    except:
        run_count = 0
    
    print(f"Current run count: {run_count}")
    
    if run_count >= 3:
        # Stop the DAG from running again
        print("Already completed 3 runs. Pausing DAG...")
        # This will raise an exception to stop the current run
        raise Exception("Completed 3 runs. DAG should be manually triggered for additional runs.")
    
    # Increment run count
    new_count = run_count + 1
    Variable.set("ovf_forecast_run_count", new_count)
    print(f"This is run #{new_count} of 3")
    
    # Store in XCom
    context['task_instance'].xcom_push(key='run_number', value=new_count)
    
    return new_count

check_count_task = PythonOperator(
    task_id='check_run_count',
    python_callable=check_run_count,
    provide_context=True,
    dag=dag
)

# Task 2: Start EMR Cluster for Forecasting
def start_forecast_emr_cluster(**context):
    """Start EMR cluster with m5.xlarge instances for forecasting"""
    import boto3
    import json
    import time
    from datetime import datetime
    
    print("=== Starting EMR Cluster for Forecasting ===")
    
    # Import configuration
    try:
        from config import (S3_BUCKET_DATA, EMR_KEY_NAME, EMR_SUBNET_ID, 
                          EMR_MASTER_SG, EMR_SLAVE_SG, AWS_REGION)
        log_uri = f's3://{S3_BUCKET_DATA}/logs/'
    except:
        print("Warning: Using default EMR configuration")
        EMR_KEY_NAME = 'grubhub-ovf-emr-key'
        log_uri = 's3://grubhub-ovf-data-lake/logs/'
        AWS_REGION = 'us-east-1'
        EMR_SUBNET_ID = None
        EMR_MASTER_SG = None
        EMR_SLAVE_SG = None
        S3_BUCKET_DATA = 'grubhub-ovf-data-lake'
    
    emr = boto3.client('emr', region_name=AWS_REGION)
    s3 = boto3.client('s3')
    
    # Get run number from XCom
    run_number = context['task_instance'].xcom_pull(task_ids='check_run_count', key='run_number')
    
    # EMR cluster configuration with m5.xlarge
    cluster_config = {
        'Name': f'OVF-Forecast-Run{run_number}-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        'ReleaseLabel': 'emr-6.10.0',
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',  # Using m5.xlarge as requested
                    'InstanceCount': 1,
                },
                {
                    'Name': "Workers",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',  # Using m5.xlarge as requested
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': EMR_KEY_NAME,
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        'Applications': [
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Presto'}
        ],
        'BootstrapActions': [
            {
                'Name': 'Install Python packages for forecasting',
                'ScriptBootstrapAction': {
                    'Path': f's3://{S3_BUCKET_DATA}/scripts/bootstrap.sh'
                }
            }
        ],
        'ServiceRole': 'EMR_DefaultRole',
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'LogUri': log_uri,
        'StepConcurrencyLevel': 1,
        'Tags': [
            {'Key': 'Project', 'Value': 'OVF'},
            {'Key': 'Type', 'Value': 'Forecast'},
            {'Key': 'RunNumber', 'Value': str(run_number)}
        ]
    }
    
    # Add subnet and security groups if provided
    if EMR_SUBNET_ID:
        cluster_config['Instances']['Ec2SubnetId'] = EMR_SUBNET_ID
    if EMR_MASTER_SG and EMR_SLAVE_SG:
        cluster_config['Instances']['EmrManagedMasterSecurityGroup'] = EMR_MASTER_SG
        cluster_config['Instances']['EmrManagedSlaveSecurityGroup'] = EMR_SLAVE_SG
    
#     # Create bootstrap script
#     bootstrap_script = '''#!/bin/bash
# # Bootstrap script for forecasting dependencies
# echo "Installing forecast dependencies..."

# # Install Python packages needed for forecasting
# sudo python3 -m pip install --upgrade pip
# sudo python3 -m pip install pandas==1.5.3 numpy==1.24.3 boto3==1.26.137
# sudo python3 -m pip install pyarrow==12.0.0 dask[complete]==2023.5.0
# sudo python3 -m pip install prophet==1.1.4 statsmodels==0.14.0
# sudo python3 -m pip install joblib==1.3.1

# # Fix potential click version issues
# sudo python3 -m pip install "click<8.0" --force-reinstall

# echo "✅ Forecast dependencies installed"
# '''
    
#     # Upload bootstrap script
#     s3.put_object(
#         Bucket=S3_BUCKET_DATA,
#         Key='scripts/bootstrap_forecast.sh',
#         Body=bootstrap_script
#     )
    
    # Create cluster
    print(f"Creating EMR cluster (Run #{run_number})...")
    response = emr.run_job_flow(**cluster_config)
    cluster_id = response['JobFlowId']
    
    print(f"Cluster ID: {cluster_id}")
    print("Waiting for cluster to be ready...")
    
    # Wait for cluster to be ready
    waiter = emr.get_waiter('cluster_running')
    waiter.wait(ClusterId=cluster_id)
    
    # Get cluster details
    cluster_details = emr.describe_cluster(ClusterId=cluster_id)
    master_dns = cluster_details['Cluster']['MasterPublicDnsName']
    
    print(f"✅ Cluster ready!")
    print(f"Master DNS: {master_dns}")
    
    # Set up Dask on the cluster
    print("\nSetting up Dask...")
    
    # Upload Dask startup script
    dask_script = f'''#!/bin/bash
# Start Dask scheduler on master
MASTER_IP=$(hostname -i)
nohup dask-scheduler --host 0.0.0.0 --port 8786 --dashboard-address 8787 > ~/dask-scheduler.log 2>&1 &

echo "Waiting for scheduler to start..."
sleep 15

# Get worker nodes and start workers
WORKERS=$(yarn node -list 2>/dev/null | grep RUNNING | grep -v Total | awk '{{print $1}}' | cut -d':' -f1)

echo "Starting workers on: $WORKERS"
for WORKER in $WORKERS; do
    ssh -o StrictHostKeyChecking=no $WORKER "nohup dask-worker $MASTER_IP:8786 --nthreads 4 --memory-limit 14GB > ~/dask-worker.log 2>&1 &"
    sleep 3
done

# Save cluster info
cat > ~/cluster_info.json << EOF
{{
    "cluster_id": "{cluster_id}",
    "master_dns": "{master_dns}",
    "master_ip": "$MASTER_IP",
    "dask_scheduler": "$MASTER_IP:8786",
    "created_at": "$(date -Iseconds)",
    "run_number": {run_number}
}}
EOF

# Upload to S3
aws s3 cp ~/cluster_info.json s3://{S3_BUCKET_DATA}/cluster/current_cluster_info.json

echo "✅ Dask cluster started"
'''
    
    # Save script to S3
    s3.put_object(
        Bucket=S3_BUCKET_DATA,
        Key='scripts/start_dask.sh',
        Body=dask_script
    )
    
    # Run Dask setup as EMR step
    dask_step = {
        'Name': 'Setup Dask Cluster',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'bash', '-c',
                f'aws s3 cp s3://{S3_BUCKET_DATA}/scripts/start_dask.sh . && chmod +x start_dask.sh && ./start_dask.sh'
            ]
        }
    }
    
    emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[dask_step])
    
    # Wait for Dask to be ready
    print("Waiting for Dask to start...")
    time.sleep(45)
    
    # Store cluster info in XCom
    cluster_info = {
        'cluster_id': cluster_id,
        'master_dns': master_dns,
        'dask_scheduler': f"{master_dns}:8786",
        'region': AWS_REGION,
        'run_number': run_number
    }
    
    context['task_instance'].xcom_push(key='cluster_info', value=cluster_info)
    
    print(f"\n✅ EMR cluster ready for forecasting (Run #{run_number})")
    
    return cluster_info

start_cluster_task = PythonOperator(
    task_id='start_emr_cluster',
    python_callable=start_forecast_emr_cluster,
    provide_context=True,
    dag=dag
)

# Task 3: Generate Forecasts Using Pre-trained Models
def run_ensemble_forecasting(**context):
    """Run ensemble forecasting using the provided ensemble_forecasting.py"""
    import sys
    sys.path.append(os.path.join(PROJECT_PATH, 'src'))
    
    from ensemble_forecasting import EnsembleForecaster
    import boto3
    import json
    
    print("=== Running Ensemble Forecasting ===")
    
    # Get cluster info from XCom
    cluster_info = context['task_instance'].xcom_pull(task_ids='start_emr_cluster', key='cluster_info')
    run_number = cluster_info['run_number']
    
    print(f"Run #{run_number}: Starting ensemble forecasting...")
    
    scheduler_address = cluster_info['dask_scheduler']
    print(f"Using Dask cluster at: {scheduler_address}")
    
    # Initialize forecaster
    forecaster = EnsembleForecaster(scheduler_address)
    
    # Generate forecasts for 72 hours
    print("\nGenerating 72-hour forecasts...")
    forecast_df, summary = forecaster.generate_ensemble_forecasts(horizon_hours=72)
    
    # Add run number to summary
    summary['run_number'] = run_number
    summary['cluster_id'] = cluster_info['cluster_id']
    
    print(f"\n✅ Run #{run_number} Forecasting complete!")
    print(f"Generated {summary['total_predictions']:,} predictions")
    print(f"Saved to: {summary['s3_location']}")
    
    # Display statistics
    print("\nForecast Statistics:")
    print(f"Average hourly forecast: {forecast_df['ensemble_forecast'].mean():.1f} orders")
    print(f"Peak hour forecast: {forecast_df['ensemble_forecast'].max()} orders")
    print(f"Minimum hour forecast: {forecast_df['ensemble_forecast'].min()} orders")
    
    # Store summary in XCom
    context['task_instance'].xcom_push(key='forecast_summary', value=summary)
    
    return summary

forecast_task = PythonOperator(
    task_id='generate_ensemble_forecasts',
    python_callable=run_ensemble_forecasting,
    provide_context=True,
    dag=dag
)

# Task 4: Cleanup EMR Cluster
cleanup_task = BashOperator(
    task_id='cleanup_emr_cluster',
    bash_command="""
    echo "=== Cleaning Up EMR Cluster ==="
    
    # Get cluster ID from XCom
    cluster_info='{{ ti.xcom_pull(task_ids="start_emr_cluster", key="cluster_info") }}'
    
    cluster_id=$(echo "$cluster_info" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('cluster_id', ''))")
    
    if [ ! -z "$cluster_id" ]; then
        echo "Terminating EMR cluster: $cluster_id"
        aws emr terminate-clusters --cluster-ids $cluster_id
        
        echo "✅ Cluster termination initiated"
    else
        echo "No cluster ID found"
    fi
    """,
    dag=dag
)

# Task 5: Final Summary
def final_summary(**context):
    """Generate final summary for this run"""
    import json
    from datetime import datetime
    
    # Get run information
    run_number = context['task_instance'].xcom_pull(task_ids='check_run_count', key='run_number')
    forecast_summary = context['task_instance'].xcom_pull(task_ids='generate_ensemble_forecasts', key='forecast_summary')
    
    print("\n" + "="*60)
    print(f"OVF FORECAST PIPELINE - RUN #{run_number} OF 3 COMPLETE")
    print("="*60)
    
    print(f"Completed at: {datetime.now()}")
    
    if forecast_summary:
        print(f"\nForecast Results:")
        print(f"  - Predictions generated: {forecast_summary['total_predictions']:,}")
        print(f"  - Regions forecasted: {forecast_summary['regions_forecasted']}")
        print(f"  - Time horizon: {forecast_summary['horizon_hours']} hours")
        print(f"  - S3 location: {forecast_summary['s3_location']}")
    
    if run_number >= 3:
        print("\n" + "="*60)
        print("ALL 3 SCHEDULED RUNS COMPLETED!")
        print("To run additional forecasts, trigger manually from Airflow UI")
        print("="*60)
    else:
        print(f"\nNext run will start in 1 hour (Run #{run_number + 1} of 3)")
    
    return True

summary_task = PythonOperator(
    task_id='final_summary',
    python_callable=final_summary,
    provide_context=True,
    dag=dag
)

# Task 6: Pause DAG After 3 Runs
def pause_dag_if_complete(**context):
    """Pause the DAG after 3 runs"""
    from airflow.models import DagModel
    
    run_number = context['task_instance'].xcom_pull(task_ids='check_run_count', key='run_number')
    
    if run_number >= 3:
        print(f"Completed {run_number} runs. Pausing DAG...")
        
        # Pause the DAG
        DagModel.get_dagmodel('ovf_forecast_limited_runs').set_is_paused(is_paused=True)
        
        print("✅ DAG has been paused. Manual trigger required for future runs.")
    else:
        print(f"Run {run_number} of 3 complete. DAG will continue.")
    
    return True

pause_dag_task = PythonOperator(
    task_id='pause_dag_if_complete',
    python_callable=pause_dag_if_complete,
    provide_context=True,
    dag=dag
)

# Define task dependencies
check_count_task >> start_cluster_task >> forecast_task >> cleanup_task >> summary_task >> pause_dag_task