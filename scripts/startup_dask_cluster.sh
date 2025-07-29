#!/bin/bash
# Startup script to initialize Dask cluster after EMR is ready
# Run this on master node after cluster starts

echo "=== Starting Dask Cluster Setup ==="

# 1. Verify packages are installed
echo "Checking package installations..."
python3 -c "
import dask, distributed, prophet, statsmodels
print('✓ All packages available')
print(f'Dask version: {dask.__version__}')
" || {
    echo "❌ Packages missing! Running install..."
    sudo python3 -m pip install dask==2023.5.0 distributed==2023.5.0 prophet==1.1.4 statsmodels==0.14.0
}

# 2. Fix click issue if needed
sudo python3 -m pip install "click<8.0" --force-reinstall

# 3. Start Dask Scheduler
echo "Starting Dask scheduler..."
pkill -f dask-scheduler 2>/dev/null
sleep 2

MASTER_IP=$(hostname -i)
nohup dask-scheduler --host 0.0.0.0 --port 8786 --dashboard-address 8787 > ~/dask-scheduler.log 2>&1 &

echo "Waiting for scheduler to start..."
sleep 10

# 4. Verify scheduler is running
if ps aux | grep -v grep | grep dask-scheduler; then
    echo "✓ Dask scheduler running at $MASTER_IP:8786"
else
    echo "❌ Scheduler failed to start!"
    tail -20 ~/dask-scheduler.log
    exit 1
fi

# 5. Get worker nodes and save info
echo "Getting worker node information..."
WORKERS=$(yarn node -list 2>/dev/null | grep RUNNING | grep -v Total | awk '{print $1}' | cut -d':' -f1)

cat > ~/start_workers.sh << EOF
#!/bin/bash
# Run this to start workers on all nodes

MASTER_IP=$MASTER_IP
WORKERS="$WORKERS"

echo "Starting workers on: \$WORKERS"
for WORKER in \$WORKERS; do
    echo "Starting worker on \$WORKER..."
    ssh -o StrictHostKeyChecking=no \$WORKER "
        pkill -f dask-worker 2>/dev/null
        sleep 2
        nohup dask-worker \$MASTER_IP:8786 --nthreads 4 --memory-limit 14GB > ~/dask-worker.log 2>&1 &
        sleep 3
        ps aux | grep -v grep | grep dask-worker && echo '✓ Worker started' || echo '✗ Worker failed'
    "
done
EOF

chmod +x ~/start_workers.sh

# 6. Save cluster info to S3
CLUSTER_ID=$(cat /mnt/var/lib/info/job-flow.json | jq -r .jobFlowId)
MASTER_DNS=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)

# Convert workers to JSON array format
WORKERS_JSON=$(echo "$WORKERS" | tr '\n' ' ' | sed 's/ /", "/g' | sed 's/^/["/' | sed 's/, ""$/"]/')

python3 << EOF
import boto3
import json
from datetime import datetime

# Handle workers list properly
workers_str = '''$WORKERS'''
workers_list = [w.strip() for w in workers_str.split('\n') if w.strip()]

cluster_info = {
    'cluster_id': '$CLUSTER_ID',
    'master_dns': '$MASTER_DNS',
    'master_private_ip': '$MASTER_IP',
    'dask_scheduler': '$MASTER_IP:8786',
    'dask_dashboard': 'http://$MASTER_DNS:8787',
    'region': 'us-east-2',
    'created_at': datetime.now().isoformat(),
    'workers': workers_list
}

s3 = boto3.client('s3')
s3.put_object(
    Bucket='${S3_BUCKET_DATA:-grubhub-ovf-data-lake}',
    Key='cluster/current_cluster_info.json',
    Body=json.dumps(cluster_info, indent=2)
)
print('✓ Cluster info saved to S3')
print(f'Workers saved: {workers_list}')
EOF

echo "=== Setup Complete ==="
echo "Master IP: $MASTER_IP"
echo "Scheduler: $MASTER_IP:8786"
echo ""
echo "Next steps:"
echo "1. Run ./start_workers.sh to start all workers"
echo "2. Or manually SSH to each worker and start dask-worker"
echo ""
echo "Workers found: $WORKERS"
