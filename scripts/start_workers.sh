#!/bin/bash
# Run this to start workers on all nodes

MASTER_IP=$(hostname -i)
WORKERS=$(yarn node -list 2>/dev/null | grep RUNNING | grep -v Total | awk '{print $1}' | cut -d':' -f1)

echo "Starting workers on: $WORKERS"
for WORKER in $WORKERS; do
    echo "Starting worker on $WORKER..."
    ssh -o StrictHostKeyChecking=no $WORKER "
        pkill -f dask-worker 2>/dev/null
        sleep 2
        nohup dask-worker $MASTER_IP:8786 --nthreads 4 --memory-limit 14GB > ~/dask-worker.log 2>&1 &
        sleep 3
        ps aux | grep -v grep | grep dask-worker && echo '✓ Worker started' || echo '✗ Worker failed'
    "
done