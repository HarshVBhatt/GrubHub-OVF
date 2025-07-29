#!/bin/bash
# EMR Bootstrap script for OVF pipeline - Simplified version
# Just installs packages, no complex startup logic

# Log to file
exec > /tmp/bootstrap.log 2>&1

echo "=== OVF Bootstrap Started at $(date) ==="

# Update and install system packages
echo "Installing system dependencies..."
sudo yum install -y python3 python3-devel python3-pip gcc gcc-c++ atlas-devel

# Make sure pip is up to date
echo "Upgrading pip..."
sudo python3 -m ensurepip --upgrade || true
sudo python3 -m pip install --upgrade pip

# Install packages in small batches to avoid memory issues
echo "Installing numpy first (required by many packages)..."
sudo python3 -m pip install numpy

echo "Installing core data packages..."
sudo python3 -m pip install \
    pandas \
    pyarrow \
    boto3 \
    s3fs \
    pyhive

echo "Installing ML packages..."
sudo python3 -m pip install \
    scikit-learn \
    joblib

echo "Installing statsmodels..."
sudo python3 -m pip install statsmodels

echo "Installing Prophet dependencies..."
sudo python3 -m pip install \
    Cython \
    cmdstanpy \
    pystan

echo "Installing Prophet..."
sudo python3 -m pip install prophet

echo "Installing Dask..."
sudo python3 -m pip install \
    dask \
    distributed \
    bokeh \
    click==7.1.2

echo "Installing PrestoDB client..."
sudo python3 -m pip install pyhive[presto]

echo "=== Bootstrap Completed at $(date) ==="