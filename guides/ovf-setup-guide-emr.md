## Overview
Setup and start the EMR cluster to run the pipeline

## Prerequisites
- AWS Console access with IAM permissions

## Steps

1. **Navigate to Clusters on EMR Console**
   - Go to AWS Console → IAM → Clusters
   - Click "Create cluster"

2. **Select Name and applications**
   - Set name "Grubhub-OVF-cluster" or your choice
   - EMR Release: emr-6.10.0
   - Application bundle - Hadoop, Spark, Presto
   - Keep everything else default

3. **Cluster configuration**
   - Select Uniform instance groups
   - If available select t2.micro instances for both primary and core
   - If not then select m5.xlarge instances for both primary and core - this will incur some charges
   - Remove task nodes if any
   - Keep everything else default

4. **Cluster scaling and provisioning**
   - Choose set cluter size manually
   - Set Instance(s) size to 2

5. **Networking**
    - Select your VPC
    - Select your subnet, if any
    - Go to EC2 Security groups
    - Select the EMR-MASTER-SG for Primary node
    - Select the EMR-SLAVE-SG for Core node

6. **Boostrapping**
    - Make sure you have uploaded the bootstrap.sh file into your bucket
    - Add the bootstrap.sh file

7. **Cluster logs**
    - Select the logs directory in the data lake bucket

8. **Software settings**
    - Enter the following in the input box
    [
      {
        "Classification": "spark-defaults",
        "Properties": {
          "spark.driver.memory": "2g",
          "spark.executor.cores": "2",
          "spark.executor.memory": "2g"
        }
      },
      {
        "Classification": "presto-config",
        "Properties": {
          "query.max-memory": "2GB",
          "query.max-memory-per-node": "1GB"
        }
      },
      {
        "Classification": "presto-connector-hive",
        "Properties": {
          "hive.s3-file-system-type": "PRESTO"
        }
      }
    ]

9. **Security config and EC2 key pair**
    - Select grubhub-ovf-key-pair

10. **IAM roles**
    - For Amazon EMR select EMR_DefaultRole
    - For EC2 instance profile select EMR_EC2_DefaultRole

11. **Create Cluster**
    - Wait for cluster status to change to "Waiting" before using