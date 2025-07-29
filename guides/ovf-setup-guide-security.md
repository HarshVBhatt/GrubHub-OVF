# Step 4: EMR Security Group Setup Guide

## Overview
Configure security groups and network settings for EMR cluster access.

## Prerequisites
- AWS Console access with EC2/VPC permissions
- Completed local environment setup (Step 3)
- Your IP address for SSH access

## Steps

### 1. Find Your VPC and Subnet

1. **Navigate to VPC Console**
   - AWS Console → VPC → Your VPCs
   - Note the default VPC ID (vpc-xxxxxxxx)

2. **Find a subnet**
   - VPC → Subnets
   - Choose any subnet in your default VPC
   - Note the Subnet ID (subnet-xxxxxxxx)
   - Note the Availability Zone

### 2. Create EMR Security Groups

EMR needs two security groups: Master and Slave (Core/Task)

#### A. Create Master Security Group

1. **Go to EC2 Console**
   - EC2 → Security Groups → Create security group

2. **Basic details**
   - Security group name: `EMR-Master-SG`
   - Description: `Security group for EMR master node`
   - VPC: Select your default VPC

3. **Inbound rules** - Add these rules:

   | Type | Protocol | Port Range | Source | Description |
   |------|----------|------------|---------|-------------|
   | SSH | TCP | 22 | My IP | SSH access |
   | HTTPS | TCP | 443 | My IP | EMR console |
   | Custom TCP | TCP | 8088 | My IP | YARN ResourceManager |
   | Custom TCP | TCP | 8786 | My IP | Dask Scheduler |
   | Custom TCP | TCP | 8787 | My IP | Dask Dashboard |
   | Custom TCP | TCP | 8889 | My IP | PrestoDB |
   | All TCP | TCP | 0-65535 | [Slave-SG-ID] | Internal communication |

4. **Outbound rules**
   - Leave default (Allow all)

5. **Create security group**

#### B. Create Slave Security Group

1. **Create another security group**
   - Security group name: `EMR-Slave-SG`
   - Description: `Security group for EMR slave nodes`
   - VPC: Same as master

2. **Inbound rules**

   | Type | Protocol | Port Range | Source | Description |
   |------|----------|------------|---------|-------------|
   | All TCP | TCP | 0-65535 | [Master-SG-ID] | From master |
   | All TCP | TCP | 0-65535 | [Slave-SG-ID] | Between slaves |
   | SSH | TCP | 22 | My IP | SSH access (optional) |

3. **Create security group**

#### C. Update Master Security Group

1. **Go back to Master SG**
   - Add the Slave SG as source for internal communication
   - Edit inbound rules
   - Update the "All TCP" rule source to use Slave SG ID

### 3. Create or Update Default EMR Roles Security Groups

If EMR default security groups exist, update them. Otherwise, create:

1. **ElasticMapReduce-master**
   - Same rules as EMR-Master-SG above

2. **ElasticMapReduce-slave**
   - Same rules as EMR-Slave-SG above

### 4. Configure S3 Endpoint (Recommended)

To avoid data transfer charges:

1. **Go to VPC Console**
   - VPC → Endpoints → Create endpoint

2. **Configure endpoint**
   - Service name: Search for "s3"
   - Select: `com.amazonaws.[region].s3`
   - VPC: Your default VPC
   - Route tables: Select main route table
   - Policy: Full access

3. **Create endpoint**

### 5. Get Your IP Address

1. **Find your public IP**
   - Google "what is my ip"
   - Or run: `curl ifconfig.me`

2. **Note for security groups**
   - Use format: `YOUR.IP.ADDRESS/32`
   - Example: `203.0.113.12/32`

### 6. Document Network Information

Create `config/network_info.txt`:
```
VPC ID: vpc-xxxxxxxx
Subnet ID: subnet-xxxxxxxx
Availability Zone: us-east-1a
Master Security Group: sg-xxxxxxxx
Slave Security Group: sg-xxxxxxxx
My IP: 203.0.113.12/32
```


### 7. (Optional) Testing Security Groups

### Test Script
Create `test_security.py`:
```python
import boto3

ec2 = boto3.client('ec2')

# List security groups
response = ec2.describe_security_groups(
    Filters=[
        {'Name': 'group-name', 'Values': ['EMR-Master-SG', 'EMR-Slave-SG']}
    ]
)

for sg in response['SecurityGroups']:
    print(f"\nSecurity Group: {sg['GroupName']} ({sg['GroupId']})")
    print("Inbound Rules:")
    for rule in sg['IpPermissions']:
        print(f"  - Port {rule.get('FromPort', 'All')}: {rule.get('IpRanges', [])}")
```

## EMR Network Configuration

When creating EMR cluster, use:
```python
'Instances': {
    'Ec2SubnetId': 'subnet-xxxxxxxx',  # Your subnet
    'EmrManagedMasterSecurityGroup': 'sg-xxxxxxxx',  # Master SG
    'EmrManagedSlaveSecurityGroup': 'sg-xxxxxxxx',   # Slave SG
}
```

## Verification Checklist

- [ ] Default VPC and subnet identified
- [ ] Master security group created with correct rules
- [ ] Slave security group created with correct rules
- [ ] Security groups reference each other correctly
- [ ] Your IP address added to SSH rules
- [ ] S3 VPC endpoint created (optional)
- [ ] Network information documented

## Next Steps

Proceed to `05_copy_code_files.md` to set up the Python code files.

## Troubleshooting

### "Invalid IP address"
- Ensure IP is in CIDR format: `1.2.3.4/32`
- The `/32` means single IP address

### "Port already in use"
- Check for existing security groups
- EMR may have created default groups

### "Cannot connect to EMR"
- Verify your current IP matches security group
- Check EMR is in same VPC/subnet
- Ensure EMR cluster is running