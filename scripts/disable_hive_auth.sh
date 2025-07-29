#!/bin/bash
# Script to disable Hive authorization on EMR

echo "=== Disabling Hive Authorization ==="

# Stop Hive services
echo "Stopping Hive services..."
sudo stop hive-server2 2>/dev/null || true
sudo stop hive-metastore 2>/dev/null || true

# Backup original configuration
sudo cp /etc/hive/conf/hive-site.xml /etc/hive/conf/hive-site.xml.backup

# Update hive-site.xml to disable authorization
echo "Updating Hive configuration..."
sudo python3 << 'EOF'
import xml.etree.ElementTree as ET

# Parse the XML file
tree = ET.parse('/etc/hive/conf/hive-site.xml')
root = tree.getroot()

# Properties to set
properties_to_set = {
    'hive.security.authorization.enabled': 'false',
    'hive.security.metastore.authorization.manager': 'org.apache.hadoop.hive.ql.security.authorization.DefaultHiveMetastoreAuthorizationProvider',
    'hive.security.authenticator.manager': 'org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator',
    'hive.metastore.pre.event.listeners': '',
    'hive.server2.enable.doAs': 'false',
    'hive.users.in.admin.role': 'hadoop,hive',
    'hive.metastore.execute.setugi': 'false'
}

# Find existing properties and update them
existing_props = {}
for prop in root.findall('.//property'):
    name_elem = prop.find('name')
    if name_elem is not None:
        existing_props[name_elem.text] = prop

# Update or add properties
for prop_name, prop_value in properties_to_set.items():
    if prop_name in existing_props:
        # Update existing property
        value_elem = existing_props[prop_name].find('value')
        if value_elem is not None:
            value_elem.text = prop_value
    else:
        # Add new property
        new_prop = ET.SubElement(root, 'property')
        ET.SubElement(new_prop, 'name').text = prop_name
        ET.SubElement(new_prop, 'value').text = prop_value

# Write the updated XML
tree.write('/tmp/hive-site.xml')
EOF

# Move the updated file
sudo mv /tmp/hive-site.xml /etc/hive/conf/hive-site.xml

# Also update hiveserver2-site.xml if it exists
if [ -f /etc/hive/conf/hiveserver2-site.xml ]; then
    echo "Updating hiveserver2 configuration..."
    sudo bash -c 'cat > /etc/hive/conf/hiveserver2-site.xml << EOF
<configuration>
  <property>
    <name>hive.security.authorization.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
</configuration>
EOF'
fi

# Start Hive services
echo "Starting Hive services..."
sudo service hive-metastore start
sleep 10
sudo service hive-server2 start 2>/dev/null || true

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 20

# Test the connection
echo "Testing Hive connection..."
hive -e "SHOW DATABASES;" && echo "✅ Hive is working" || echo "❌ Hive test failed"

# For Presto, update the Hive connector properties
if [ -d /etc/presto/conf/catalog ]; then
    echo "Updating Presto Hive connector..."
    sudo bash -c 'cat > /etc/presto/conf/catalog/hive.properties << EOF
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
hive.allow-drop-table=true
hive.allow-rename-table=true
hive.allow-add-column=true
hive.allow-drop-column=true
hive.allow-rename-column=true
EOF'
    
    # Restart Presto
    echo "Restarting Presto..."
    sudo service presto-server restart
    sleep 20
fi

echo "=== Authorization disabled successfully ==="
echo ""
echo "You should now be able to:"
echo "- Drop tables"
echo "- Create tables" 
echo "- Modify schemas"
echo ""
echo "Note: This removes all security. Only use in development!"