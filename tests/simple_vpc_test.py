import yaml

with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
    template = yaml.safe_load(f.read())

# Check if VPC endpoint resources exist
resources = template.get('Resources', {})
vpc_endpoints = ['SourceS3VpcEndpoint', 'TargetS3VpcEndpoint']

for endpoint in vpc_endpoints:
    if endpoint in resources:
        endpoint_config = resources[endpoint]
        properties = endpoint_config.get('Properties', {})
        
        # Check VPC endpoint type
        vpc_type = properties.get('VpcEndpointType')
        service_name = properties.get('ServiceName')
        
        with open(f'{endpoint}_validation.txt', 'w') as f:
            f.write(f"Resource: {endpoint}\n")
            f.write(f"Type: {endpoint_config.get('Type')}\n")
            f.write(f"Condition: {endpoint_config.get('Condition')}\n")
            f.write(f"VpcEndpointType: {vpc_type}\n")
            f.write(f"ServiceName: {service_name}\n")
            f.write(f"Has PolicyDocument: {'PolicyDocument' in properties}\n")
            f.write(f"Has RouteTableIds: {'RouteTableIds' in properties}\n")

# Check conditions
conditions = template.get('Conditions', {})
with open('conditions_validation.txt', 'w') as f:
    f.write(f"Total conditions: {len(conditions)}\n")
    for name, value in conditions.items():
        f.write(f"{name}: {type(value).__name__}\n")

print("Validation files created successfully")