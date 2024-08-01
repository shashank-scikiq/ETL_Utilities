import yaml

def load_env_file(env_file_path):
    env_vars = {}
    with open(env_file_path, 'r') as file:
        for line in file:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            key, value = map(str.strip, line.split('=', 1))
            # Remove quotes if they are present
            value = value.strip('"')
            env_vars[key] = value
    return env_vars

def load_yaml_file(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        # Use safe_load_all to handle multiple documents
        return list(yaml.safe_load_all(file))

def save_yaml_file(yaml_data, yaml_file_path):
    with open(yaml_file_path, 'w') as file:
        # Write each document separated by '---'
        yaml.safe_dump_all(yaml_data, file, default_flow_style=False)

def extract_env_vars_from_yaml(yaml_data):
    env_vars = {}
    for document in yaml_data:
        containers = document.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
        for container in containers:
            env = container.get('env', [])
            for item in env:
                try:
                    env_vars[item['name']] = item['value']
                except KeyError as e:
                    print(f"KeyError: {e} - item: {item}")
                    # Provide structure of the item for debugging
                    print(f"Unexpected item format: {item}")
    return env_vars

def generate_proposed_changes(yaml_data, env_vars):
    proposed_changes = []
    
    for document in yaml_data:
        containers = document.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
        for container in containers:
            env = container.get('env', [])
            existing_vars = {item['name']: item['value'] for item in env if 'name' in item and 'value' in item}
            
            # Add or update env vars
            for key, value in env_vars.items():
                if key in existing_vars:
                    if existing_vars[key] != value:
                        proposed_changes.append((key, existing_vars[key], value))
                else:
                    proposed_changes.append((key, None, value))
                    env.append({'name': key, 'value': value})
    
    return proposed_changes

def update_yaml_with_selected_changes(yaml_data, selected_changes):
    for document in yaml_data:
        containers = document.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
        for container in containers:
            env = container.get('env', [])
            existing_vars = {item['name']: item['value'] for item in env if 'name' in item and 'value' in item}
            
            for key, old_value, new_value in selected_changes:
                if old_value is None:
                    # Add the new environment variable
                    env.append({'name': key, 'value': new_value})
                else:
                    # Update the existing environment variable
                    for item in env:
                        if item['name'] == key:
                            item['value'] = new_value

def main(env_file_path, yaml_file_path):
    # Load the environment variables from the .env file
    env_vars = load_env_file(env_file_path)
    
    # Load the Kubernetes YAML file (handles multiple documents)
    yaml_data = load_yaml_file(yaml_file_path)
    
    # Extract existing environment variables from the YAML data
    existing_env_vars = extract_env_vars_from_yaml(yaml_data)
    
    # Compare and report differences
    differences = {}
    for key, value in env_vars.items():
        if key in existing_env_vars:
            if existing_env_vars[key] != value:
                differences[key] = {
                    'env_file_value': value,
                    'yaml_value': existing_env_vars[key]
                }
        else:
            # Missing in YAML
            differences[key] = {
                'env_file_value': value,
                'yaml_value': None
            }

    # Print differences and collect proposed changes
    if differences:
        print("Differences found:")
        for key, diff in differences.items():
            print(f"{key}:")
            if diff['yaml_value'] is None:
                print(f"  - Missing in YAML, added value: {diff['env_file_value']}")
            else:
                print(f"  - Value in .env file: {diff['env_file_value']}")
                print(f"  - Value in YAML file: {diff['yaml_value']}")
        
        # Generate proposed changes
        proposed_changes = generate_proposed_changes(yaml_data, env_vars)
        
        if proposed_changes:
            print("\nProposed Changes:")
            for idx, (key, old_value, new_value) in enumerate(proposed_changes):
                if old_value is None:
                    print(f"{idx + 1}: Add - {key} = {new_value}")
                else:
                    print(f"{idx + 1}: Update - {key}: {old_value} -> {new_value}")

            selected_indices = input("\nEnter the numbers of the changes you want to apply (comma-separated, e.g., 1,3,4): ")
            selected_indices = set(map(int, selected_indices.split(',')))
            
            selected_changes = [proposed_changes[i - 1] for i in selected_indices if 1 <= i <= len(proposed_changes)]
            
            update_yaml_with_selected_changes(yaml_data, selected_changes)
            
            save_yaml_file(yaml_data, yaml_file_path)
            print(f"Updated YAML file saved to {yaml_file_path}")
        else:
            print("No proposed changes available.")
    else:
        print("No differences found between .env file and YAML file.")

if __name__ == "__main__":
    # Set the paths to your .env file and YAML file
    env_file = "D:\\ONDC\\ETL_Utilities\\.env"
    k8s_prod = "D:\\ONDC\\ETL_Utilities\\k8-deployment.yaml"
    
    main(env_file, k8s_prod)
