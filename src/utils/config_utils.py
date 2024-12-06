import yaml
import os
from pathlib import Path

def load_config(project_name = 'assessment-rent-airbnb'):
    """Load configuration from a YAML file."""
    home = str(Path.home())
    config_path = os.path.join(home, project_name, 'resources/config.yml')
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    
    return config

config = load_config()

def get_output_dir(layer, config = config):
    """Returns the output directory for a given layer."""
    return os.path.join(config['base_dir'], config['paths']['output_dirs'][layer])



def get_file_path(dir, file_type, file, layer = 'bronze'):
    """Returns the file path for a given file."""
    if dir == 'data':
        dir = os.path.join(config['base_dir'], config['paths']['data_dir'])
    elif dir == 'output':
        dir = os.path.join(config['base_dir'], config['paths']['output_dirs'][layer])
    return os.path.join(dir, config['files'][file_type][file])

def get_parquet_path(layer, file):
    file_type = layer + '_parquet'
    return get_file_path('output', file_type, file, layer)
