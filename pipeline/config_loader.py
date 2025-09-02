"""
Configuration loader with environment variable support
"""
import os
import json
from pathlib import Path
from typing import Dict, Any

def load_config_with_env(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from JSON file and override with environment variables
    
    Environment variables:
    - SEC_USER_NAME: Override user_agent.name
    - SEC_USER_EMAIL: Override user_agent.email
    """
    # Load base config
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Override with environment variables if present
    if 'SEC_USER_NAME' in os.environ:
        if 'user_agent' not in config:
            config['user_agent'] = {}
        config['user_agent']['name'] = os.environ['SEC_USER_NAME']
    
    if 'SEC_USER_EMAIL' in os.environ:
        if 'user_agent' not in config:
            config['user_agent'] = {}
        config['user_agent']['email'] = os.environ['SEC_USER_EMAIL']
    
    # Validate required fields
    if 'user_agent' not in config or not config['user_agent'].get('email'):
        raise ValueError(
            "SEC_USER_EMAIL environment variable must be set or "
            "user_agent.email must be configured in analysis_config.json"
        )
    
    return config