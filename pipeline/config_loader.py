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
    
    # Validate required fields — the committed config holds a placeholder, so a
    # real contact must arrive via env or a local config edit (SEC requires one)
    email = config.get('user_agent', {}).get('email')
    if not email or email == 'your.email@example.com':
        raise ValueError(
            "SEC_USER_EMAIL environment variable must be set or "
            "user_agent.email must be configured in analysis_config.json "
            "(the committed placeholder is rejected)"
        )
    
    return config