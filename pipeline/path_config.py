"""
Path configuration helper for pipeline scripts
Ensures consistent path handling whether run from root or pipeline directory
"""

from pathlib import Path
from datetime import datetime
import json

def get_paths():
    """
    Get correct paths based on where the script is run from
    Returns dict with all common paths used by pipeline scripts
    """
    # Check if we're in the pipeline directory or root
    current_dir = Path.cwd()
    
    # If pipeline folder exists in current dir, we're at root
    if (current_dir / "pipeline").exists():
        # Running from root directory
        paths = {
            'root': current_dir,
            'data': current_dir / "data",
            'config': current_dir / "config",
            'output': current_dir / "output",
            'pipeline': current_dir / "pipeline",
            
            # Specific data subdirectories
            'filings': current_dir / "data" / "13f_filings" / "sec-edgar-filings",
            'mappings': current_dir / "data" / "mappings",
            'sec_data': current_dir / "data" / "sec_data",
            'cik_metadata': current_dir / "data" / "cik_metadata.json",
            
            # Config files
            'target_firms': current_dir / "config" / "target_firms.json",
            'analysis_config': current_dir / "config" / "analysis_config.json",
            'top_institutions': current_dir / "config" / "top_100_institutions.csv",
        }
    else:
        # Running from pipeline directory
        parent_dir = current_dir.parent
        paths = {
            'root': parent_dir,
            'data': parent_dir / "data",
            'config': parent_dir / "config",
            'output': parent_dir / "output",
            'pipeline': current_dir,
            
            # Specific data subdirectories
            'filings': parent_dir / "data" / "13f_filings" / "sec-edgar-filings",
            'mappings': parent_dir / "data" / "mappings",
            'sec_data': parent_dir / "data" / "sec_data",
            'cik_metadata': parent_dir / "data" / "cik_metadata.json",
            
            # Config files
            'target_firms': parent_dir / "config" / "target_firms.json",
            'analysis_config': parent_dir / "config" / "analysis_config.json",
            'top_institutions': parent_dir / "config" / "top_100_institutions.csv",
        }
    
    # Ensure critical directories exist
    paths['mappings'].mkdir(parents=True, exist_ok=True)
    paths['sec_data'].mkdir(parents=True, exist_ok=True)
    
    return paths

def get_output_dir(quarter, year):
    """Get the output directory for a specific quarter/year"""
    paths = get_paths()
    output_dir = paths['output'] / f"{quarter}_{year}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def get_latest_completed_quarter():
    """
    Determine the latest quarter with available 13F filings.
    13F filings are due 45 days after quarter end:
    - Q1 (Mar 31) → filings due May 15
    - Q2 (Jun 30) → filings due Aug 14
    - Q3 (Sep 30) → filings due Nov 14
    - Q4 (Dec 31) → filings due Feb 14
    
    Returns:
        Tuple of (year, quarter_number)
    """
    from datetime import timedelta
    
    today = datetime.now()
    
    # Define quarter end dates and filing deadlines for current year
    current_year = today.year
    quarters = [
        {'quarter': 1, 'year': current_year, 'end_date': datetime(current_year, 3, 31), 'filing_deadline': datetime(current_year, 5, 15)},
        {'quarter': 2, 'year': current_year, 'end_date': datetime(current_year, 6, 30), 'filing_deadline': datetime(current_year, 8, 14)},
        {'quarter': 3, 'year': current_year, 'end_date': datetime(current_year, 9, 30), 'filing_deadline': datetime(current_year, 11, 14)},
        {'quarter': 4, 'year': current_year - 1, 'end_date': datetime(current_year - 1, 12, 31), 'filing_deadline': datetime(current_year, 2, 14)},
    ]
    
    # Add previous year Q4 if we're early in the year
    if today.month <= 2:
        quarters.append({
            'quarter': 4, 
            'year': current_year - 2, 
            'end_date': datetime(current_year - 2, 12, 31), 
            'filing_deadline': datetime(current_year - 1, 2, 14)
        })
    
    # Find the most recent quarter where filing deadline has passed
    latest_available = None
    for q in sorted(quarters, key=lambda x: x['filing_deadline'], reverse=True):
        if today >= q['filing_deadline']:
            latest_available = q
            break
    
    if latest_available:
        return latest_available['year'], latest_available['quarter']
    else:
        # Fallback to previous year Q4 if nothing else available
        return current_year - 1, 4

def get_current_quarter():
    """
    Get the current quarter based on today's date.
    
    Returns:
        Tuple of (year, quarter_number)
    """
    today = datetime.now()
    current_month = today.month
    current_year = today.year
    
    if current_month <= 3:
        return current_year, 1
    elif current_month <= 6:
        return current_year, 2
    elif current_month <= 9:
        return current_year, 3
    else:
        return current_year, 4

def get_default_quarter_year():
    """
    Get the default quarter and year for analysis.
    First checks config, then falls back to latest completed quarter.
    
    Returns:
        Tuple of (quarter_string, year) e.g., ('Q2', 2025)
    """
    paths = get_paths()
    config_path = paths['analysis_config']
    
    # Try to load from config
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
            if 'quarter' in config and 'year' in config:
                return config['quarter'], config['year']
    
    # Fall back to latest completed quarter
    year, quarter_num = get_latest_completed_quarter()
    return f'Q{quarter_num}', year

def get_latest_downloaded_quarter():
    """
    Get the most recent quarter that was downloaded based on download reports.
    Determines the latest by quarter/year, not file modification time.
    
    Returns:
        Tuple of (quarter_string, year) e.g., ('Q2', 2025)
        Returns None if no download reports found
    """
    paths = get_paths()
    filings_dir = paths['data'] / '13f_filings'
    
    # Look for download report files
    download_reports = list(filings_dir.glob('download_report_Q*_*.txt'))
    
    if not download_reports:
        # Check for latest_download.json as fallback
        latest_file = filings_dir / 'latest_download.json'
        if latest_file.exists():
            with open(latest_file, 'r') as f:
                data = json.load(f)
                return data['quarter'], data['year']
        return None
    
    # Parse all download reports and find the latest by quarter/year
    import re
    quarters_found = []
    
    for report in download_reports:
        match = re.search(r'download_report_(Q\d)_(\d{4})\.txt', report.name)
        if match:
            quarter = match.group(1)
            year = int(match.group(2))
            quarter_num = int(quarter[1])
            # Convert to a comparable value: year * 10 + quarter_num
            quarters_found.append((year * 10 + quarter_num, quarter, year))
    
    if quarters_found:
        # Get the latest quarter by year and quarter number
        latest = max(quarters_found, key=lambda x: x[0])
        return latest[1], latest[2]
    
    return None

def get_data_driven_defaults():
    """
    Get the default quarter and year based on available data.
    Priority: Downloaded data → Latest completed quarter (with filing deadline) → Config
    
    Returns:
        Tuple of (quarter_string, year) e.g., ('Q2', 2025)
    """
    # First check what was downloaded
    downloaded = get_latest_downloaded_quarter()
    if downloaded:
        return downloaded
    
    # Use latest completed quarter (accounting for 45-day filing deadline)
    year, quarter_num = get_latest_completed_quarter()
    return f'Q{quarter_num}', year