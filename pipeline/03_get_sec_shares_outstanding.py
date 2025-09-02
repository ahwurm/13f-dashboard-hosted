"""
Optimized SEC EDGAR company facts extraction for shares outstanding
Only extracts data for companies we actually need, avoiding 17GB of unnecessary files
"""

import os
import json
import requests
import zipfile
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Set
import time
from path_config import get_paths
from config_loader import load_config_with_env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptimizedSECSharesFetcher:
    """Optimized fetcher that only extracts needed companies from SEC EDGAR"""
    
    def __init__(self, cache_dir: Path = None):
        """Initialize the optimized SEC data fetcher"""
        paths = get_paths()
        self.cache_dir = cache_dir or paths['sec_data']
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.companyfacts_zip = self.cache_dir / "companyfacts.zip"
        self.consolidated_shares_file = self.cache_dir / "sec_shares_consolidated.json"
        
        # Mapping files to determine what we need
        self.mappings_dir = paths['mappings']
        
        # Load configuration for User-Agent
        self.config = self.load_config(paths)
        
        # Load configuration for User-Agent
        self.config = self.load_config(paths)
        
    def get_needed_ciks(self) -> Set[str]:
        """
        Get list of CIKs we actually need from our pipeline
        
        Returns:
            Set of CIK strings (without leading zeros)
        """
        needed_ciks = set()
        
        # Method 1: Get from ticker to CIK mapping
        ticker_map_file = self.mappings_dir / "ticker_to_cik_map.json"
        if ticker_map_file.exists():
            logger.info(f"Loading CIKs from {ticker_map_file}")
            with open(ticker_map_file, 'r', encoding='utf-8') as f:
                ticker_data = json.load(f)
                for ticker, info in ticker_data.items():
                    if 'cik' in info:
                        # Remove leading zeros
                        cik = str(info['cik']).lstrip('0')
                        needed_ciks.add(cik)
            logger.info(f"Found {len(needed_ciks)} CIKs from ticker mappings")
        
        # Method 2: Get from CUSIP to shares mapping (if it exists)
        cusip_shares_file = self.mappings_dir / "cusip_to_shares_complete.json"
        if cusip_shares_file.exists():
            logger.info(f"Loading CIKs from {cusip_shares_file}")
            with open(cusip_shares_file, 'r', encoding='utf-8') as f:
                cusip_data = json.load(f)
                if 'mappings' in cusip_data:
                    for cusip, info in cusip_data['mappings'].items():
                        if 'cik' in info:
                            cik = str(info['cik']).lstrip('0')
                            needed_ciks.add(cik)
            logger.info(f"Total unique CIKs needed: {len(needed_ciks)}")
        
        # If we have no CIKs, warn but continue (will extract all)
        if not needed_ciks:
            logger.warning("No CIK list found. Will need to run full extraction first.")
        
        return needed_ciks
    
    def load_config(self, paths: dict) -> dict:
        """Load configuration from analysis_config.json with environment variable support"""
        config_file = paths['analysis_config']
        
        if not config_file.exists():
            logger.warning(f"Config file not found at {config_file}. Using defaults.")
            return {
                'user_agent': {
                    'name': '13F Analysis Tool',
                    'email': 'analysis@example.com'
                },
                'download': {
                    'max_retries': 3,
                    'timeout_seconds': 30
                }
            }
        
        try:
            config = load_config_with_env(config_file)
            logger.info(f"Loaded configuration from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}
    
    def download_companyfacts(self, force: bool = False) -> bool:
        """
        Download the SEC EDGAR companyfacts.zip file
        
        Args:
            force: Force re-download even if file exists
            
        Returns:
            True if successful, False otherwise
        """
        # Check if already downloaded
        if self.companyfacts_zip.exists() and not force:
            file_size = self.companyfacts_zip.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"companyfacts.zip already exists ({file_size:.1f} MB)")
            return True
        
        url = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
        
        logger.info("Downloading SEC EDGAR companyfacts.zip...")
        logger.info("This is a large file (~1.3 GB) and may take several minutes...")
        
        try:
            # Add proper SEC-compliant headers
            user_agent = self.config.get('user_agent', {})
            headers = {
                'User-Agent': f"{user_agent.get('name', '13F Analysis')} {user_agent.get('email', 'analysis@example.com')}",
                'Accept-Encoding': 'gzip, deflate',
                'Host': 'www.sec.gov'
            }
            
            # Download with streaming to handle large file
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()
            
            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            
            # Download in chunks with progress reporting
            chunk_size = 8192
            downloaded = 0
            
            with open(self.companyfacts_zip, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Report progress every 10 MB
                        if downloaded % (10 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size) * 100 if total_size > 0 else 0
                            logger.info(f"Downloaded {downloaded / (1024*1024):.1f} MB ({progress:.1f}%)")
            
            logger.info(f"Download complete: {downloaded / (1024*1024):.1f} MB")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading companyfacts.zip: {e}")
            return False
    
    def extract_shares_from_json(self, data: Dict, cik: str) -> Optional[Dict]:
        """
        Extract shares outstanding from a company's JSON data
        Prioritizes 2025 data, falls back to late 2024 if needed
        
        Args:
            data: Parsed JSON data for a company
            cik: CIK number (for reference)
            
        Returns:
            Dictionary with shares data or None
        """
        try:
            # Get entity name
            entity_name = data.get('entityName', 'Unknown')
            
            # Look for shares outstanding in facts
            shares_outstanding = None
            shares_date = None
            
            # Check us-gaap facts
            if 'facts' in data and 'us-gaap' in data['facts']:
                us_gaap = data['facts']['us-gaap']
                
                # Look for CommonStockSharesOutstanding
                if 'CommonStockSharesOutstanding' in us_gaap:
                    shares_data = us_gaap['CommonStockSharesOutstanding']
                    
                    # Get the most recent value from 2025 or late 2024
                    if 'units' in shares_data and 'shares' in shares_data['units']:
                        shares_list = shares_data['units']['shares']
                        
                        # Filter for 2025 data first
                        shares_2025 = [s for s in shares_list if s.get('end', '') >= '2025-01-01']
                        
                        # If no 2025 data, try late 2024 (Q3/Q4)
                        if not shares_2025:
                            shares_2025 = [s for s in shares_list if s.get('end', '') >= '2024-07-01']
                        
                        # Sort by end date to get most recent
                        if shares_2025:
                            shares_2025.sort(key=lambda x: x.get('end', ''), reverse=True)
                            most_recent = shares_2025[0]
                            shares_outstanding = most_recent.get('val')
                            shares_date = most_recent.get('end')
                
                # Alternative: WeightedAverageNumberOfSharesOutstandingBasic
                if not shares_outstanding and 'WeightedAverageNumberOfSharesOutstandingBasic' in us_gaap:
                    shares_data = us_gaap['WeightedAverageNumberOfSharesOutstandingBasic']
                    
                    if 'units' in shares_data and 'shares' in shares_data['units']:
                        shares_list = shares_data['units']['shares']
                        
                        # Filter for 2025 or late 2024 data
                        shares_recent = [s for s in shares_list if s.get('end', '') >= '2025-01-01']
                        if not shares_recent:
                            shares_recent = [s for s in shares_list if s.get('end', '') >= '2024-07-01']
                        
                        if shares_recent:
                            shares_recent.sort(key=lambda x: x.get('end', ''), reverse=True)
                            most_recent = shares_recent[0]
                            shares_outstanding = most_recent.get('val')
                            shares_date = most_recent.get('end')
            
            if shares_outstanding:
                return {
                    'entity_name': entity_name,
                    'shares_outstanding': shares_outstanding,
                    'shares_date': shares_date,
                    'last_updated': datetime.now().isoformat()
                }
            
        except Exception as e:
            logger.debug(f"Error extracting shares for CIK {cik}: {e}")
        
        return None
    
    def extract_selective_companyfacts(self, needed_ciks: Set[str] = None) -> Dict[str, Dict]:
        """
        Extract only needed companies directly from ZIP file
        
        Args:
            needed_ciks: Set of CIKs to extract (None = extract all)
            
        Returns:
            Dictionary mapping CIK to shares data
        """
        if not self.companyfacts_zip.exists():
            logger.error("companyfacts.zip not found. Please download first.")
            return {}
        
        results = {}
        extracted_count = 0
        processed_count = 0
        data_2025_count = 0
        data_2024_count = 0
        
        logger.info("Starting selective extraction from companyfacts.zip...")
        logger.info("Prioritizing 2025 data, falling back to Q3/Q4 2024 if needed")
        if needed_ciks:
            logger.info(f"Looking for {len(needed_ciks)} specific companies")
        else:
            logger.info("Extracting all companies (no filter specified)")
        
        try:
            with zipfile.ZipFile(self.companyfacts_zip, 'r') as zf:
                # Get list of all files in the ZIP
                all_files = zf.namelist()
                total_files = len(all_files)
                logger.info(f"ZIP contains {total_files} files")
                
                for filename in all_files:
                    if filename.startswith("CIK") and filename.endswith(".json"):
                        # Extract CIK from filename (e.g., "CIK0000320193.json")
                        cik_with_zeros = filename.replace("CIK", "").replace(".json", "")
                        cik = cik_with_zeros.lstrip("0")
                        
                        # Only process if we need this CIK (or if extracting all)
                        if needed_ciks is None or cik in needed_ciks:
                            try:
                                # Read and parse JSON directly from ZIP
                                with zf.open(filename) as f:
                                    data = json.load(f)
                                
                                # Extract shares data
                                shares_info = self.extract_shares_from_json(data, cik)
                                
                                if shares_info:
                                    results[cik] = shares_info
                                    extracted_count += 1
                                    
                                    # Track data freshness
                                    shares_date = shares_info.get('shares_date', '')
                                    if shares_date >= '2025-01-01':
                                        data_2025_count += 1
                                    elif shares_date >= '2024-07-01':
                                        data_2024_count += 1
                                
                                # Progress update
                                if extracted_count % 100 == 0:
                                    logger.info(f"Extracted data for {extracted_count} companies")
                                
                                # If we have all needed CIKs, we can stop early
                                if needed_ciks and len(results) == len(needed_ciks):
                                    logger.info(f"Found all {len(needed_ciks)} requested companies")
                                    break
                                    
                            except Exception as e:
                                logger.debug(f"Error processing {filename}: {e}")
                        
                        processed_count += 1
                        
                        # Overall progress update
                        if processed_count % 1000 == 0:
                            progress = (processed_count / total_files) * 100
                            logger.info(f"Processed {processed_count}/{total_files} files ({progress:.1f}%)")
            
            logger.info(f"Extraction complete: Found shares data for {extracted_count} companies")
            logger.info(f"Data freshness: {data_2025_count} from 2025, {data_2024_count} from late 2024")
            
        except Exception as e:
            logger.error(f"Error reading companyfacts.zip: {e}")
        
        return results
    
    def save_consolidated_data(self, shares_data: Dict[str, Dict]):
        """
        Save all shares data in a single consolidated JSON file
        
        Args:
            shares_data: Dictionary mapping CIK to shares info
        """
        output = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'total_companies': len(shares_data),
                'source': 'SEC EDGAR companyfacts.zip',
                'format_version': '2.0'  # New optimized format
            },
            'data': shares_data
        }
        
        try:
            with open(self.consolidated_shares_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            file_size = self.consolidated_shares_file.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Saved consolidated data: {len(shares_data)} companies ({file_size:.1f} MB)")
            logger.info(f"File: {self.consolidated_shares_file}")
            
        except Exception as e:
            logger.error(f"Error saving consolidated data: {e}")
    
    def run_optimized_extraction(self):
        """Run the optimized extraction process"""
        logger.info("="*60)
        logger.info("OPTIMIZED SEC EDGAR Shares Outstanding Extractor")
        logger.info("="*60)
        
        # Step 1: Download if needed
        if not self.download_companyfacts():
            logger.error("Failed to download companyfacts.zip")
            return False
        
        # Step 2: Get list of needed CIKs
        needed_ciks = self.get_needed_ciks()
        
        if needed_ciks:
            logger.info(f"Will extract data for {len(needed_ciks)} specific companies")
        else:
            logger.info("No CIK filter found - will extract all companies")
            response = input("Continue with full extraction? (y/n): ")
            if response.lower() != 'y':
                logger.info("Extraction cancelled")
                return False
        
        # Step 3: Extract only needed companies (selective extraction)
        shares_data = self.extract_selective_companyfacts(needed_ciks)
        
        if not shares_data:
            logger.error("No shares data extracted")
            return False
        
        # Step 4: Save consolidated data
        self.save_consolidated_data(shares_data)
        
        # Print summary
        print("\n" + "="*60)
        print("EXTRACTION COMPLETE")
        print("="*60)
        print(f"Companies extracted: {len(shares_data)}")
        print(f"Space saved: ~17GB (no individual files created)")
        print(f"Output file: {self.consolidated_shares_file}")
        
        # Show some examples
        print("\nExample data:")
        for cik, data in list(shares_data.items())[:5]:
            print(f"  CIK {cik}: {data['entity_name'][:30]}")
            print(f"    Shares: {data['shares_outstanding']:,}")
            print(f"    Date: {data['shares_date']}")
        
        return True
    
    def cleanup_old_data(self):
        """Remove old companyfacts folder if it exists"""
        old_companyfacts_dir = self.cache_dir / "companyfacts"
        
        if old_companyfacts_dir.exists():
            logger.info(f"Found old companyfacts directory: {old_companyfacts_dir}")
            
            # Count files
            json_files = list(old_companyfacts_dir.glob("*.json"))
            logger.info(f"Contains {len(json_files)} JSON files")
            
            # Calculate size
            total_size = sum(f.stat().st_size for f in json_files) / (1024**3)  # GB
            logger.info(f"Total size: {total_size:.1f} GB")
            
            response = input("Delete this directory to save space? (y/n): ")
            if response.lower() == 'y':
                import shutil
                shutil.rmtree(old_companyfacts_dir)
                logger.info(f"Deleted {total_size:.1f} GB of unnecessary data")
            else:
                logger.info("Keeping old data")


def main():
    """Main entry point"""
    fetcher = OptimizedSECSharesFetcher()
    
    # Check if consolidated file already exists
    if fetcher.consolidated_shares_file.exists():
        with open(fetcher.consolidated_shares_file, 'r') as f:
            data = json.load(f)
            companies = len(data.get('data', {}))
        
        logger.info(f"Consolidated file exists with {companies} companies")
        response = input("Re-run extraction? (y/n): ")
        if response.lower() != 'y':
            logger.info("Using existing data")
            
            # Offer to clean up old data
            fetcher.cleanup_old_data()
            return
    
    # Run optimized extraction
    if fetcher.run_optimized_extraction():
        # Offer to clean up old data
        fetcher.cleanup_old_data()


if __name__ == "__main__":
    main()