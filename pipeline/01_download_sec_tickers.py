"""
Download and process SEC company_tickers.json
This provides the ticker to CIK mapping needed to complete our data chain
"""

import json
import requests
import logging
import csv
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Import path configuration
from path_config import get_paths
from config_loader import load_config_with_env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SECTickerDownloader:
    """Download and process SEC company tickers data"""
    
    def __init__(self):
        # Get correct paths using path_config
        self.paths = get_paths()
        self.data_dir = self.paths['mappings']
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.company_tickers_file = self.data_dir / "company_tickers.json"
        self.ticker_cik_map_file = self.data_dir / "ticker_to_cik_map.json"
        self.validation_report_file = self.data_dir / "ticker_validation_report.json"
        
        # Load configuration
        self.config = self.load_config()
        
        # Load institution data for validation
        self.institutions = self.load_institutions()
        
    def load_config(self) -> dict:
        """Load configuration from analysis_config.json with environment variable support"""
        config_file = self.paths['analysis_config']
        
        if not config_file.exists():
            logger.warning(f"Config file not found at {config_file}. Using defaults.")
            return {
                'user_agent': {
                    'name': '13F Analysis Tool',
                    'email': 'analysis@example.com'
                },
                'api': {
                    'sec': {
                        'rate_limit_seconds': 0.1
                    }
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
    
    def load_institutions(self) -> List[Dict]:
        """Load institution data from clean_institutions.csv"""
        institutions = []
        csv_path = self.paths['config'] / "clean_institutions.csv"
        
        if not csv_path.exists():
            logger.warning(f"clean_institutions.csv not found at {csv_path}")
            return institutions
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('Institution'):
                        institutions.append({
                            'name': row['Institution'],
                            'cik': row.get('CIK', ''),
                            'type': row.get('Type', ''),
                            'aum': row.get('AUM_Billions', 0)
                        })
            logger.info(f"Loaded {len(institutions)} institutions for validation")
        except Exception as e:
            logger.error(f"Error loading institutions: {e}")
        
        return institutions
    
    def download_company_tickers(self, force: bool = False) -> bool:
        """
        Download the SEC company_tickers.json file with retry logic
        
        Args:
            force: Force re-download even if file exists
            
        Returns:
            True if successful, False otherwise
        """
        # Check if already downloaded
        if self.company_tickers_file.exists() and not force:
            logger.info(f"company_tickers.json already exists at {self.company_tickers_file}")
            return True
        
        url = "https://www.sec.gov/files/company_tickers.json"
        
        # Prepare headers with proper User-Agent from config
        user_agent = self.config.get('user_agent', {})
        headers = {
            'User-Agent': f"{user_agent.get('name', '13F Analysis')} {user_agent.get('email', 'analysis@example.com')}",
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        
        logger.info(f"Downloading SEC company_tickers.json with User-Agent: {headers['User-Agent']}")
        
        # Retry logic
        max_retries = self.config.get('download', {}).get('max_retries', 3)
        timeout = self.config.get('download', {}).get('timeout_seconds', 30)
        
        for attempt in range(max_retries):
            try:
                # Download the file
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                
                # Parse JSON
                data = response.json()
                
                # Save to file
                with open(self.company_tickers_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
                
                logger.info(f"Downloaded {len(data)} company records")
                logger.info(f"Saved to {self.company_tickers_file}")
                
                # Rate limiting
                rate_limit = self.config.get('api', {}).get('sec', {}).get('rate_limit_seconds', 0.1)
                time.sleep(rate_limit)
                
                return True
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}/{max_retries}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}/{max_retries}: {e}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        logger.error(f"Failed to download company_tickers.json after {max_retries} attempts")
        return False
    
    def process_ticker_mappings(self) -> dict:
        """
        Process the company tickers data to create ticker->CIK mapping
        
        Returns:
            Dictionary mapping ticker to CIK and company info
        """
        if not self.company_tickers_file.exists():
            logger.error("company_tickers.json not found. Please download first.")
            return {}
        
        logger.info("Processing ticker to CIK mappings...")
        
        try:
            with open(self.company_tickers_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Create ticker to CIK mapping
            ticker_to_cik = {}
            
            for idx, company in data.items():
                ticker = company.get('ticker', '').upper()
                cik = str(company.get('cik_str', '')).lstrip('0')  # Remove leading zeros
                title = company.get('title', '')
                
                if ticker and cik:
                    # Store the mapping
                    ticker_to_cik[ticker] = {
                        'cik': cik,
                        'company_name': title,
                        'cik_padded': str(company.get('cik_str', '')).zfill(10)  # 10-digit padded CIK
                    }
            
            logger.info(f"Processed {len(ticker_to_cik)} ticker->CIK mappings")
            
            # Save the mapping
            with open(self.ticker_cik_map_file, 'w', encoding='utf-8') as f:
                json.dump(ticker_to_cik, f, indent=2)
            
            logger.info(f"Saved ticker->CIK mapping to {self.ticker_cik_map_file}")
            
            return ticker_to_cik
            
        except Exception as e:
            logger.error(f"Error processing company tickers: {e}")
            return {}
    
    def validate_against_institutions(self, ticker_to_cik: dict) -> dict:
        """
        Validate ticker mappings against our tracked institutions
        
        Args:
            ticker_to_cik: Dictionary of ticker to CIK mappings
            
        Returns:
            Validation report
        """
        logger.info("Validating ticker mappings against tracked institutions...")
        
        validation_report = {
            'timestamp': datetime.now().isoformat(),
            'total_institutions': len(self.institutions),
            'institutions_with_tickers': [],
            'institutions_without_tickers': [],
            'potential_ticker_matches': []
        }
        
        # Create a CIK to ticker reverse mapping
        cik_to_ticker = {}
        for ticker, info in ticker_to_cik.items():
            cik_padded = info['cik_padded']
            if cik_padded not in cik_to_ticker:
                cik_to_ticker[cik_padded] = []
            cik_to_ticker[cik_padded].append({
                'ticker': ticker,
                'company_name': info['company_name']
            })
        
        # Check each institution
        for inst in self.institutions:
            inst_name = inst['name']
            inst_cik = inst['cik']
            
            if inst_cik in cik_to_ticker:
                # Found ticker(s) for this institution
                tickers = cik_to_ticker[inst_cik]
                validation_report['institutions_with_tickers'].append({
                    'institution': inst_name,
                    'cik': inst_cik,
                    'type': inst['type'],
                    'tickers': tickers
                })
            else:
                # No ticker found - try fuzzy matching by name
                validation_report['institutions_without_tickers'].append({
                    'institution': inst_name,
                    'cik': inst_cik,
                    'type': inst['type']
                })
                
                # Look for potential matches by partial name matching
                name_parts = inst_name.upper().split()
                potential_matches = []
                
                for ticker, info in ticker_to_cik.items():
                    company_name = info['company_name'].upper()
                    # Check if any significant part of institution name is in company name
                    for part in name_parts:
                        if len(part) > 3 and part in company_name:
                            potential_matches.append({
                                'ticker': ticker,
                                'company_name': info['company_name'],
                                'cik': info['cik_padded']
                            })
                            break
                
                if potential_matches:
                    validation_report['potential_ticker_matches'].append({
                        'institution': inst_name,
                        'inst_cik': inst_cik,
                        'potential_matches': potential_matches[:5]  # Limit to top 5
                    })
        
        # Save validation report
        with open(self.validation_report_file, 'w', encoding='utf-8') as f:
            json.dump(validation_report, f, indent=2)
        
        logger.info(f"Validation report saved to {self.validation_report_file}")
        
        return validation_report
    
    def get_statistics(self, ticker_to_cik: dict) -> dict:
        """Get statistics about the ticker mappings"""
        stats = {
            'total_tickers': len(ticker_to_cik),
            'unique_ciks': len(set(item['cik'] for item in ticker_to_cik.values())),
            'sample_mappings': []
        }
        
        # Get some sample mappings including common stocks
        sample_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'HIMS', 'TSLA', 'META', 
                         'BRK.A', 'BRK.B', 'JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'BLK']
        
        for ticker in sample_tickers:
            if ticker in ticker_to_cik:
                stats['sample_mappings'].append({
                    'ticker': ticker,
                    'cik': ticker_to_cik[ticker]['cik'],
                    'cik_padded': ticker_to_cik[ticker]['cik_padded'],
                    'company': ticker_to_cik[ticker]['company_name']
                })
        
        return stats
    
    def run(self):
        """Run the complete download and processing"""
        logger.info("="*60)
        logger.info("SEC Company Tickers Downloader")
        logger.info("="*60)
        
        # Step 1: Download
        if not self.download_company_tickers():
            logger.error("Failed to download company_tickers.json")
            return
        
        # Step 2: Process
        ticker_to_cik = self.process_ticker_mappings()
        
        if not ticker_to_cik:
            logger.error("Failed to process ticker mappings")
            return
        
        # Step 3: Validate against institutions
        validation_report = self.validate_against_institutions(ticker_to_cik)
        
        # Step 4: Statistics
        stats = self.get_statistics(ticker_to_cik)
        
        # Print summary
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        print(f"Total tickers mapped: {stats['total_tickers']:,}")
        print(f"Unique CIKs: {stats['unique_ciks']:,}")
        
        print("\nSample mappings:")
        for mapping in stats['sample_mappings']:
            print(f"  {mapping['ticker']:8} -> CIK {mapping['cik_padded']} ({mapping['company'][:40]})")
        
        print("\n" + "="*60)
        print("INSTITUTION VALIDATION")
        print("="*60)
        print(f"Total institutions tracked: {validation_report['total_institutions']}")
        print(f"Institutions with tickers: {len(validation_report['institutions_with_tickers'])}")
        print(f"Institutions without tickers: {len(validation_report['institutions_without_tickers'])}")
        
        # Show some institutions without tickers (these are typically private funds)
        if validation_report['institutions_without_tickers']:
            print("\nInstitutions without public tickers (expected for private funds):")
            for inst in validation_report['institutions_without_tickers'][:10]:
                print(f"  - {inst['institution']} ({inst['type']})")
            if len(validation_report['institutions_without_tickers']) > 10:
                print(f"  ... and {len(validation_report['institutions_without_tickers']) - 10} more")
        
        print(f"\nData saved to:")
        print(f"  - {self.company_tickers_file}")
        print(f"  - {self.ticker_cik_map_file}")
        print(f"  - {self.validation_report_file}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download SEC company tickers and create mappings')
    parser.add_argument('--force', action='store_true', 
                       help='Force re-download even if files exist')
    
    args = parser.parse_args()
    
    downloader = SECTickerDownloader()
    
    # Override download if force flag is set
    if args.force:
        logger.info("Force flag set - will re-download company_tickers.json")
        downloader.company_tickers_file.unlink(missing_ok=True)
    
    downloader.run()


if __name__ == "__main__":
    main()