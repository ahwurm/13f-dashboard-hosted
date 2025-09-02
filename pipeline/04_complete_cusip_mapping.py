"""
Build complete CUSIP → Ticker → CIK → Shares Outstanding mapping
This creates the final data chain for accurate institutional ownership calculations
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
from path_config import get_paths

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CompleteCUSIPMapper:
    """Build complete mapping from CUSIP to shares outstanding via ticker and CIK"""
    
    def __init__(self, data_dir: Path = None):
        self.paths = get_paths()
        self.data_dir = data_dir or self.paths['mappings']
        
        # Load all necessary mappings
        self.cusip_to_ticker = self.load_json("cusip_ticker_map.json", "CUSIP to ticker")
        self.ticker_to_cik = self.load_json("ticker_to_cik_map.json", "ticker to CIK")
        self.cik_to_shares = self.load_sec_shares_data()
        
        # Final mapping
        self.cusip_to_shares = {}
        
    def is_valid_shares_data(self, shares_outstanding: int, shares_date: str) -> Tuple[bool, str]:
        """
        Validate shares outstanding data quality
        Prioritizes recent data (2025 or late 2024)
        
        Returns:
            Tuple of (is_valid, reason)
        """
        # Exclude known placeholder values
        if shares_outstanding in [1, 10, 12, 100, 1000, 10000]:
            return False, "Placeholder value"
        
        # Exclude unreasonably low values (< 100k shares)
        if shares_outstanding < 100000:
            return False, "Too few shares for public company"
        
        # Exclude old data (> 1 year, or before Q3 2024)
        if shares_date:
            try:
                # Parse date
                date_obj = datetime.fromisoformat(shares_date.split('T')[0])
                days_old = (datetime.now() - date_obj).days
                
                # Reject if older than 1 year
                if days_old > 365:
                    return False, f"Stale data ({days_old} days old)"
                
                # Warn if before 2025 but allow Q3/Q4 2024
                if shares_date < '2024-07-01':
                    return False, f"Data too old (before Q3 2024: {shares_date})"
                    
            except:
                pass  # If date parsing fails, continue
        else:
            # No date means likely very old data
            return False, "No date information"
        
        return True, "Valid"
    
    def load_sec_shares_data(self) -> Dict:
        """Load SEC shares data from consolidated or legacy format"""
        # Try new consolidated format first
        consolidated_file = self.paths['sec_data'] / "sec_shares_consolidated.json"
        if consolidated_file.exists():
            logger.info(f"Loading SEC shares from consolidated file: {consolidated_file}")
            try:
                with open(consolidated_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    shares_data = data.get('data', {})
                    logger.info(f"Loaded shares data for {len(shares_data)} companies (consolidated format)")
                    return shares_data
            except Exception as e:
                logger.error(f"Error loading consolidated file: {e}")
        
        # Fallback to old format
        old_file = self.paths['sec_data'] / "sec_shares_outstanding.json"
        if old_file.exists():
            logger.info(f"Loading SEC shares from legacy file: {old_file}")
            try:
                with open(old_file, 'r', encoding='utf-8') as f:
                    shares_data = json.load(f)
                    logger.info(f"Loaded shares data for {len(shares_data)} companies (legacy format)")
                    return shares_data
            except Exception as e:
                logger.error(f"Error loading legacy file: {e}")
        
        logger.warning("No SEC shares data found")
        return {}
    
    def load_json(self, filename: str, description: str) -> Dict:
        """Load a JSON file from the data directory"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            logger.error(f"{description} file not found: {filepath}")
            return {}
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} {description} mappings")
                return data
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return {}
    
    def build_complete_mapping(self) -> Dict[str, Dict]:
        """
        Build the complete CUSIP → shares outstanding mapping
        
        Returns:
            Dictionary mapping CUSIP to shares outstanding data
        """
        logger.info("Building complete CUSIP → shares outstanding mapping...")
        
        successful_mappings = 0
        missing_ticker_to_cik = 0
        missing_cik_to_shares = 0
        invalid_shares_data = 0
        validation_reasons = {}
        
        # For each CUSIP
        for cusip, ticker in self.cusip_to_ticker.items():
            if not ticker:
                continue
            
            # Normalize ticker
            ticker = ticker.upper()
            
            # Step 1: Get CIK from ticker
            ticker_data = self.ticker_to_cik.get(ticker)
            if not ticker_data:
                missing_ticker_to_cik += 1
                continue
            
            cik = ticker_data.get('cik')
            if not cik:
                continue
            
            # Step 2: Get shares outstanding from CIK
            shares_data = self.cik_to_shares.get(cik)
            if not shares_data:
                missing_cik_to_shares += 1
                continue
            
            # Step 3: Validate shares data quality
            shares_outstanding = shares_data.get('shares_outstanding')
            shares_date = shares_data.get('shares_date')
            
            is_valid, reason = self.is_valid_shares_data(shares_outstanding, shares_date)
            
            if not is_valid:
                invalid_shares_data += 1
                validation_reasons[reason] = validation_reasons.get(reason, 0) + 1
                continue
            
            # Success! We have the complete chain with valid data
            self.cusip_to_shares[cusip] = {
                'cusip': cusip,
                'ticker': ticker,
                'cik': cik,
                'company_name': ticker_data.get('company_name'),
                'sec_entity_name': shares_data.get('entity_name'),
                'shares_outstanding': shares_outstanding,
                'shares_date': shares_date,
                'last_updated': shares_data.get('last_updated')
            }
            successful_mappings += 1
        
        logger.info(f"Successfully mapped {successful_mappings} CUSIPs to shares outstanding")
        logger.info(f"Missing ticker->CIK mappings: {missing_ticker_to_cik}")
        logger.info(f"Missing CIK->shares mappings: {missing_cik_to_shares}")
        logger.info(f"Invalid/stale shares data (excluded): {invalid_shares_data}")
        
        # Log validation reasons
        if validation_reasons:
            logger.info("Validation exclusion reasons:")
            for reason, count in sorted(validation_reasons.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  - {reason}: {count}")
        
        # Calculate coverage
        total_cusips = len(self.cusip_to_ticker)
        coverage = (successful_mappings / total_cusips * 100) if total_cusips > 0 else 0
        logger.info(f"Coverage: {successful_mappings}/{total_cusips} ({coverage:.1f}%)")
        
        return self.cusip_to_shares
    
    def save_complete_mapping(self):
        """Save the complete mapping to file"""
        output_file = self.data_dir / "cusip_to_shares_complete.json"
        
        try:
            # Add metadata
            output = {
                'metadata': {
                    'generated': datetime.now().isoformat(),
                    'total_cusips': len(self.cusip_to_ticker),
                    'mapped_cusips': len(self.cusip_to_shares),
                    'coverage_percentage': (len(self.cusip_to_shares) / len(self.cusip_to_ticker) * 100) 
                                         if self.cusip_to_ticker else 0
                },
                'mappings': self.cusip_to_shares
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            logger.info(f"Complete mapping saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving complete mapping: {e}")
    
    def test_specific_cusips(self):
        """Test specific CUSIPs to verify the mapping works"""
        test_cases = [
            ('037833100', 'AAPL', 'Apple'),
            ('594918104', 'MSFT', 'Microsoft'),
            ('023135106', 'AMZN', 'Amazon'),
            ('433000106', 'HIMS', 'HIMS & Hers'),
            ('67066G104', 'NVDA', 'Nvidia'),
            ('11135F101', 'AVGO', 'Broadcom'),
            ('88160R101', 'TSLA', 'Tesla'),
            ('30303M102', 'META', 'Meta'),
        ]
        
        print("\n" + "="*60)
        print("TEST SPECIFIC STOCKS")
        print("="*60)
        
        for cusip, expected_ticker, company in test_cases:
            data = self.cusip_to_shares.get(cusip)
            if data:
                shares = data.get('shares_outstanding', 0)
                actual_ticker = data.get('ticker', 'N/A')
                entity = data.get('sec_entity_name', 'Unknown')
                date = data.get('shares_date', 'Unknown')
                
                print(f"\n{company} ({expected_ticker}):")
                print(f"  / CUSIP {cusip} -> {actual_ticker}")
                print(f"  Shares Outstanding: {shares:,}")
                print(f"  SEC Entity: {entity}")
                print(f"  As of: {date}")
            else:
                print(f"\n{company} ({expected_ticker}):")
                print(f"  X No mapping found for CUSIP {cusip}")
    
    def get_statistics(self) -> Dict:
        """Get detailed statistics about the mapping"""
        stats = {
            'input_data': {
                'total_cusips': len(self.cusip_to_ticker),
                'total_tickers': len(set(self.cusip_to_ticker.values())),
                'ticker_to_cik_mappings': len(self.ticker_to_cik),
                'cik_to_shares_mappings': len(self.cik_to_shares)
            },
            'output_data': {
                'cusips_with_shares': len(self.cusip_to_shares),
                'coverage_percentage': (len(self.cusip_to_shares) / len(self.cusip_to_ticker) * 100) 
                                     if self.cusip_to_ticker else 0
            }
        }
        
        # Find some interesting statistics
        if self.cusip_to_shares:
            shares_values = [d['shares_outstanding'] for d in self.cusip_to_shares.values() 
                           if d.get('shares_outstanding')]
            if shares_values:
                stats['shares_statistics'] = {
                    'min_shares': min(shares_values),
                    'max_shares': max(shares_values),
                    'median_shares': sorted(shares_values)[len(shares_values)//2]
                }
        
        return stats
    
    def run(self):
        """Run the complete mapping process"""
        logger.info("="*60)
        logger.info("COMPLETE CUSIP TO SHARES OUTSTANDING MAPPER")
        logger.info("="*60)
        
        # Build the mapping
        self.build_complete_mapping()
        
        # Save the results
        self.save_complete_mapping()
        
        # Get statistics
        stats = self.get_statistics()
        
        # Print summary
        print("\n" + "="*60)
        print("MAPPING COMPLETE")
        print("="*60)
        print(f"\nInput Data:")
        print(f"  Total CUSIPs: {stats['input_data']['total_cusips']:,}")
        print(f"  Unique tickers: {stats['input_data']['total_tickers']:,}")
        print(f"  Ticker->CIK mappings: {stats['input_data']['ticker_to_cik_mappings']:,}")
        print(f"  CIK->Shares mappings: {stats['input_data']['cik_to_shares_mappings']:,}")
        
        print(f"\nOutput Results:")
        print(f"  CUSIPs with shares data: {stats['output_data']['cusips_with_shares']:,}")
        print(f"  Coverage: {stats['output_data']['coverage_percentage']:.1f}%")
        
        if 'shares_statistics' in stats:
            print(f"\nShares Outstanding Range:")
            print(f"  Min: {stats['shares_statistics']['min_shares']:,}")
            print(f"  Median: {stats['shares_statistics']['median_shares']:,}")
            print(f"  Max: {stats['shares_statistics']['max_shares']:,}")
        
        # Test specific stocks
        self.test_specific_cusips()
        
        print(f"\nData saved to: {self.data_dir / 'cusip_to_shares_complete.json'}")


def main():
    """Main entry point"""
    mapper = CompleteCUSIPMapper()
    mapper.run()


if __name__ == "__main__":
    main()