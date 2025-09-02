"""
Data enrichment module using SEC data for shares outstanding
Uses the complete CUSIP → ticker → CIK → shares mapping
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from pipeline.path_config import get_paths

logger = logging.getLogger(__name__)

class SECDataEnricher:
    """Enrich 13F holdings data with SEC shares outstanding information"""
    
    def __init__(self, cache_dir: Path = None):
        """Initialize the data enricher with SEC data"""
        paths = get_paths()
        self.cache_dir = cache_dir or paths['mappings']
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        
        # Load the complete CUSIP to shares mapping
        self.cusip_to_shares_file = self.cache_dir / "cusip_to_shares_complete.json"
        self.cusip_to_shares = self.load_cusip_shares_mapping()
        
        # Statistics
        self.stats = {
            'cusip_lookups': 0,
            'shares_found': 0,
            'shares_missing': 0
        }
    
    def load_cusip_shares_mapping(self) -> Dict:
        """Load the complete CUSIP to shares outstanding mapping"""
        if not self.cusip_to_shares_file.exists():
            logger.error(f"CUSIP to shares mapping not found: {self.cusip_to_shares_file}")
            logger.error("Please run complete_cusip_mapping.py first")
            return {}
        
        try:
            with open(self.cusip_to_shares_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Extract just the mappings
            mappings = data.get('mappings', {})
            metadata = data.get('metadata', {})
            
            logger.info(f"Loaded SEC shares data for {len(mappings)} CUSIPs")
            logger.info(f"Coverage: {metadata.get('coverage_percentage', 0):.1f}%")
            
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading CUSIP shares mapping: {e}")
            return {}
    
    def get_shares_outstanding(self, cusip: str) -> Optional[Dict]:
        """
        Get shares outstanding data for a CUSIP
        
        Args:
            cusip: CUSIP identifier
            
        Returns:
            Dictionary with shares outstanding data or None
        """
        self.stats['cusip_lookups'] += 1
        
        shares_data = self.cusip_to_shares.get(cusip)
        
        if shares_data:
            self.stats['shares_found'] += 1
            return shares_data
        else:
            self.stats['shares_missing'] += 1
            return None
    
    def enrich_holdings(self, holdings: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Enrich holdings dictionary with SEC shares outstanding data
        
        Args:
            holdings: Dictionary with CUSIP as key and holding data as value
            
        Returns:
            Enriched holdings dictionary
        """
        logger.info(f"Enriching {len(holdings)} holdings with SEC shares outstanding data...")
        
        enriched_count = 0
        missing_count = 0
        
        for cusip, data in holdings.items():
            # Get shares outstanding from SEC data
            shares_data = self.get_shares_outstanding(cusip)
            
            if shares_data:
                # Add all SEC data to the holding
                data['ticker'] = shares_data.get('ticker')
                data['shares_outstanding'] = shares_data.get('shares_outstanding')
                data['shares_date'] = shares_data.get('shares_date')
                data['sec_entity_name'] = shares_data.get('sec_entity_name')
                data['cik'] = shares_data.get('cik')
                data['shares_data_available'] = True
                data['shares_data_source'] = 'SEC_EDGAR'
                enriched_count += 1
            else:
                # Mark as no data available
                data['shares_data_available'] = False
                data['shares_data_source'] = None
                missing_count += 1
        
        # Calculate success rate
        success_rate = (enriched_count / len(holdings)) * 100 if holdings else 0
        
        logger.info(f"Enrichment complete:")
        logger.info(f"  - Successfully enriched: {enriched_count} holdings")
        logger.info(f"  - Missing data: {missing_count} holdings")
        logger.info(f"  - Success rate: {success_rate:.1f}%")
        logger.info(f"  - Stats: {self.stats}")
        
        return holdings


def test_enricher():
    """Test the SEC data enricher"""
    enricher = SECDataEnricher()
    
    # Test some specific CUSIPs
    test_cusips = {
        '037833100': 'Apple',
        '594918104': 'Microsoft', 
        '433000106': 'HIMS',
        '67066G104': 'Nvidia',
        '11135F101': 'Broadcom'
    }
    
    print("\n" + "="*60)
    print("TESTING SEC DATA ENRICHER")
    print("="*60)
    
    for cusip, name in test_cusips.items():
        data = enricher.get_shares_outstanding(cusip)
        if data:
            print(f"\n{name} (CUSIP: {cusip}):")
            print(f"  Ticker: {data.get('ticker')}")
            print(f"  Shares Outstanding: {data.get('shares_outstanding'):,}")
            print(f"  As of: {data.get('shares_date')}")
            print(f"  Entity: {data.get('sec_entity_name')}")
        else:
            print(f"\n{name} (CUSIP: {cusip}): No data found")
    
    print(f"\nStatistics: {enricher.stats}")


if __name__ == "__main__":
    test_enricher()