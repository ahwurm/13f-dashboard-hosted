"""
Build comprehensive CUSIP to ticker mappings for all securities in 13F filings
Uses OpenFIGI API to map all CUSIPs found in the filings
"""

import os
import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import logging
import time
import requests
from typing import Dict, List, Set, Tuple
from path_config import get_paths

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CUSIPMapper:
    """Build comprehensive CUSIP to ticker mappings"""
    
    def __init__(self, filings_dir: str = None):
        # Get correct paths
        paths = get_paths()
        self.filings_dir = paths['filings'] if filings_dir is None else Path(filings_dir)
        self.data_dir = paths['mappings']
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache files
        self.cusip_ticker_cache_file = self.data_dir / "cusip_ticker_map.json"
        self.manual_mappings_file = self.data_dir / "manual_mappings.json"
        
        # Load existing caches
        self.cusip_ticker_cache = self.load_json_cache(self.cusip_ticker_cache_file)
        self.manual_mappings = self.load_json_cache(self.manual_mappings_file)
        
        # Merge manual mappings into cache
        for cusip, ticker in self.manual_mappings.items():
            if cusip not in self.cusip_ticker_cache:
                self.cusip_ticker_cache[cusip] = ticker
        
        # OpenFIGI API settings
        self.openfigi_url = "https://api.openfigi.com/v3/mapping"
        self.openfigi_headers = {
            "Content-Type": "application/json"
        }
        
        # Rate limiting
        self.last_openfigi_call = 0
        self.openfigi_delay = 2.5  # 2.5s between calls to respect 25 requests/minute limit
        
        # All unique CUSIPs found
        self.all_cusips = {}  # cusip -> name mapping
        
        # Statistics
        self.stats = {
            'total_cusips': 0,
            'already_mapped': 0,
            'newly_mapped': 0,
            'failed_mappings': 0,
            'api_calls': 0,
            'api_errors': 0
        }
    
    def load_json_cache(self, filepath: Path) -> Dict:
        """Load JSON cache file"""
        if filepath.exists():
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading cache {filepath}: {e}")
        return {}
    
    def save_json_cache(self, data: Dict, filepath: Path):
        """Save JSON cache file"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving cache {filepath}: {e}")
    
    def parse_13f_filing(self, filing_path: Path) -> List[Tuple[str, str]]:
        """Parse a single 13F filing and extract CUSIPs with names"""
        cusips = []
        
        try:
            with open(filing_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            import re
            xml_content = None
            
            # First, check for separate DOCUMENT format (most common - 134/138 filings)
            if '<TYPE>INFORMATION TABLE' in content:
                # Find the INFORMATION TABLE document section
                doc_start = content.find('<TYPE>INFORMATION TABLE')
                if doc_start != -1:
                    # Find the XML content within this document
                    xml_start = content.find('<XML>', doc_start)
                    xml_end = content.find('</XML>', xml_start)
                    
                    if xml_start != -1 and xml_end != -1:
                        # Extract the XML content
                        xml_content = content[xml_start + 5:xml_end].strip()
                        logger.debug(f"Found separate DOCUMENT format in {filing_path.name}")
            
            # If not found in separate document, try embedded format (4/138 filings)
            if not xml_content:
                # Look for informationTable root element (contains infoTable elements)
                info_table_match = re.search(r'<(\w*):?informationTable', content, re.IGNORECASE)
                if not info_table_match:
                    return cusips
                
                # Extract the namespace prefix (could be empty, ns, ns1, ns2, etc.)
                prefix = info_table_match.group(1) if info_table_match.group(1) else ''
                
                # Build the start and end markers based on detected prefix
                if prefix:
                    start_marker = f'<{prefix}:informationTable'
                    end_marker = f'</{prefix}:informationTable>'
                else:
                    start_marker = '<informationTable'
                    end_marker = '</informationTable>'
                
                start_idx = content.find(start_marker)
                if start_idx == -1:
                    return cusips
                
                end_idx = content.find(end_marker, start_idx)
                if end_idx == -1:
                    return cusips
                
                # Extract just the information table XML
                xml_content = content[start_idx:end_idx + len(end_marker)]
                logger.debug(f"Found embedded format in {filing_path.name}")
            
            # If we still don't have XML content, return empty
            if not xml_content:
                logger.warning(f"No information table found in {filing_path}")
                return cusips
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Detect namespace prefix in the XML content (look for infoTable elements)
            info_table_match = re.search(r'<(\w*):?infoTable', xml_content, re.IGNORECASE)
            prefix = ''
            if info_table_match and info_table_match.group(1):
                prefix = info_table_match.group(1)
            
            # Try to extract with detected namespace
            namespace_url = 'http://www.sec.gov/edgar/document/thirteenf/informationtable'
            
            # Build namespace dict based on detected prefix
            if prefix:
                # Has explicit namespace prefix
                ns = {prefix: namespace_url}
                xpath_prefix = prefix
            else:
                # Default namespace (no prefix) - register with 'ns' for XPath queries
                ns = {'ns': namespace_url}
                xpath_prefix = 'ns'
            
            # Build XPath queries with the appropriate prefix
            info_table_xpath = f'.//{xpath_prefix}:infoTable'
            name_xpath = f'{xpath_prefix}:nameOfIssuer'
            cusip_xpath = f'{xpath_prefix}:cusip'
            
            # Try with namespace
            info_tables = root.findall(info_table_xpath, ns)
            
            # If no results, try without namespace as fallback (for malformed files)
            if not info_tables:
                info_tables = root.findall('.//infoTable')
                name_xpath = 'nameOfIssuer'
                cusip_xpath = 'cusip'
                ns = {}
            
            # Extract CUSIPs and names
            for info_table in info_tables:
                name_elem = info_table.find(name_xpath, ns) if ns else info_table.find(name_xpath)
                cusip_elem = info_table.find(cusip_xpath, ns) if ns else info_table.find(cusip_xpath)
                
                if cusip_elem is not None and name_elem is not None:
                    cusip = cusip_elem.text
                    name = name_elem.text
                    if cusip:
                        cusips.append((cusip, name))
            
            # Log parsing success with details
            if cusips:
                if prefix and prefix != 'ns':
                    logger.debug(f"Successfully parsed filing with namespace prefix '{prefix}': found {len(cusips)} CUSIPs")
                else:
                    logger.debug(f"Successfully parsed filing: found {len(cusips)} CUSIPs")
            else:
                logger.warning(f"No CUSIPs extracted from {filing_path}")
        
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {filing_path}: {e}")
        except Exception as e:
            logger.error(f"Error parsing {filing_path}: {e}")
        
        return cusips
    
    def collect_all_cusips(self):
        """Collect all unique CUSIPs from all filings"""
        logger.info("Collecting all CUSIPs from 13F filings...")
        
        filing_count = 0
        successful_parses = 0
        failed_parses = []
        
        # Iterate through all CIK directories
        for cik_dir in self.filings_dir.glob("*"):
            if not cik_dir.is_dir():
                continue
            
            # Look for 13F-HR filings
            filing_dir = cik_dir / "13F-HR"
            if not filing_dir.exists():
                continue
            
            # Process each filing
            for accession_dir in filing_dir.glob("*"):
                filing_file = accession_dir / "full-submission.txt"
                if not filing_file.exists():
                    continue
                
                cusips = self.parse_13f_filing(filing_file)
                filing_count += 1
                
                if cusips:
                    successful_parses += 1
                    for cusip, name in cusips:
                        # Store the most recent name for each CUSIP
                        self.all_cusips[cusip] = name
                else:
                    failed_parses.append(filing_file.parent.parent.name)  # CIK number
        
        logger.info(f"Processed {filing_count} total filings")
        logger.info(f"Successfully parsed {successful_parses} filings")
        if failed_parses:
            logger.warning(f"Failed to parse {len(failed_parses)} filings: {', '.join(failed_parses[:5])}{'...' if len(failed_parses) > 5 else ''}")
        logger.info(f"Found {len(self.all_cusips)} unique CUSIPs")
        self.stats['total_cusips'] = len(self.all_cusips)
        self.stats['filings_processed'] = filing_count
        self.stats['successful_parses'] = successful_parses
        self.stats['failed_parses'] = len(failed_parses)
    
    def map_cusips_via_api(self):
        """Map all unmapped CUSIPs using OpenFIGI API"""
        # Identify unmapped CUSIPs
        unmapped_cusips = []
        for cusip, name in self.all_cusips.items():
            if cusip not in self.cusip_ticker_cache:
                unmapped_cusips.append((cusip, name))
            else:
                self.stats['already_mapped'] += 1
        
        if not unmapped_cusips:
            logger.info("All CUSIPs already mapped!")
            return
        
        logger.info(f"Need to map {len(unmapped_cusips)} CUSIPs via API")
        logger.info(f"Already have {self.stats['already_mapped']} CUSIPs mapped")
        
        # Process in batches of 10 (max without API key per OpenFIGI docs)
        batch_size = 10
        total_batches = (len(unmapped_cusips) - 1) // batch_size + 1
        logger.info(f"Will process in {total_batches} batches of up to {batch_size} CUSIPs each")
        
        for batch_num in range(0, len(unmapped_cusips), batch_size):
            batch = unmapped_cusips[batch_num:batch_num + batch_size]
            current_batch = batch_num // batch_size + 1
            
            logger.info(f"Processing batch {current_batch}/{total_batches} ({len(batch)} CUSIPs)...")
            
            # Prepare batch request with US exchange filter
            batch_request = [{"idType": "ID_CUSIP", "idValue": cusip, "exchCode": "US"} for cusip, _ in batch]
            
            # Debug: Log first batch to see what we're sending
            if current_batch == 1:
                logger.debug(f"First batch request: {json.dumps(batch_request[:2])}...")
                logger.debug(f"Request size: {len(json.dumps(batch_request))} bytes")
            
            # Rate limiting
            elapsed = time.time() - self.last_openfigi_call
            if elapsed < self.openfigi_delay:
                time.sleep(self.openfigi_delay - elapsed)
            
            try:
                response = requests.post(
                    self.openfigi_url,
                    headers=self.openfigi_headers,
                    json=batch_request,
                    timeout=30
                )
                
                self.last_openfigi_call = time.time()
                self.stats['api_calls'] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for i, item in enumerate(data):
                        cusip, name = batch[i]
                        
                        if "data" in item and len(item["data"]) > 0:
                            # Get ticker from first result
                            ticker_found = False
                            for result in item["data"]:
                                if "ticker" in result:
                                    ticker = result["ticker"]
                                    self.cusip_ticker_cache[cusip] = ticker
                                    self.stats['newly_mapped'] += 1
                                    ticker_found = True
                                    logger.debug(f"Mapped {cusip} ({name}) -> {ticker}")
                                    break
                            
                            if not ticker_found:
                                self.stats['failed_mappings'] += 1
                                logger.debug(f"No ticker in response for {cusip} ({name})")
                        else:
                            self.stats['failed_mappings'] += 1
                            logger.debug(f"No mapping found for {cusip} ({name})")
                    
                    # Save cache after each successful batch
                    self.save_json_cache(self.cusip_ticker_cache, self.cusip_ticker_cache_file)
                    
                elif response.status_code == 429:
                    logger.warning("Rate limited by OpenFIGI. Waiting 60 seconds...")
                    time.sleep(60)
                    # Retry this batch
                    batch_num -= batch_size
                elif response.status_code == 413:
                    logger.error(f"Payload too large (413) even with batch size {batch_size}")
                    logger.error("Consider reducing batch size further or checking request structure")
                    self.stats['api_errors'] += 1
                else:
                    logger.error(f"API error: {response.status_code} - {response.text[:200]}")
                    self.stats['api_errors'] += 1
                    
            except Exception as e:
                logger.error(f"Error calling OpenFIGI: {e}")
                self.stats['api_errors'] += 1
            
            # Progress update every 20 batches (since we have more batches now)
            if current_batch % 20 == 0 or current_batch == total_batches:
                logger.info(f"Progress: {self.stats['newly_mapped']} newly mapped, "
                          f"{self.stats['failed_mappings']} failed, "
                          f"{self.stats['api_errors']} API errors")
    
    def generate_summary_report(self):
        """Generate a summary report of the mapping process"""
        report_file = self.data_dir / "mapping_summary.json"
        
        # Get some example mappings
        example_mappings = []
        for cusip, ticker in list(self.cusip_ticker_cache.items())[:20]:
            name = self.all_cusips.get(cusip, "Unknown")
            example_mappings.append({
                "cusip": cusip,
                "ticker": ticker,
                "name": name
            })
        
        summary = {
            "generated": datetime.now().isoformat(),
            "statistics": self.stats,
            "filing_coverage": {
                "total_filings": self.stats.get('filings_processed', 0),
                "successful_parses": self.stats.get('successful_parses', 0),
                "failed_parses": self.stats.get('failed_parses', 0),
                "parse_success_rate": round(
                    self.stats.get('successful_parses', 0) / 
                    self.stats.get('filings_processed', 1) * 100, 2
                ) if self.stats.get('filings_processed', 0) > 0 else 0
            },
            "coverage": {
                "total_cusips": self.stats['total_cusips'],
                "mapped_cusips": self.stats['already_mapped'] + self.stats['newly_mapped'],
                "unmapped_cusips": self.stats['failed_mappings'],
                "coverage_percentage": round(
                    (self.stats['already_mapped'] + self.stats['newly_mapped']) / 
                    self.stats['total_cusips'] * 100, 2
                ) if self.stats['total_cusips'] > 0 else 0
            },
            "api_usage": {
                "total_calls": self.stats['api_calls'],
                "errors": self.stats['api_errors']
            },
            "example_mappings": example_mappings
        }
        
        self.save_json_cache(summary, report_file)
        
        return summary
    
    def run(self):
        """Run the complete CUSIP mapping process"""
        logger.info("="*60)
        logger.info("CUSIP MAPPING BUILDER")
        logger.info("="*60)
        
        # Step 1: Collect all CUSIPs
        self.collect_all_cusips()
        
        # Step 2: Map via API
        self.map_cusips_via_api()
        
        # Step 3: Generate summary
        summary = self.generate_summary_report()
        
        # Print results
        print("\n" + "="*60)
        print("MAPPING COMPLETE")
        print("="*60)
        print(f"\nFiling Statistics:")
        print(f"  Total filings found: {summary['statistics'].get('filings_processed', 'N/A')}")
        print(f"  Successfully parsed: {summary['statistics'].get('successful_parses', 'N/A')}")
        print(f"  Failed to parse: {summary['statistics'].get('failed_parses', 'N/A')}")
        print(f"\nCUSIP Statistics:")
        print(f"  Total CUSIPs found: {summary['statistics']['total_cusips']}")
        print(f"  Successfully mapped: {summary['coverage']['mapped_cusips']}")
        print(f"  Failed to map: {summary['coverage']['unmapped_cusips']}")
        print(f"  Coverage: {summary['coverage']['coverage_percentage']}%")
        print(f"\nAPI Usage:")
        print(f"  API calls made: {summary['api_usage']['total_calls']}")
        print(f"  API errors: {summary['api_usage']['errors']}")
        
        print(f"\nCache saved to: {self.cusip_ticker_cache_file}")
        print(f"Summary saved to: {self.data_dir / 'mapping_summary.json'}")
        
        # Show some examples
        print("\nExample mappings:")
        for ex in summary['example_mappings'][:10]:
            print(f"  {ex['cusip']} -> {ex['ticker']:6} ({ex['name'][:40]})")

def main():
    """Main entry point"""
    mapper = CUSIPMapper()
    mapper.run()

if __name__ == "__main__":
    main()