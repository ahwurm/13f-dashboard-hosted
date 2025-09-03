"""
Analyze 13F filings to identify top net additions as percentage of shares outstanding
Two analyses:
1. Quarterly net additions (Q2 vs Q1 changes)
2. Total cumulative holdings
"""

import os
import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import logging
import time

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

# Import data enrichment module
from modules.data_enrichment_sec import SECDataEnricher
from path_config import get_paths, get_output_dir, get_data_driven_defaults

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Filing13FAnalyzer:
    """Analyze 13F filings for net additions and total holdings"""
    
    def __init__(self, filings_dir: str = None, quarter: str = None, year: int = None):
        # Use dynamic defaults if not provided
        if quarter is None or year is None:
            default_q, default_y = get_data_driven_defaults()
            quarter = quarter or default_q
            year = year or default_y
        paths = get_paths()
        self.filings_dir = paths['filings'] if filings_dir is None else Path(filings_dir)
        self.quarter = quarter
        self.year = year
        self.output_dir = get_output_dir(quarter, year)
        self.data_dir = paths['mappings']
        self.progress_file = self.data_dir / "analysis_progress.json"
        
        # Load configuration
        config_path = Path(__file__).parent.parent / 'config' / 'analysis_config.json'
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Load filing metadata if available
        self.filing_metadata = self.load_filing_metadata()
        
        # No hardcoded ETF exclusions - will filter dynamically
        
        # Ensure directories exist
        self.output_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
        # Load CIK metadata
        self.cik_metadata = self.load_cik_metadata()
        
        # Track filing periods for each institution
        self.institution_filing_periods = {}
        
        # Initialize data enricher for SEC shares outstanding data
        self.data_enricher = SECDataEnricher(cache_dir=self.data_dir)
        
        # Holdings data
        self.current_holdings = defaultdict(lambda: {
            'shares': 0,
            'value': 0,
            'holders': [],
            'name': '',
            'cusip': '',
            'positions': {}  # Store individual institution positions
        })
        
        # For tracking quarterly changes (will need Q1 data)
        self.previous_holdings = {}
        self.net_additions = defaultdict(lambda: {
            'shares_added': 0,
            'value_added': 0,
            'new_holders': [],
            'name': '',
            'cusip': ''
        })
    
    def load_cik_metadata(self) -> Dict:
        """Load CIK metadata for company names"""
        paths = get_paths()
        metadata_file = paths['cik_metadata']
        metadata = {}
        
        # First load the main metadata file
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                metadata = data.get("cik_details", {})
        
        # Also load any previously extracted names
        extracted_names_file = self.data_dir / 'extracted_institution_names.json'
        if extracted_names_file.exists():
            try:
                with open(extracted_names_file, 'r', encoding='utf-8') as f:
                    extracted = json.load(f)
                    # Merge extracted names into metadata
                    for cik, info in extracted.items():
                        if cik not in metadata:
                            metadata[cik] = info
                        elif 'name' in info and 'name' not in metadata[cik]:
                            metadata[cik]['name'] = info['name']
                    logger.info(f"Loaded {len(extracted)} extracted institution names from cache")
            except Exception as e:
                logger.warning(f"Could not load extracted names cache: {e}")
        
        return metadata
    
    def load_filing_metadata(self) -> Dict:
        """Load filing metadata that includes filing periods"""
        metadata_file = self.filings_dir.parent / "13f_filings" / "filing_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def is_amendment_filing(self, filing_path: Path) -> bool:
        """Check if a filing is an amendment (13F-HR/A)"""
        try:
            with open(filing_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(2000)
                # Check for amendment indicators
                if 'CONFORMED SUBMISSION TYPE:\t13F-HR/A' in content:
                    return True
                if '<TYPE>13F-HR/A' in content:
                    return True
                if '<submissionType>13F-HR/A</submissionType>' in content:
                    return True
                return False
        except:
            return False
    
    def extract_filing_period(self, filing_file: Path) -> Optional[Dict]:
        """Extract the filing period from a 13F filing"""
        try:
            with open(filing_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(50000)  # Read first 50KB
            
            # Look for period of report
            import re
            period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', content)
            if period_match:
                period_date = period_match.group(1)
                # Parse YYYYMMDD format
                year = int(period_date[:4])
                month = int(period_date[4:6])
                
                # Determine quarter from month
                if month <= 3:
                    quarter = 1
                elif month <= 6:
                    quarter = 2
                elif month <= 9:
                    quarter = 3
                else:
                    quarter = 4
                
                return {"year": year, "quarter": quarter, "month": month}
        except Exception as e:
            logger.debug(f"Could not extract filing period: {e}")
        
        return None
    
    def extract_company_name_from_filing(self, filing_path: Path) -> Optional[str]:
        """Extract company name from SEC filing header - GitHub-friendly approach"""
        try:
            with open(filing_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read first 5KB which contains the header
                content = f.read(5000)
                
            # Look for COMPANY CONFORMED NAME in SEC header
            name_match = re.search(r'COMPANY CONFORMED NAME:\s*(.+)', content)
            if name_match:
                name = name_match.group(1).strip()
                # Clean up common formatting
                name = re.sub(r'\s+L\.?P\.?$', ' LP', name)
                name = re.sub(r'\s+LLC$', ' LLC', name)
                name = re.sub(r'\s+INC\.?$', ' Inc', name)
                name = re.sub(r'\s+CORP\.?$', ' Corp', name)
                # Remove extra whitespace
                name = ' '.join(name.split())
                return name
        except Exception as e:
            logger.debug(f"Could not extract name from {filing_path}: {e}")
        return None
    
    def parse_13f_filing(self, filing_path: Path) -> List[Dict]:
        """Parse a single 13F filing XML"""
        holdings = []
        
        try:
            with open(filing_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check filing date - skip very old filings
            import re
            date_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', content)
            if date_match:
                filing_date = date_match.group(1)
                filing_year = int(filing_date[:4])
                min_filing_year = self.config.get('analysis', {}).get('min_filing_year', 2020)
                if filing_year < min_filing_year:
                    logger.debug(f"Skipping old filing from {filing_year}: {filing_path}")
                    return holdings
            
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
                # Look for any namespace prefix with informationTable (case-insensitive)
                info_table_match = re.search(r'<(\w*):?informationTable', content, re.IGNORECASE)
                if not info_table_match:
                    logger.warning(f"No information table found in {filing_path}")
                    return holdings
                
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
                    logger.warning(f"No information table found in {filing_path}")
                    return holdings
                
                end_idx = content.find(end_marker, start_idx)
                if end_idx == -1:
                    logger.warning(f"Incomplete information table in {filing_path}")
                    return holdings
                
                # Extract just the information table XML
                xml_content = content[start_idx:end_idx + len(end_marker)]
                logger.debug(f"Found embedded format in {filing_path.name}")
            
            # If we still don't have XML content, return empty
            if not xml_content:
                logger.warning(f"Could not extract information table from {filing_path}")
                return holdings
            
            # Parse XML
            root = ET.fromstring(xml_content)
            
            # Detect namespace prefix in the XML content
            info_table_match = re.search(r'<(\w*):?informationTable', xml_content, re.IGNORECASE)
            prefix = ''
            if info_table_match and info_table_match.group(1):
                prefix = info_table_match.group(1)
            
            # Try to extract with detected namespace
            namespace_url = 'http://www.sec.gov/edgar/document/thirteenf/informationtable'
            
            # Build namespace dict and XPath queries based on detected prefix
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
            value_xpath = f'{xpath_prefix}:value'
            shares_xpath = f'.//{xpath_prefix}:sshPrnamt'
            
            # Try with namespace
            info_tables = root.findall(info_table_xpath, ns)
            
            # If no results, try without namespace as fallback (for malformed files)
            if not info_tables:
                info_tables = root.findall('.//infoTable')
                name_xpath = 'nameOfIssuer'
                cusip_xpath = 'cusip'
                value_xpath = 'value'
                shares_xpath = './/sshPrnamt'
                ns = {}
            
            # Extract holdings
            for info_table in info_tables:
                holding = {}
                
                # Extract fields
                name_elem = info_table.find(name_xpath, ns) if ns else info_table.find(name_xpath)
                cusip_elem = info_table.find(cusip_xpath, ns) if ns else info_table.find(cusip_xpath)
                value_elem = info_table.find(value_xpath, ns) if ns else info_table.find(value_xpath)
                shares_elem = info_table.find(shares_xpath, ns) if ns else info_table.find(shares_xpath)
                
                if name_elem is not None:
                    holding['name'] = name_elem.text
                if cusip_elem is not None:
                    holding['cusip'] = cusip_elem.text
                if value_elem is not None:
                    holding['value'] = int(value_elem.text) * 1000  # Value is in thousands
                if shares_elem is not None:
                    holding['shares'] = int(shares_elem.text)
                
                if holding.get('cusip') and holding.get('shares'):
                    holdings.append(holding)
            
            # Log parsing results
            if holdings:
                if prefix and prefix not in ['ns', '']:
                    logger.debug(f"Successfully parsed filing with namespace prefix '{prefix}': found {len(holdings)} holdings")
                else:
                    logger.debug(f"Successfully parsed filing: found {len(holdings)} holdings")
            else:
                logger.warning(f"No holdings extracted from {filing_path}")
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {filing_path}: {e}")
        except Exception as e:
            logger.error(f"Error parsing {filing_path}: {e}")
        
        return holdings
    
    def process_all_filings(self):
        """Process all 13F filings, tracking filing periods"""
        logger.info("Processing all 13F filings...")
        
        filing_count = 0
        successful_parses = 0
        failed_institutions = []
        total_holdings = 0
        target_quarter_count = 0
        fallback_quarter_count = 0
        
        # Iterate through all CIK directories
        for cik_dir in self.filings_dir.glob("*"):
            if not cik_dir.is_dir():
                continue
            
            cik = cik_dir.name
            company_info = self.cik_metadata.get(cik, {})
            company_name = company_info.get('name')
            
            # Look for 13F-HR filings
            filing_dir = cik_dir / "13F-HR"
            if not filing_dir.exists():
                continue
            
            # If company name not found in metadata, try to extract from filing
            if not company_name:
                for accession_dir in sorted(filing_dir.glob("*"), reverse=True):
                    filing_file = accession_dir / "full-submission.txt"
                    if filing_file.exists():
                        extracted_name = self.extract_company_name_from_filing(filing_file)
                        if extracted_name:
                            company_name = extracted_name
                            # Cache for future use
                            if cik not in self.cik_metadata:
                                self.cik_metadata[cik] = {}
                            self.cik_metadata[cik]['name'] = company_name
                            logger.info(f"Extracted name for CIK {cik}: {company_name}")
                            break
                
                # Last resort fallback
                if not company_name:
                    company_name = f'CIK {cik}'
            
            # Process the most recent filing, preferring main filings over amendments
            latest_accession = None
            latest_filing_period = None
            
            # First, collect all filings and categorize them
            main_filings = []
            amendment_filings = []
            
            for accession_dir in filing_dir.glob("*"):
                filing_file = accession_dir / "full-submission.txt"
                if filing_file.exists():
                    if self.is_amendment_filing(filing_file):
                        amendment_filings.append(accession_dir)
                        logger.debug(f"Skipping amendment filing for {company_name}: {accession_dir.name}")
                    else:
                        main_filings.append(accession_dir)
            
            # ONLY use main filings, skip amendments entirely to avoid incomplete data
            all_filings = main_filings
            
            if not all_filings:
                continue
            
            # Try to find filing for target quarter ONLY - NO FALLBACK
            target_quarter_int = int(self.quarter[1])
            target_filing = None
            
            for accession_dir in sorted(all_filings, reverse=True):
                filing_file = accession_dir / "full-submission.txt"
                period = self.extract_filing_period(filing_file)
                
                if period:
                    if period['quarter'] == target_quarter_int and period['year'] == self.year:
                        target_filing = accession_dir
                        logger.debug(f"{company_name}: Found target quarter filing Q{period['quarter']} {period['year']}")
                        break
            
            # ONLY use target quarter filing - no fallback to ensure data accuracy
            filing_to_process = target_filing
            
            if not filing_to_process:
                logger.debug(f"{company_name}: No Q{target_quarter_int} {self.year} filing found - skipping (no fallback)")
                continue
            
            # Process the selected filing
            for accession_dir in [filing_to_process]:
                filing_file = accession_dir / "full-submission.txt"
                if not filing_file.exists():
                    continue
                
                # Extract filing period
                filing_period = self.extract_filing_period(filing_file)
                
                # Check if this filing is from our filing metadata
                if cik in self.filing_metadata:
                    metadata_period = self.filing_metadata[cik].get('filing_period')
                    if metadata_period:
                        filing_period = metadata_period
                
                if filing_period:
                    self.institution_filing_periods[company_name] = filing_period
                    
                    # Check if this is target quarter or fallback
                    if filing_period['quarter'] == int(self.quarter[1]) and filing_period['year'] == self.year:
                        target_quarter_count += 1
                        logger.debug(f"{company_name}: Q{filing_period['quarter']} {filing_period['year']} (target quarter)")
                    else:
                        fallback_quarter_count += 1
                        logger.debug(f"{company_name}: Q{filing_period['quarter']} {filing_period['year']} (fallback)")
                
                holdings = self.parse_13f_filing(filing_file)
                filing_count += 1
                
                if holdings:
                    successful_parses += 1
                    total_holdings += len(holdings)
                    logger.info(f"Processed {company_name}: {len(holdings)} holdings")
                    
                    # Aggregate holdings by CUSIP
                    for holding in holdings:
                        cusip = holding['cusip']
                        self.current_holdings[cusip]['shares'] += holding['shares']
                        self.current_holdings[cusip]['value'] += holding['value']
                        self.current_holdings[cusip]['name'] = holding['name']
                        self.current_holdings[cusip]['cusip'] = cusip
                        
                        # Store individual position for this institution
                        if company_name not in self.current_holdings[cusip]['positions']:
                            self.current_holdings[cusip]['positions'][company_name] = {
                                'shares': 0,
                                'value': 0,
                                'pct_of_company_shares': 0  # Will be calculated later when we have shares outstanding
                            }
                        
                        self.current_holdings[cusip]['positions'][company_name]['shares'] += holding['shares']
                        self.current_holdings[cusip]['positions'][company_name]['value'] += holding['value']
                        
                        if company_name not in self.current_holdings[cusip]['holders']:
                            self.current_holdings[cusip]['holders'].append(company_name)
                else:
                    failed_institutions.append(company_name)
                    logger.warning(f"No holdings extracted for {company_name}")
                
                # Only process the most recent filing
                break
        
        logger.info(f"Processed {filing_count} total filings")
        logger.info(f"Successfully parsed: {successful_parses}, Failed: {len(failed_institutions)}")
        if failed_institutions:
            logger.warning(f"Failed institutions: {', '.join(failed_institutions[:5])}{'...' if len(failed_institutions) > 5 else ''}")
        logger.info(f"Total holdings extracted: {total_holdings}")
        logger.info(f"Unique securities: {len(self.current_holdings)}")
        logger.info(f"Filing periods: {target_quarter_count} from Q{self.quarter[1]} {self.year} (no fallback data used)")
        
        # Store statistics for reporting
        self.filing_stats = {
            'total_filings': filing_count,
            'successful_parses': successful_parses,
            'failed_parses': len(failed_institutions),
            'parse_success_rate': round(successful_parses/filing_count*100, 1) if filing_count > 0 else 0
        }
    
    
    def save_progress(self, stage: str, data: Dict = None):
        """Save analysis progress to enable resumption"""
        progress = {
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "holdings_count": len(self.current_holdings)
        }
        if data:
            progress["data"] = data
        
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2)
        logger.info(f"Progress saved at stage: {stage}")
    
    def load_progress(self) -> Optional[Dict]:
        """Load saved progress if available"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def save_enriched_holdings(self):
        """Save enriched holdings to cache file"""
        cache_file = self.data_dir / "enriched_holdings_cache.json"
        try:
            # Convert defaultdict to regular dict for JSON serialization
            holdings_dict = dict(self.current_holdings)
            
            # Calculate enrichment stats
            enriched_count = sum(1 for h in holdings_dict.values() if 'shares_outstanding' in h and h['shares_outstanding'])
            
            # Get dependency file timestamp
            cusip_shares_file = self.data_dir / "cusip_to_shares_complete.json"
            dependency_timestamp = None
            if cusip_shares_file.exists():
                dependency_timestamp = cusip_shares_file.stat().st_mtime
            
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "quarter": self.quarter,
                "year": self.year,
                "cache_version": "2.0",  # Version to track schema changes
                "holdings_count": len(holdings_dict),
                "enrichment_stats": {
                    "total_holdings": len(holdings_dict),
                    "enriched_with_shares": enriched_count,
                    "enrichment_percentage": (enriched_count / len(holdings_dict) * 100) if holdings_dict else 0
                },
                "dependencies": {
                    "cusip_to_shares_timestamp": dependency_timestamp
                },
                "holdings": holdings_dict
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            logger.info(f"Saved {len(holdings_dict)} enriched holdings to cache (enriched: {enriched_count})")
        except Exception as e:
            logger.error(f"Error saving enriched holdings cache: {e}")
    
    def load_enriched_holdings(self) -> bool:
        """Load enriched holdings from cache file"""
        cache_file = self.data_dir / "enriched_holdings_cache.json"
        
        if not cache_file.exists():
            logger.warning("No enriched holdings cache found")
            return False
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check if cache is for the correct quarter/year
            if cache_data.get('quarter') != self.quarter or cache_data.get('year') != self.year:
                logger.warning(f"Cache is for {cache_data.get('quarter')} {cache_data.get('year')}, but analyzing {self.quarter} {self.year}")
                return False
            
            # Check cache version
            cache_version = cache_data.get('cache_version', '1.0')
            if cache_version != '2.0':
                logger.warning(f"Cache version mismatch (found: {cache_version}, expected: 2.0). Invalidating cache.")
                return False
            
            # Check dependency timestamps
            cusip_shares_file = self.data_dir / "cusip_to_shares_complete.json"
            if cusip_shares_file.exists():
                current_timestamp = cusip_shares_file.stat().st_mtime
                cached_timestamp = cache_data.get('dependencies', {}).get('cusip_to_shares_timestamp', 0)
                if current_timestamp > cached_timestamp:
                    logger.warning("CUSIP to shares mapping file is newer than cache. Invalidating cache.")
                    return False
            
            # Check enrichment quality
            enrichment_stats = cache_data.get('enrichment_stats', {})
            enrichment_pct = enrichment_stats.get('enrichment_percentage', 0)
            
            # Load holdings to check
            holdings = cache_data.get('holdings', {})
            
            # Additional validation: Check if enough holdings have shares_outstanding
            holdings_with_shares = sum(1 for h in holdings.values() if 'shares_outstanding' in h and h['shares_outstanding'])
            actual_enrichment_pct = (holdings_with_shares / len(holdings) * 100) if holdings else 0
            
            # If less than 25% have shares data (expected is ~30-35%), invalidate
            if actual_enrichment_pct < 25:
                logger.warning(f"Cache has insufficient enrichment ({actual_enrichment_pct:.1f}% vs expected >25%). Invalidating cache.")
                return False
            
            # Convert back to defaultdict
            self.current_holdings = defaultdict(lambda: {
                'shares': 0,
                'value': 0,
                'holders': [],
                'name': '',
                'cusip': ''
            })
            
            for cusip, data in holdings.items():
                self.current_holdings[cusip] = data
            
            logger.info(f"Loaded {len(holdings)} enriched holdings from cache (enriched: {holdings_with_shares}, {actual_enrichment_pct:.1f}%)")
            return True
            
        except Exception as e:
            logger.error(f"Error loading enriched holdings cache: {e}")
            return False
    
    def calculate_metrics(self):
        """Calculate percent of shares outstanding metrics using real data"""
        logger.info("Calculating percent of shares outstanding metrics with real data...")
        
        # Check for saved progress
        progress = self.load_progress()
        if progress and progress.get('stage') == 'enrichment_complete':
            logger.info("Found saved enriched data, attempting to load from cache...")
            # Try to load cached enriched holdings
            if not self.load_enriched_holdings():
                logger.warning("Failed to load cached holdings, re-enriching...")
                # Re-enrich if cache load fails
                logger.info("Enriching holdings data with tickers and shares outstanding information...")
                self.current_holdings = self.data_enricher.enrich_holdings(self.current_holdings)
                self.save_enriched_holdings()
                self.save_progress("enrichment_complete")
        else:
            # First, enrich holdings with ticker and shares outstanding data
            logger.info("Enriching holdings data with tickers and shares outstanding information...")
            self.current_holdings = self.data_enricher.enrich_holdings(self.current_holdings)
            self.save_enriched_holdings()
            self.save_progress("enrichment_complete")
        
        # Calculate total holdings as percent of shares outstanding
        total_holdings_pct = []
        unmapped_count = 0
        
        for cusip, data in self.current_holdings.items():
            ticker = data.get('ticker', '')
            name = data.get('name', '').upper()
            
            # Use shares outstanding data if available
            shares_outstanding = data.get('shares_outstanding')
            
            if shares_outstanding and shares_outstanding > 0:
                pct_of_shares = (data['shares'] / shares_outstanding) * 100
                
                # Cap at 101% - anything above this indicates data quality issues
                if pct_of_shares > 101:
                    logger.debug(f"Excluding {data['name']} - ownership {pct_of_shares:.1f}% exceeds 101% cap")
                    continue
                
                # Calculate each institution's percentage of company shares
                positions_with_pct = {}
                for inst_name, inst_position in data.get('positions', {}).items():
                    inst_pct = (inst_position['shares'] / shares_outstanding * 100) if shares_outstanding > 0 else 0
                    positions_with_pct[inst_name] = {
                        'shares': inst_position['shares'],
                        'value': inst_position['value'],
                        'pct_of_company_shares': inst_pct
                    }
                
                total_holdings_pct.append({
                    'cusip': cusip,
                    'ticker': data.get('ticker', 'N/A'),
                    'name': data['name'],
                    'shares': data['shares'],
                    'value': data['value'],
                    'shares_outstanding': shares_outstanding,
                    'float': data.get('float_shares'),
                    'pct_of_shares': pct_of_shares,
                    'num_holders': len(data['holders']),
                    'holders': data['holders'],
                    'positions': positions_with_pct,  # Include positions with calculated percentages
                    'held_pct_institutions': data.get('held_pct_institutions'),
                    'held_pct_insiders': data.get('held_pct_insiders')
                })
            else:
                unmapped_count += 1
                # Skip securities without shares outstanding data
                logger.debug(f"No shares outstanding data for {data['name']} (CUSIP: {cusip})")
        
        logger.info(f"Successfully mapped {len(total_holdings_pct)} securities, {unmapped_count} unmapped")
        
        # Sort by percent of shares outstanding
        total_holdings_pct.sort(key=lambda x: x['pct_of_shares'], reverse=True)
        
        # Load previous quarter data for true net additions calculation
        quarterly_adds_pct = self.calculate_quarterly_net_adds(total_holdings_pct)
        
        return total_holdings_pct, quarterly_adds_pct
    
    def has_filings_for_quarter(self, quarter, year):
        """Check if we have raw filings for a specific quarter"""
        # Map quarter to period ending date
        quarter_end_map = {
            'Q1': '0331',
            'Q2': '0630', 
            'Q3': '0930',
            'Q4': '1231'
        }
        
        period_end = f"{year}{quarter_end_map.get(quarter, '0630')}"
        
        # Check if any filings contain this period
        filing_count = 0
        for cik_dir in self.filings_dir.iterdir():
            if not cik_dir.is_dir():
                continue
                
            filing_type_dir = cik_dir / "13F-HR"
            if not filing_type_dir.exists():
                continue
                
            for filing_dir in filing_type_dir.iterdir():
                if not filing_dir.is_dir():
                    continue
                    
                submission_file = filing_dir / "full-submission.txt"
                if submission_file.exists():
                    with open(submission_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read(10000)  # Read first 10KB
                        if f"CONFORMED PERIOD OF REPORT:\t{period_end}" in content:
                            filing_count += 1
                            if filing_count >= 10:  # If we find at least 10 filings, we have enough
                                return True
        
        return filing_count > 0
    
    def calculate_quarterly_net_adds(self, current_holdings_pct):
        """Calculate true net additions by comparing with previous quarter"""
        logger.info("Calculating quarterly net additions...")
        
        # Determine previous quarter
        current_q = int(self.quarter[1])
        prev_quarter = f"Q{current_q - 1}" if current_q > 1 else "Q4"
        prev_year = self.year if current_q > 1 else self.year - 1
        
        # Check if previous quarter data exists in output
        prev_output_dir = Path(f"output/{prev_quarter}_{prev_year}")
        prev_data_file = prev_output_dir / "total_holdings_data.json"
        
        # If previous quarter output doesn't exist, try to generate it from raw filings
        if not prev_data_file.exists():
            logger.info(f"Previous quarter output not found, checking for raw {prev_quarter} {prev_year} filings...")
            
            # Check if we have raw filings for previous quarter
            if self.has_filings_for_quarter(prev_quarter, prev_year):
                logger.info(f"Found raw filings for {prev_quarter} {prev_year}, processing them first...")
                prev_analyzer = Filing13FAnalyzer(
                    filings_dir=self.filings_dir,
                    quarter=prev_quarter,
                    year=prev_year
                )
                
                # Process previous quarter filings
                prev_analyzer.process_all_filings()
                prev_total_holdings, _ = prev_analyzer.analyze_holdings()
                
                # Save previous quarter results
                prev_analyzer.generate_json_output(prev_total_holdings, "total_holdings_data.json")
                logger.info(f"Generated {prev_quarter} {prev_year} data for comparison")
            else:
                logger.warning(f"No filings found for {prev_quarter} {prev_year}")
                logger.info("Treating all current holdings as new positions")
                return current_holdings_pct
        
        # Load previous quarter data
        logger.info(f"Loading previous quarter data from {prev_quarter} {prev_year}")
        with open(prev_data_file, 'r') as f:
            prev_data = json.load(f)
        
        # Create lookup maps for previous quarter
        prev_positions_by_cusip = {}
        for security in prev_data.get('securities', []):
            cusip = security.get('cusip')
            if cusip:
                prev_positions_by_cusip[cusip] = {
                    'shares': security.get('shares_held', 0),
                    'value': security.get('value_usd', 0),
                    'holders': set(security.get('holders', [])),
                    'positions': security.get('positions', {})
                }
        
        # Calculate net additions for each security
        quarterly_adds = []
        
        for security in current_holdings_pct:
            cusip = security['cusip']
            current_holders = set(security.get('holders', []))
            current_positions = security.get('positions', {})
            
            # Get previous quarter data for this security
            prev_security = prev_positions_by_cusip.get(cusip, {
                'shares': 0,
                'value': 0,
                'holders': set(),
                'positions': {}
            })
            prev_holders = prev_security['holders']
            prev_positions = prev_security['positions']
            
            # Calculate net changes
            shares_change = security['shares'] - prev_security['shares']
            value_change = security['value'] - prev_security['value']
            
            # Find institutions that added or dropped the position
            new_holders = current_holders - prev_holders
            dropped_holders = prev_holders - current_holders
            
            # Calculate net institutional change
            net_institutional_change = len(new_holders) - len(dropped_holders)
            
            # Track individual institution changes
            institution_changes = {}
            
            # Check for position changes in continuing holders
            for inst_name in current_holders & prev_holders:
                current_pos = current_positions.get(inst_name, {'shares': 0})
                prev_pos = prev_positions.get(inst_name, {'shares': 0})
                shares_diff = current_pos.get('shares', 0) - prev_pos.get('shares', 0)
                if shares_diff != 0:
                    institution_changes[inst_name] = {
                        'shares_change': shares_diff,
                        'prev_shares': prev_pos.get('shares', 0),
                        'current_shares': current_pos.get('shares', 0)
                    }
            
            # Add new holders
            for inst_name in new_holders:
                current_pos = current_positions.get(inst_name, {'shares': 0})
                institution_changes[inst_name] = {
                    'shares_change': current_pos.get('shares', 0),
                    'prev_shares': 0,
                    'current_shares': current_pos.get('shares', 0)
                }
            
            # Add dropped holders
            for inst_name in dropped_holders:
                prev_pos = prev_positions.get(inst_name, {'shares': 0})
                institution_changes[inst_name] = {
                    'shares_change': -prev_pos.get('shares', 0),
                    'prev_shares': prev_pos.get('shares', 0),
                    'current_shares': 0
                }
            
            # Create the quarterly add entry
            quarterly_add = security.copy()
            quarterly_add['shares_change'] = shares_change
            quarterly_add['value_change'] = value_change
            quarterly_add['net_institutional_change'] = net_institutional_change
            quarterly_add['net_adds'] = net_institutional_change  # For compatibility with app.py
            quarterly_add['new_holders'] = list(new_holders)
            quarterly_add['dropped_holders'] = list(dropped_holders)
            quarterly_add['institution_changes'] = institution_changes
            quarterly_add['prev_shares'] = prev_security['shares']
            quarterly_add['prev_value'] = prev_security['value']
            quarterly_add['prev_num_holders'] = len(prev_holders)
            
            quarterly_adds.append(quarterly_add)
        
        # Sort by net institutional change (most adds first)
        quarterly_adds.sort(key=lambda x: x['net_institutional_change'], reverse=True)
        
        logger.info(f"Calculated net additions for {len(quarterly_adds)} securities")
        return quarterly_adds
    
    def generate_markdown_report(self, data: List[Dict], report_type: str, output_file: str):
        """Generate a markdown report"""
        logger.info(f"Generating {report_type} report...")
        
        with open(self.output_dir / output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write(f"# {report_type}\n\n")
            f.write(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
            
            # Summary statistics
            f.write("## Summary Statistics\n\n")
            total_value = sum(item['value'] for item in data)
            avg_holders = sum(item['num_holders'] for item in data) / len(data) if data else 0
            high_concentration = [item for item in data if item['pct_of_shares'] > 50]
            
            f.write(f"- Total Securities Analyzed: {len(data)}\n")
            f.write(f"- Total Portfolio Value: ${total_value/1e9:.2f}B\n")
            f.write(f"- Average Holders per Security: {avg_holders:.1f}\n")
            f.write(f"- High Concentration Positions (>50% of shares outstanding): {len(high_concentration)}\n")
            
            # Add filing period breakdown if available
            if hasattr(self, 'institution_filing_periods') and self.institution_filing_periods:
                target_q = int(self.quarter[1])
                target_count = sum(1 for p in self.institution_filing_periods.values() 
                                 if p['quarter'] == target_q and p['year'] == self.year)
                fallback_count = len(self.institution_filing_periods) - target_count
                f.write(f"- Institutions Included: {len(self.institution_filing_periods)} total\n")
                f.write(f"  - {target_count} from Q{target_q} {self.year} (current quarter)\n")
                f.write(f"  - {fallback_count} from earlier quarters (fallback data)\n")
            f.write("\n")
            
            # Top 50 table
            f.write("## Top 50 Positions by Percent of Shares Outstanding\n\n")
            f.write("| Rank | Security | Ticker | Shares Held | Shares Outstanding | % of Shares | # Holders | Value ($M) |\n")
            f.write("|------|----------|--------|-------------|-------------------|------------|-----------|------------|\n")
            
            for i, item in enumerate(data[:50], 1):
                ticker = item.get('ticker', 'N/A')
                f.write(f"| {i} | {item['name'][:30]} | {ticker} | "
                       f"{item['shares']:,} | {item['shares_outstanding']:,} | "
                       f"{item['pct_of_shares']:.2f}% | {item['num_holders']} | "
                       f"${item['value']/1e6:.1f} |\n")
            
            # Concentration analysis
            f.write("\n## Concentration Risk Analysis\n\n")
            f.write("### Extremely High Concentration (>70% of shares outstanding)\n\n")
            
            extreme_concentration = [item for item in data if item['pct_of_shares'] > 70]
            if extreme_concentration:
                for item in extreme_concentration[:10]:
                    f.write(f"- **{item['name']}**: {item['pct_of_shares']:.2f}% of shares outstanding "
                           f"held by {item['num_holders']} institutions\n")
            else:
                f.write("*No positions exceed 70% of shares outstanding*\n")
            
            # Most widely held
            f.write("\n### Most Widely Held Securities\n\n")
            widely_held = sorted(data, key=lambda x: x['num_holders'], reverse=True)[:10]
            
            for item in widely_held:
                f.write(f"- **{item['name']}**: {item['num_holders']} holders "
                       f"({item['pct_of_shares']:.2f}% of shares outstanding)\n")
            
            # Key insights
            f.write("\n## Key Insights\n\n")
            
            if report_type == "Total Institutional Holdings as % of Shares Outstanding":
                f.write("- **Crowded Trades**: Securities with high institutional ownership "
                       "may face liquidity issues during market stress\n")
                f.write("- **Consensus Positions**: Widely-held names represent institutional consensus\n")
                f.write("- **Exit Risk**: High concentration positions may be difficult to exit "
                       "without significant market impact\n")
            else:
                f.write("- **Fresh Capital**: These positions represent new institutional interest this quarter\n")
                f.write("- **Momentum Plays**: High net additions suggest growing institutional confidence\n")
                f.write("- **Sector Rotation**: Analyze sector composition to identify rotation patterns\n")
            
            # Top holders for top 10 positions
            f.write("\n## Detailed Holdings (Top 10)\n\n")
            for i, item in enumerate(data[:10], 1):
                f.write(f"### {i}. {item['name']}\n")
                f.write(f"- **CUSIP**: {item['cusip']}\n")
                f.write(f"- **% of Shares Outstanding**: {item['pct_of_shares']:.2f}%\n")
                f.write(f"- **Total Value**: ${item['value']/1e6:.1f}M\n")
                f.write(f"- **Key Holders**: {', '.join(item['holders'][:5])}")
                if len(item['holders']) > 5:
                    f.write(f" and {len(item['holders'])-5} others")
                f.write("\n\n")
        
        logger.info(f"Report saved to {self.output_dir / output_file}")
    
    def generate_json_output(self, data: List[Dict], output_file: str = "holdings_data.json"):
        """Generate JSON output with all securities data"""
        logger.info(f"Generating JSON output with all {len(data)} securities...")
        
        # Calculate institution breakdown
        institution_breakdown = {}
        if hasattr(self, 'institution_filing_periods') and self.institution_filing_periods:
            target_q = int(self.quarter[1])
            target_institutions = [name for name, p in self.institution_filing_periods.items() 
                                 if p['quarter'] == target_q and p['year'] == self.year]
            fallback_institutions = [name for name, p in self.institution_filing_periods.items() 
                                    if not (p['quarter'] == target_q and p['year'] == self.year)]
            institution_breakdown = {
                "total_institutions": len(self.institution_filing_periods),
                "current_quarter_institutions": len(target_institutions),
                "fallback_institutions": len(fallback_institutions),
                "filing_periods": self.institution_filing_periods
            }
        
        # Prepare the output structure
        output = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_securities": len(data),
                "total_value_usd": sum(item['value'] for item in data),
                "data_source": f"{self.quarter} {self.year} 13F Filings",
                "metric": "Percentage of Shares Outstanding",
                "institution_breakdown": institution_breakdown
            },
            "securities": []
        }
        
        # Add all securities data
        for item in data:
            security = {
                "cusip": item['cusip'],
                "ticker": item.get('ticker', 'N/A'),
                "name": item['name'],
                "shares_held": item.get('shares', item.get('shares_held', 0)),  # Handle both field names
                "shares_outstanding": item['shares_outstanding'],
                "pct_of_shares_outstanding": round(item['pct_of_shares'], 4),
                "value_usd": item.get('value', item.get('value_usd', 0)),  # Handle both field names
                "num_holders": item['num_holders'],
                "holders": item['holders'],
                "positions": item.get('positions', {}),  # Include individual positions
                "held_pct_institutions": item.get('held_pct_institutions'),
                "held_pct_insiders": item.get('held_pct_insiders'),
                # Add quarterly comparison fields if present
                "net_adds": item.get('net_adds'),
                "shares_change": item.get('shares_change'),
                "value_change": item.get('value_change'),
                "net_institutional_change": item.get('net_institutional_change'),
                "new_holders": item.get('new_holders'),
                "dropped_holders": item.get('dropped_holders'),
                "institution_changes": item.get('institution_changes'),
                "prev_shares": item.get('prev_shares'),
                "prev_value": item.get('prev_value'),
                "prev_num_holders": item.get('prev_num_holders')
            }
            output["securities"].append(security)
        
        # Save to JSON file
        with open(self.output_dir / output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"JSON data saved to {self.output_dir / output_file}")
    
    def run_analysis(self):
        """Run the complete analysis"""
        logger.info("Starting 13F net additions analysis...")
        
        # Process all filings
        self.process_all_filings()
        
        # Calculate metrics
        total_holdings, quarterly_adds = self.calculate_metrics()
        
        # Generate reports
        self.generate_markdown_report(
            total_holdings,
            "Total Institutional Holdings as % of Shares Outstanding",
            "total_holdings_report.md"
        )
        
        self.generate_markdown_report(
            quarterly_adds,
            f"{self.quarter} {self.year} Net Additions as % of Shares Outstanding",
            "quarterly_net_adds_report.md"
        )
        
        # Generate JSON output with all data
        self.generate_json_output(total_holdings, "total_holdings_data.json")
        self.generate_json_output(quarterly_adds, "quarterly_adds_data.json")
        
        # Save any newly extracted institution names for future use
        if hasattr(self, 'cik_metadata') and self.cik_metadata:
            cache_file = self.data_dir / 'extracted_institution_names.json'
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self.cik_metadata, f, indent=2)
                logger.info(f"Saved extracted institution names to {cache_file}")
            except Exception as e:
                logger.warning(f"Could not save extracted names cache: {e}")
        
        logger.info("Analysis complete!")
        
        # Print summary
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE")
        print("="*60)
        
        # Display parsing statistics
        if hasattr(self, 'filing_stats'):
            print(f"\nParsing Statistics:")
            print(f"  Total filings processed: {self.filing_stats['total_filings']}")
            print(f"  Successfully parsed: {self.filing_stats['successful_parses']}")
            print(f"  Failed to parse: {self.filing_stats['failed_parses']}")
            print(f"  Success rate: {self.filing_stats['parse_success_rate']}%")
        
        print(f"\nTop 5 by Total Holdings (% of Shares Outstanding):")
        for i, item in enumerate(total_holdings[:5], 1):
            print(f"{i}. {item['name'][:30]:<30} {item['pct_of_shares']:>6.2f}% ({item['num_holders']} holders)")
        
        print(f"\nReports generated:")
        print(f"- {self.output_dir / 'total_holdings_report.md'}")
        print(f"- {self.output_dir / 'quarterly_net_adds_report.md'}")
        print(f"- {self.output_dir / 'total_holdings_data.json'} (Complete data for all {len(total_holdings)} securities)")
        print(f"- {self.output_dir / 'quarterly_adds_data.json'}")


def main():
    """Main entry point"""
    import argparse
    
    # Get dynamic defaults based on downloaded data
    default_quarter, default_year = get_data_driven_defaults()
    
    parser = argparse.ArgumentParser(description='Analyze 13F filings')
    parser.add_argument('--quarter', default=default_quarter, help='Quarter (e.g., Q2, Q3)')
    parser.add_argument('--year', type=int, default=default_year, help='Year')
    parser.add_argument('--force-refresh', action='store_true', 
                        help='Force re-analysis, ignoring cached data')
    args = parser.parse_args()
    
    # Determine if we need to run both quarters
    target_quarter = args.quarter
    target_year = args.year
    
    # Determine previous quarter
    current_q = int(target_quarter[1])
    prev_quarter = f"Q{current_q - 1}" if current_q > 1 else "Q4"
    prev_year = target_year if current_q > 1 else target_year - 1
    
    # Check if previous quarter data exists
    prev_output_dir = Path(f"output/{prev_quarter}_{prev_year}")
    prev_data_file = prev_output_dir / "total_holdings_data.json"
    
    # Run previous quarter first if it doesn't exist
    if not prev_data_file.exists():
        logger.info(f"Previous quarter data not found. Processing {prev_quarter} {prev_year} first...")
        prev_analyzer = Filing13FAnalyzer(quarter=prev_quarter, year=prev_year)
        
        # Clear cache if force refresh
        if args.force_refresh:
            cache_files = [
                prev_analyzer.progress_file,
                prev_analyzer.data_dir / "enriched_holdings_cache.json"
            ]
            for cache_file in cache_files:
                if cache_file.exists():
                    cache_file.unlink()
        
        prev_analyzer.run_analysis()
        logger.info(f"Completed {prev_quarter} {prev_year} analysis")
        print("\n" + "="*60)
        print(f"Now processing {target_quarter} {target_year} with comparison data...")
        print("="*60 + "\n")
    
    # Now run the target quarter
    analyzer = Filing13FAnalyzer(quarter=target_quarter, year=target_year)
    
    # Clear cache if force refresh
    if args.force_refresh:
        logger.info("Force refresh requested, clearing cache...")
        cache_files = [
            analyzer.progress_file,
            analyzer.data_dir / "enriched_holdings_cache.json"
        ]
        for cache_file in cache_files:
            if cache_file.exists():
                cache_file.unlink()
                logger.info(f"Deleted cache file: {cache_file}")
    
    analyzer.run_analysis()


if __name__ == "__main__":
    main()