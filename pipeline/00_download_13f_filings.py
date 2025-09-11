"""
Download 13F-HR filings using sec-edgar-downloader package
Much simpler and more reliable approach
"""

import os
import json
import requests
import time
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging
from tqdm import tqdm
from sec_edgar_downloader import Downloader
from path_config import get_paths
from config_loader import load_config_with_env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ModernSEC13FDownloader:
    """Download 13F-HR filings using sec-edgar-downloader"""
    
    def __init__(self, company_name: str = None, 
                 email: str = None,
                 output_dir: str = None,
                 institution_filter: Dict = None):
        """
        Initialize the downloader
        
        Args:
            company_name: Your company name (for SEC user agent)
            email: Your email (for SEC user agent)
            output_dir: Directory to save downloaded filings
        """
        # Get correct paths
        self.paths = get_paths()
        
        # Load config and get user agent from env vars if not provided
        config = load_config_with_env(self.paths['analysis_config'])
        if company_name is None:
            company_name = config.get('user_agent', {}).get('name', 'IndividualInvestor')
        if email is None:
            email = config.get('user_agent', {}).get('email', 'investor@example.com')
        
        if output_dir is None:
            output_dir = str(self.paths['data'] / "13f_filings")
        
        self.downloader = Downloader(company_name, email, output_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Store institution filter settings FIRST
        self.institution_filter = institution_filter or {}
        
        # Track progress
        self.progress_file = self.output_dir / "download_progress_v2.json"
        self.downloaded_ciks = self.load_progress()
        
        # Initialize target firms if needed
        self.initialize_target_firms()
        
        # Load CIK metadata
        self.cik_metadata = self.load_cik_metadata()
        
        # Track detailed results
        self.successful_downloads = []
        self.failed_downloads = []
        self.skipped_downloads = []
        
        # Track filing metadata
        self.filing_metadata = {}
    
    def load_progress(self) -> set:
        """Load previously downloaded CIKs"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_progress(self):
        """Save download progress"""
        with open(self.progress_file, 'w') as f:
            json.dump(list(self.downloaded_ciks), f, indent=2)
    
    def initialize_target_firms(self):
        """Auto-initialize target_firms.json from clean_institutions.csv"""
        target_firms_path = self.paths['target_firms']
        
        # Always recreate from clean_institutions.csv to get latest data
        logger.info("Initializing target_firms.json from clean_institutions.csv...")
        
        # Read the CSV file
        csv_path = self.paths['config'] / "clean_institutions.csv"
        if not csv_path.exists():
            logger.error("clean_institutions.csv not found! Cannot initialize.")
            return
        
        firms = []
        all_institutions = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            row_num = 0
            for row in reader:
                row_num += 1
                if not row.get('Institution'):
                    continue
                    
                inst_name = row['Institution'].strip()
                cik = row.get('CIK', '').strip()
                
                if not cik:
                    logger.warning(f"No CIK for {inst_name} at row {row_num}")
                    continue
                
                institution_data = {
                    'row_number': row_num,
                    'cik': cik,
                    'name': inst_name,
                    'type': row.get('Type', 'Unknown'),
                    'aum_billions': float(row.get('AUM_Billions', 0)) if row.get('AUM_Billions') else 0,
                    'key_person': row.get('Key_Person', '')
                }
                all_institutions.append(institution_data)
        
        # Apply filtering based on institution_filter settings
        if self.institution_filter.get('top'):
            # Get top N institutions
            n = self.institution_filter['top']
            filtered_institutions = all_institutions[:n]
            logger.info(f"Filtering to top {n} institutions")
        elif self.institution_filter.get('range'):
            # Get institutions in range
            start, end = self.institution_filter['range']
            filtered_institutions = all_institutions[start-1:end]  # Convert to 0-based indexing
            logger.info(f"Filtering to institutions {start}-{end}")
        else:
            # Get all institutions
            filtered_institutions = all_institutions
            logger.info(f"Using all {len(all_institutions)} institutions")
        
        # Create firms list for target_firms.json
        firms = []
        for inst in filtered_institutions:
            firms.append({
                'cik': inst['cik'],
                'name': inst['name']
            })
        
        # Save to target_firms.json
        target_firms_data = {'firms': firms}
        with open(target_firms_path, 'w') as f:
            json.dump(target_firms_data, f, indent=2)
        
        logger.info(f"Created target_firms.json with {len(firms)} institutions")
        
        # Also update cik_metadata.json
        metadata_file = self.paths['cik_metadata']
        logger.info("Creating cik_metadata.json...")
        metadata = {"cik_details": {}}
        
        for inst in filtered_institutions:
            metadata['cik_details'][inst['cik']] = {
                'name': inst['name'],
                'type': inst['type'],
                'key_person': inst['key_person'],
                'aum_billions': inst['aum_billions']
            }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Created cik_metadata.json with {len(metadata['cik_details'])} entries")
    
    def load_cik_metadata(self) -> Dict:
        """Load CIK metadata from JSON file"""
        metadata_file = self.paths['cik_metadata']
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("cik_details", {})
        return {}
    
    def get_quarter_dates(self, year: int, quarter: int) -> Tuple[datetime, datetime]:
        """
        Get start and end dates for a specific quarter
        
        Args:
            year: Year (e.g., 2025)
            quarter: Quarter number (1-4)
            
        Returns:
            Tuple of (start_date, end_date) for the quarter
        """
        if quarter not in [1, 2, 3, 4]:
            raise ValueError("Quarter must be 1, 2, 3, or 4")
        
        quarter_months = {
            1: (1, 3),   # Q1: Jan-Mar
            2: (4, 6),   # Q2: Apr-Jun
            3: (7, 9),   # Q3: Jul-Sep
            4: (10, 12)  # Q4: Oct-Dec
        }
        
        start_month, end_month = quarter_months[quarter]
        start_date = datetime(year, start_month, 1)
        
        # Get last day of end month
        if end_month == 12:
            end_date = datetime(year, 12, 31)
        else:
            end_date = datetime(year, end_month + 1, 1) - timedelta(days=1)
        
        return start_date, end_date
    
    def get_latest_completed_quarter(self) -> Tuple[int, int]:
        """
        Determine the latest completed quarter based on today's date
        
        Returns:
            Tuple of (year, quarter)
        """
        today = datetime.now()
        current_month = today.month
        current_year = today.year
        
        if current_month <= 3:
            # Q1 in progress, latest complete is Q4 of previous year
            return current_year - 1, 4
        elif current_month <= 6:
            # Q2 in progress, latest complete is Q1
            return current_year, 1
        elif current_month <= 9:
            # Q3 in progress, latest complete is Q2
            return current_year, 2
        else:
            # Q4 in progress, latest complete is Q3
            return current_year, 3
    
    def get_filing_window(self, quarter_end_date: datetime) -> Tuple[str, str]:
        """
        Get the filing window for 13F forms (45 days after quarter end)
        
        Args:
            quarter_end_date: Last day of the quarter
            
        Returns:
            Tuple of (start_date, end_date) for filing window in YYYY-MM-DD format
        """
        # 13F filings are due 45 days after quarter end
        filing_deadline = quarter_end_date + timedelta(days=45)
        
        # Most filings come in the 2 weeks before deadline
        filing_start = filing_deadline - timedelta(days=14)
        
        # Some may file late, so check a few days after deadline too
        filing_end = filing_deadline + timedelta(days=7)
        
        # If filing_end is in the future, use today instead
        today = datetime.now()
        if filing_end > today:
            filing_end = today
        
        return filing_start.strftime("%Y-%m-%d"), filing_end.strftime("%Y-%m-%d")
    
    def get_13f_filers_ciks(self) -> List[str]:
        """
        Get list of CIKs for companies that file 13F forms
        Reads from config/target_firms.json which is created from clean_institutions.csv
        """
        logger.info("Loading list of 13F filers from config...")
        
        # Load from target_firms.json which was created by initialize_target_firms
        target_firms_path = self.paths['target_firms']
        if target_firms_path.exists():
            with open(target_firms_path, 'r') as f:
                data = json.load(f)
                ciks = [firm['cik'] for firm in data['firms']]
                logger.info(f"Loaded {len(ciks)} CIKs from target_firms.json")
                return ciks
        else:
            logger.error("target_firms.json not found! Run initialize_target_firms first.")
            return []
    
    def download_quarter_filings(self, year: Optional[int] = None, 
                                quarter: Optional[int] = None,
                                cik_list: Optional[List[str]] = None):
        """
        Download all 13F-HR filings for a specific quarter
        
        Args:
            year: Year (if None, uses latest completed quarter)
            quarter: Quarter number 1-4 (if None, uses latest completed quarter)
            cik_list: List of CIKs to download (if None, uses known 13F filers)
        """
        # Determine which quarter to download
        if year is None or quarter is None:
            year, quarter = self.get_latest_completed_quarter()
            logger.info(f"Auto-detected latest completed quarter: Q{quarter} {year}")
        
        # Get quarter dates
        quarter_start, quarter_end = self.get_quarter_dates(year, quarter)
        
        # Get filing window (when filings are actually submitted)
        after_date, before_date = self.get_filing_window(quarter_end)
        
        logger.info(f"Downloading Q{quarter} {year} 13F-HR filings")
        logger.info(f"Quarter period: {quarter_start.strftime('%Y-%m-%d')} to {quarter_end.strftime('%Y-%m-%d')}")
        logger.info(f"Filing window: {after_date} to {before_date}")
        
        # Get list of CIKs to download
        if cik_list is None:
            cik_list = self.get_13f_filers_ciks()
        
        logger.info(f"Will attempt to download filings for {len(cik_list)} entities")
        
        # Download filings for each CIK
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Reset tracking lists
        self.successful_downloads = []
        self.failed_downloads = []
        self.skipped_downloads = []
        
        with tqdm(total=len(cik_list), desc=f"Downloading Q{quarter} {year} filings") as pbar:
            for cik in cik_list:
                # Get company name from metadata
                company_info = self.cik_metadata.get(cik, {})
                company_name = company_info.get("name", f"CIK {cik}")
                
                # Skip if already downloaded
                if cik in self.downloaded_ciks:
                    skipped_count += 1
                    self.skipped_downloads.append({
                        "cik": cik,
                        "name": company_name,
                        "investor": company_info.get("investor", ""),
                        "type": company_info.get("type", "")
                    })
                    pbar.update(1)
                    continue
                
                try:
                    # Try to download without date restrictions to avoid sec-edgar-downloader date bug
                    # Get the most recent filing(s)
                    logger.debug(f"Downloading most recent 13F-HR for {company_name} (CIK: {cik})")
                    
                    # IMPORTANT: Do not include amendments to avoid incomplete data
                    self.downloader.get(
                        "13F-HR",
                        cik,
                        limit=2,  # Get 2 most recent filings
                        include_amends=False  # Exclude amendments to get full portfolio data
                    )
                    
                    # Check if we got any filings and determine period
                    filing_path = self.output_dir / "sec-edgar-filings" / cik / "13F-HR"
                    filing_period = None
                    
                    if filing_path.exists() and any(filing_path.iterdir()):
                        # Extract filing period from the most recent filing
                        latest_filing = max(filing_path.iterdir(), key=lambda x: x.name)
                        filing_period = self.extract_filing_period(latest_filing)
                        
                        period_str = f"Q{filing_period.get('quarter', '?')} {filing_period.get('year', '?')}" if filing_period else "Unknown period"
                        logger.debug(f"Got filing for {company_name}: {period_str}")
                    
                    success_count += 1
                    self.successful_downloads.append({
                        "cik": cik,
                        "name": company_name,
                        "investor": company_info.get("investor", ""),
                        "type": company_info.get("type", ""),
                        "filing_period": filing_period
                    })
                    self.downloaded_ciks.add(cik)
                    self.save_progress()
                    
                    # Save filing metadata
                    if not hasattr(self, 'filing_metadata'):
                        self.filing_metadata = {}
                    self.filing_metadata[cik] = {
                        "name": company_name,
                        "filing_period": filing_period,
                        "download_date": datetime.now().isoformat()
                    }
                    
                except Exception as e:
                    # Some CIKs might not have any filings
                    logger.debug(f"No filings or error for {company_name} (CIK {cik}): {e}")
                    failed_count += 1
                    self.failed_downloads.append({
                        "cik": cik,
                        "name": company_name,
                        "investor": company_info.get("investor", ""),
                        "type": company_info.get("type", ""),
                        "error": str(e)
                    })
                
                finally:
                    pbar.update(1)
                    # Small delay to be respectful to SEC servers
                    time.sleep(0.1)
        
        logger.info(f"Download complete: {success_count} successful, {failed_count} no filings/failed, {skipped_count} skipped")
        logger.info(f"Files saved to: {self.output_dir.absolute()}")
        
        # Save filing metadata
        if hasattr(self, 'filing_metadata'):
            metadata_file = self.output_dir / "filing_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(self.filing_metadata, f, indent=2)
            logger.info(f"Filing metadata saved to {metadata_file}")
        
        # Generate detailed report
        self.generate_download_report(year, quarter)
        
        # Save latest download metadata for other scripts to use
        self.save_latest_download_metadata(year, quarter)
    
    def extract_filing_period(self, filing_dir: Path) -> Optional[Dict]:
        """Extract the filing period from a 13F filing"""
        try:
            # Try to read the filing
            submission_file = filing_dir / "full-submission.txt"
            if not submission_file.exists():
                return None
            
            with open(submission_file, 'r', encoding='utf-8', errors='ignore') as f:
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
    
    def generate_download_report(self, year: int, quarter: int):
        """Generate a detailed report of the download results"""
        report_file = self.output_dir / f"download_report_Q{quarter}_{year}.txt"
        csv_file = self.output_dir / f"download_summary_Q{quarter}_{year}.csv"
        
        # Generate text report
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write(f"13F-HR DOWNLOAD REPORT - Q{quarter} {year}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary statistics
            f.write("SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total attempted: {len(self.successful_downloads) + len(self.failed_downloads) + len(self.skipped_downloads)}\n")
            f.write(f"Successfully downloaded: {len(self.successful_downloads)}\n")
            f.write(f"Failed/No filings: {len(self.failed_downloads)}\n")
            f.write(f"Skipped (already downloaded): {len(self.skipped_downloads)}\n\n")
            
            # Successful downloads
            f.write("SUCCESSFUL DOWNLOADS\n")
            f.write("-" * 40 + "\n")
            if self.successful_downloads:
                for item in sorted(self.successful_downloads, key=lambda x: x['name']):
                    f.write(f"[SUCCESS] {item['name']:<40} ({item['type']})\n")
                    if item['investor']:
                        f.write(f"  Investor: {item['investor']}\n")
                    f.write(f"  CIK: {item['cik']}\n\n")
            else:
                f.write("None\n\n")
            
            # Failed downloads
            f.write("FAILED/NO FILINGS\n")
            f.write("-" * 40 + "\n")
            if self.failed_downloads:
                for item in sorted(self.failed_downloads, key=lambda x: x['name']):
                    f.write(f"[FAILED] {item['name']:<40} ({item['type']})\n")
                    if item['investor']:
                        f.write(f"  Investor: {item['investor']}\n")
                    f.write(f"  CIK: {item['cik']}\n")
                    f.write(f"  Likely reason: No filing for Q{quarter} {year}\n\n")
            else:
                f.write("None\n\n")
            
            # Skipped downloads
            if self.skipped_downloads:
                f.write("SKIPPED (ALREADY DOWNLOADED)\n")
                f.write("-" * 40 + "\n")
                for item in sorted(self.skipped_downloads, key=lambda x: x['name']):
                    f.write(f"- {item['name']:<40} ({item['type']})\n")
        
        # Generate CSV summary
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Status', 'CIK', 'Name', 'Investor', 'Type']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write successful downloads
            for item in self.successful_downloads:
                writer.writerow({
                    'Status': 'Success',
                    'CIK': item['cik'],
                    'Name': item['name'],
                    'Investor': item['investor'],
                    'Type': item['type']
                })
            
            # Write failed downloads
            for item in self.failed_downloads:
                writer.writerow({
                    'Status': 'Failed/No Filing',
                    'CIK': item['cik'],
                    'Name': item['name'],
                    'Investor': item['investor'],
                    'Type': item['type']
                })
            
            # Write skipped downloads
            for item in self.skipped_downloads:
                writer.writerow({
                    'Status': 'Skipped',
                    'CIK': item['cik'],
                    'Name': item['name'],
                    'Investor': item['investor'],
                    'Type': item['type']
                })
        
        print(f"\n[REPORT] Detailed report saved to: {report_file}")
        print(f"[REPORT] CSV summary saved to: {csv_file}")
        
        # Print summary to console
        print("\n" + "=" * 60)
        print("DOWNLOAD SUMMARY")
        print("=" * 60)
        
        print("\n[SUCCESS] SUCCESSFUL DOWNLOADS:")
        if self.successful_downloads:
            for item in sorted(self.successful_downloads, key=lambda x: x['name'])[:10]:
                print(f"  - {item['name']} ({item['type']})")
            if len(self.successful_downloads) > 10:
                print(f"  ... and {len(self.successful_downloads) - 10} more")
        else:
            print("  None")
        
        print("\n[FAILED] NO FILINGS FOUND:")
        if self.failed_downloads:
            for item in sorted(self.failed_downloads, key=lambda x: x['name']):
                print(f"  - {item['name']} ({item['type']})")
        else:
            print("  None")
    
    def save_latest_download_metadata(self, year: int, quarter: int):
        """Save metadata about the latest download for other scripts to use"""
        metadata = {
            "last_download": datetime.now().isoformat(),
            "quarter": quarter,
            "year": year,
            "successful_downloads": len(self.successful_downloads),
            "failed_downloads": len(self.failed_downloads),
            "total_attempted": len(self.successful_downloads) + len(self.failed_downloads)
        }
        
        metadata_file = self.output_dir / "latest_download.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download 13F-HR filings from SEC EDGAR')
    parser.add_argument('--quarter', type=int, help='Quarter (1-4)', default=None)
    parser.add_argument('--year', type=int, help='Year (e.g., 2025)', default=None)
    parser.add_argument('--company', type=str, help='Company/Your Name (for SEC)', 
                       default='Research Project')
    parser.add_argument('--email', type=str, help='Email address (for SEC)', 
                       default='research@example.com')
    parser.add_argument('--force', action='store_true',
                       help='Force re-download, ignoring progress cache')
    parser.add_argument('--interactive', action='store_true', 
                       help='Run in interactive mode')
    
    # Institution selection arguments
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--top', type=int, metavar='N',
                       help='Download only top N institutions from clean_institutions.csv')
    group.add_argument('--range', type=int, nargs=2, metavar=('START', 'END'),
                       help='Download institutions from row START to END (1-based)')
    group.add_argument('--all', action='store_true',
                       help='Download all institutions (default behavior)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("SEC EDGAR 13F-HR Downloader")
    print("=" * 60)
    
    # Use command-line args or interactive mode
    if args.interactive:
        # Interactive mode
        print("\nPlease provide your information (required by SEC):")
        company_name = input("Company/Your Name: ").strip()
        if not company_name:
            company_name = "Individual Investor"
        
        email = input("Email address: ").strip()
        if not email or "@" not in email:
            print("Valid email required for SEC compliance")
            return
        
        print("\n" + "=" * 60)
        print("Download Options:")
        print("1. Download latest completed quarter (automatic)")
        print("2. Download specific quarter")
        print("=" * 60)
        
        choice = input("\nSelect option (1 or 2): ").strip()
        
        year = None
        quarter = None
        
        if choice == "2":
            # Get specific quarter from user
            try:
                year = int(input("Enter year (e.g., 2025): "))
                quarter = int(input("Enter quarter (1-4): "))
                if quarter not in [1, 2, 3, 4]:
                    print("Invalid quarter. Please enter 1, 2, 3, or 4.")
                    return
            except ValueError:
                print("Invalid input. Please enter numbers only.")
                return
        elif choice != "1":
            print("Invalid choice. Please select 1 or 2.")
            return
    else:
        # Command-line mode
        company_name = args.company
        email = args.email
        year = args.year
        quarter = args.quarter
        
        if quarter and quarter not in [1, 2, 3, 4]:
            print("Invalid quarter. Please enter 1, 2, 3, or 4.")
            return
    
    # Prepare institution filter
    institution_filter = {}
    if args.top:
        institution_filter['top'] = args.top
        print(f"Will download top {args.top} institutions")
    elif args.range:
        institution_filter['range'] = args.range
        print(f"Will download institutions {args.range[0]}-{args.range[1]}")
    else:
        print("Will download all institutions from clean_institutions.csv")
    
    # Create downloader instance with institution filter
    downloader = ModernSEC13FDownloader(company_name, email, 
                                       institution_filter=institution_filter)
    
    # Clear progress if force flag is set
    if args.force:
        print("Force flag set - clearing download progress cache")
        downloader.downloaded_ciks.clear()
        downloader.save_progress()
    
    # Show what will be downloaded
    if year and quarter:
        print(f"\nDownloading Q{quarter} {year} filings...")
    else:
        latest_year, latest_quarter = downloader.get_latest_completed_quarter()
        print(f"\nDownloading latest quarter: Q{latest_quarter} {latest_year}")
        year = latest_year
        quarter = latest_quarter
    
    print("This may take a while depending on the number of filers.\n")
    
    try:
        downloader.download_quarter_filings(year, quarter)
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        print("Progress has been saved. Run again to resume.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\nError occurred: {e}")
        print("Progress has been saved. You can run again to resume.")


if __name__ == "__main__":
    main()