# 13F Institutional Holdings Analyzer

A comprehensive pipeline for analyzing SEC 13F institutional holdings filings with an interactive Streamlit dashboard. Track institutional positions, identify consensus trades, and visualize quarter-over-quarter changes across major investment firms.

## 🎯 Key Features

- **Automated 13F Filing Download**: Pulls latest institutional holdings from SEC EDGAR
- **Quarter-over-Quarter Analysis**: Tracks position changes and identifies new institutional bets
- **Interactive Dashboard**: Explore holdings by institution, security, or portfolio metrics
- **Consensus Detection**: Identifies securities with highest institutional conviction
- **Real-time Filtering**: Dynamic filtering by institution type, specific investors, or securities

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/13f-holdings-analyzer.git
cd 13f-holdings-analyzer

# Install dependencies
pip install -r requirements.txt

# Run the analysis pipeline
python pipeline/05_analyze_net_adds.py

# Launch the dashboard
streamlit run app.py
```

## 📁 Essential Files Structure

```
13f-holdings-analyzer/
│
├── app.py                              # Interactive Streamlit dashboard
├── requirements.txt                    # Python dependencies
├── CLAUDE.md                          # Project documentation
│
├── config/
│   ├── analysis_config.json          # Configuration for quarters, paths, and analysis
│   └── clean_institutions.csv        # List of tracked institutions (top 100)
│
├── pipeline/
│   ├── 00_download_13f_filings.py    # Download 13F-HR filings from SEC EDGAR
│   ├── 01_download_sec_tickers.py    # Fetch SEC ticker mappings
│   ├── 02_build_cusip_mappings.py    # Build CUSIP to ticker mappings
│   ├── 03_get_sec_shares_outstanding.py # Get shares outstanding data
│   ├── 04_complete_cusip_mapping.py  # Complete missing CUSIP mappings
│   ├── 05_analyze_net_adds.py        # Main analysis: calculate net adds & holdings
│   └── path_config.py                # Centralized path configuration
│
└── modules/
    └── data_enrichment_sec.py        # SEC data enrichment utilities
```

## 🔧 Setup Instructions

### Prerequisites

- Python 3.8 or higher
- 10GB+ free disk space for data storage
- Internet connection for SEC EDGAR access

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/13f-holdings-analyzer.git
   cd 13f-holdings-analyzer
   ```

2. **Create virtual environment (recommended)**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure SEC User Agent**
   
   Edit `config/analysis_config.json` to add your contact info (required by SEC):
   ```json
   {
     "sec_user_agent": {
       "company": "Your Name/Company",
       "email": "your.email@example.com"
     }
   }
   ```

## 📊 Running the Pipeline

### Option 1: Run Complete Analysis (Recommended)

```bash
# This runs the complete pipeline with proper quarter detection
python pipeline/05_analyze_net_adds.py
```

This will:
- Detect the latest completed quarter with 45-day filing deadline
- Process both current and previous quarter if needed
- Generate holdings analysis and net additions
- Output JSON data for the dashboard

### Option 2: Step-by-Step Pipeline

```bash
# Step 1: Download 13F filings (takes 10-30 minutes)
python pipeline/00_download_13f_filings.py --top 100

# Step 2: Download SEC reference data
python pipeline/01_download_sec_tickers.py

# Step 3: Build CUSIP mappings
python pipeline/02_build_cusip_mappings.py

# Step 4: Get shares outstanding data
python pipeline/03_get_sec_shares_outstanding.py

# Step 5: Complete CUSIP mapping
python pipeline/04_complete_cusip_mapping.py

# Step 6: Run main analysis
python pipeline/05_analyze_net_adds.py
```

### Pipeline Output

After running, you'll find results in:
- `output/Q2_2025/` (or current quarter)
  - `total_holdings_data.json` - Complete holdings data
  - `quarterly_adds_data.json` - Quarter-over-quarter changes
  - `*.md` - Human-readable reports

## 🖥️ Using the Dashboard

### Launch the Dashboard

```bash
streamlit run app.py
```

Opens at: http://localhost:8501

### Dashboard Features

#### 📈 Overview Tab
- **Scatter Plot**: Portfolio allocation vs company ownership
- **Top Holdings Bar Chart**: Highest value positions

#### 🏆 Top Holdings Tab
- **Sortable Table**: All holdings with ownership percentages
- **Filters**: By institution, investor type, value range

#### 🆕 Latest Additions Tab
- **Top Portfolio Increases**: Securities with highest average portfolio % increases
- **Top Portfolio Decreases**: Securities institutions are reducing

#### 🔍 Securities Detail (Search)
- Search any ticker (e.g., "AAPL", "MSFT", "UNH")
- View all institutional holders
- See quarter-over-quarter changes with visual indicators
- Portfolio impact percentages

### Key Metrics Explained

- **% of Shares Outstanding**: How much of the company institutions own
- **% of Institution Portfolio**: How important the position is to each institution
- **Net Adds**: Number of institutions adding/dropping the position
- **Avg Port Δ%**: Average change in portfolio allocation across all institutions

## 📝 Configuration

### config/analysis_config.json

```json
{
  "quarter": "Q2",
  "year": 2025,
  "paths": {
    "filings_base": "data/13f_filings/sec-edgar-filings",
    "output_base": "output"
  },
  "analysis": {
    "ownership_cap_percent": 101,
    "exclude_etfs": true,
    "min_shares_outstanding": 100000
  }
}
```

### config/clean_institutions.csv

Contains the list of institutions to track. Default includes top 100 by AUM.

## 🔄 Updating Data

To get the latest quarter's data:

1. **Wait for filing deadline**: 45 days after quarter end
2. **Clear old downloads** (optional):
   ```bash
   rm -rf data/13f_filings/sec-edgar-filings/
   rm data/13f_filings/download_progress_v2.json
   ```
3. **Run pipeline**:
   ```bash
   python pipeline/05_analyze_net_adds.py
   ```

## 📊 Data Sources

- **SEC EDGAR**: 13F-HR institutional holdings filings
- **SEC XBRL**: Company shares outstanding data
- **OpenFIGI API**: CUSIP to ticker mapping (free tier)

## ⚠️ Important Notes

1. **SEC Rate Limits**: Be respectful of SEC servers. The pipeline includes delays.
2. **Data Coverage**: Shares outstanding data covers ~40% of securities
3. **Filing Delays**: 13F filings are due 45 days after quarter end
4. **Amendments**: Pipeline excludes amendments (13F-HR/A) to avoid incomplete data

## 🐛 Troubleshooting

### Common Issues

1. **"No filings found"**: Check if 45-day filing deadline has passed
2. **Missing tickers**: Some CUSIPs may not map to tickers (normal)
3. **Slow downloads**: SEC rate limiting - this is normal and expected

### Reset and Retry

```bash
# Clear all cached data
rm -rf data/mappings/*.json
rm -rf output/

# Re-run pipeline
python pipeline/05_analyze_net_adds.py
```

## 📈 Example Use Cases

- **Track Smart Money**: See what top institutions are buying/selling
- **Identify Consensus Trades**: Find securities multiple institutions are accumulating
- **Portfolio Analysis**: Analyze concentration and diversification patterns
- **Sector Rotation**: Identify institutional shifts between sectors

## 📄 License

This project is for educational and research purposes. Ensure compliance with SEC terms of use.

## 🤝 Contributing

Contributions welcome! Please ensure any PR maintains data accuracy and performance.

## 📧 Support

For issues or questions, please open a GitHub issue.

---

Built with Python, Streamlit, and SEC EDGAR data.