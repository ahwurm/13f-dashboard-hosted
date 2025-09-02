# 13F Holdings Analysis Pipeline

#CLAUDE RULES
1. Prioritize token efficiency in every interaction.
2. Real data is the primary product, never do anything to compromise data quality including placeholder or simulated data.
3. Avoid implementing fallbacks, if something fails, let it fail.

## Overview
This project analyzes institutional 13F filings to identify top net additions and total holdings as a percentage of shares outstanding. It processes SEC EDGAR filings from major institutional investors to provide insights into institutional ownership concentration and consensus positions.

## Streamlit Dashboard (`app.py`)

The project includes a comprehensive Streamlit dashboard for exploring the analyzed data:

### Features
- **Interactive Filtering**: Filter by ticker symbol, investor types, specific investors, ownership %, value range, and number of holders
- **Multiple Views**:
  - Overview: Scatter plots showing ownership concentration
  - Top Holdings: Full-width sortable table with ownership percentages
  - Securities Detail: Deep dive into individual stocks with all holders
- **Portfolio Analysis**: View individual institution portfolios and overlap when comparing multiple institutions
- **Real-time Updates**: All filters and selections update visualizations instantly

### Running the Dashboard
```bash
streamlit run app.py
```
Access at: http://localhost:8501


## Configuration

### `config/analysis_config.json`
```json
{
  "quarter": "Q2",
  "year": 2025,
  "paths": {
    "filings_base": "data/13f_filings/sec-edgar-filings",
    "cik_metadata": "data/cik_metadata.json",
    "output_base": "output",
    "data_cache": "data/mappings"
  },
  "analysis": {
    "ownership_cap_percent": 101,
    "exclude_etfs": true,
    "min_shares_outstanding": 100000,
    "max_data_age_days": 1095
  }
}
```

## Data Scaling

### Value Storage
- All monetary values in JSON are stored in **millicents** (0.1 cents or 0.001 dollars)
- 1 millicent = $0.001
- 1,000 millicents = $1
- 1,000,000,000 millicents = $1 million

### Conversion in app.py
- `SCALE_FACTOR = 1,000_000_000` converts millicents directly to millions
- All display values throughout the app are in millions
- The `format_large_number()` helper formats millions as B (billions) or T (trillions) when appropriate
- Example: JSON value of `21,583,762,394,506,000` millicents displays as "$21.58T"

Key packages:
- `sec-edgar-downloader` - SEC filing downloads
- `requests` - API calls
- `pandas` - Data manipulation
- `plotly` - Interactive visualizations
- `tqdm` - Progress bars

## API Requirements

### OpenFIGI API
- Free tier: 25 requests/minute, 10 jobs per request
- Used for CUSIP to ticker mapping
- No API key required for basic usage

### SEC EDGAR
- Requires User-Agent header with name and email
- Configure in `config/analysis_config.json`
- Rate limits: Be respectful of SEC servers


## Future Enhancements

- [ ] Add quarterly comparison (Q3 vs Q2)
- [ ] Track insider transactions
- [ ] Add sector/industry analysis
- [ ] Implement real-time filing monitoring
- [ ] Add email notifications for new filings
- [ ] Improve shares outstanding coverage beyond 28.8%

## Data Sources

- **13F Filings**: SEC EDGAR (quarterly institutional holdings)
- **Shares Outstanding**: SEC XBRL data (companyfacts.zip)
- **CUSIP Mappings**: OpenFIGI API
- **Ticker Mappings**: SEC company_tickers.json

## License

This project is for educational and research purposes. Ensure compliance with:
- SEC EDGAR terms of use
- OpenFIGI API terms
- Data redistribution regulations
