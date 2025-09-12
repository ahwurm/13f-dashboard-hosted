"""
13F Institutional Holdings Dashboard
Professional Streamlit dashboard for analyzing institutional investor holdings
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# All JSON values are stored in millicents (tenths of cents). We display in millions throughout the app.
SCALE_FACTOR = 1_000_000_000  # Divide by this to convert millicents to millions (1 billion millicents = 1 million dollars)
DAYS_PER_QUARTER = 92  # Conservative estimate to avoid false exclusions in age calculations

# Load configuration
@st.cache_data
def load_config():
    """Load analysis configuration"""
    with open('config/analysis_config.json', 'r') as f:
        return json.load(f)

CONFIG = load_config()

def format_large_number(value_in_millions):
    """Format millions to B or T as appropriate."""
    if value_in_millions >= 1_000_000:
        return f"${value_in_millions/1_000_000:.2f}T"
    elif value_in_millions >= 1_000:
        return f"${value_in_millions/1_000:.1f}B"
    else:
        return f"${value_in_millions:.1f}M"

# Page configuration
st.set_page_config(
    page_title="Smart Capital Tracker",
    page_icon="financial-institution-icon.png",
    layout="wide",
    initial_sidebar_state="auto"
)

# Custom CSS for professional styling - optimized for 100% zoom
st.markdown("""
    <style>
    /* Reduce main header size */
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.25rem;
    }
    /* Tighter metric cards */
    .metric-card {
        background-color: #f9fafb;
        padding: 0.5rem;
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
    }
    /* Smaller metric values */
    .metric-value {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1f2937;
    }
    .metric-label {
        font-size: 0.75rem;
        color: #6b7280;
    }
    /* Narrower sidebar */
    div[data-testid="stSidebar"] {
        background-color: #f9fafb;
        max-width: 250px;
    }
    /* Reduce all font sizes */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 4px 12px;
        font-size: 0.9rem;
    }
    /* Tighter metric containers */
    div[data-testid="metric-container"] {
        padding: 0.5rem 0.75rem;
    }
    div[data-testid="metric-container"] label {
        font-size: 0.75rem;
    }
    div[data-testid="metric-container"] div[data-testid="metric-value"] {
        font-size: 1.1rem;
    }
    /* Reduce header sizes */
    h1 {
        font-size: 1.5rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    h2 {
        font-size: 1.25rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    h3 {
        font-size: 1.1rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    /* Tighter spacing in sidebar */
    .css-1544g2n {
        padding: 1rem 0.5rem;
    }
    /* Reduce padding around main content - more space from top ribbon */
    .block-container {
        padding-top: 4rem;
        padding-bottom: 0rem;
    }
    </style>
""", unsafe_allow_html=True)

# Helper function to get available quarters
def get_available_quarters():
    """Get list of available quarters from output directory"""
    output_dir = Path("output")
    if not output_dir.exists():
        return []
    
    quarters = []
    for folder in output_dir.iterdir():
        if folder.is_dir() and "_" in folder.name:
            # Parse folder name like "Q2_2025"
            parts = folder.name.split("_")
            if len(parts) == 2 and parts[0].startswith("Q"):
                quarters.append((parts[0], int(parts[1]), folder.name))
    
    # Sort by year and quarter
    quarters.sort(key=lambda x: (x[1], x[0]), reverse=True)
    return quarters

# Data loading and caching
@st.cache_data(ttl=3600)  # Cache for 1 hour to allow for updates
def load_holdings_data(quarter, year):
    """Load and cache holdings data from JSON file"""
    data_path = Path(f"output/{quarter}_{year}/total_holdings_data.json")
    
    if not data_path.exists():
        st.error(f"Data file not found: {data_path}")
        return None, None
    
    with open(data_path, 'r') as f:
        data = json.load(f)
    
    # Filter out institutions with stale data based on config
    max_age_days = CONFIG.get('analysis', {}).get('max_data_age_days', 365)
    current_quarter = int(quarter[1])
    
    # Get filing periods
    filing_periods = data['metadata'].get('institution_breakdown', {}).get('filing_periods', {})
    
    # Identify institutions to exclude (those with old data)
    stale_institutions = set()
    for inst_name, period in filing_periods.items():
        if period['year'] < year or (period['year'] == year and period['quarter'] < current_quarter):
            # Calculate age in quarters (rough approximation)
            quarters_old = (year - period['year']) * 4 + (current_quarter - period['quarter'])
            days_old = quarters_old * DAYS_PER_QUARTER
            
            if days_old > max_age_days:
                stale_institutions.add(inst_name)
    
    # Filter securities to remove positions from stale institutions
    filtered_securities = []
    for sec in data['securities']:
        if 'positions' in sec and sec['positions']:
            # Remove stale institution positions
            filtered_positions = {k: v for k, v in sec['positions'].items() 
                                if k not in stale_institutions}
            
            # Only keep security if it still has positions after filtering
            if filtered_positions:
                sec = sec.copy()
                sec['positions'] = filtered_positions
                # Recalculate holder count
                sec['holder_count'] = len(filtered_positions)
                filtered_securities.append(sec)
        else:
            # Keep securities without position data (shouldn't happen, but be safe)
            filtered_securities.append(sec)
    
    # Update metadata to reflect filtered counts
    current_institutions = [name for name, p in filing_periods.items() 
                           if p['quarter'] == current_quarter and p['year'] == year 
                           and name not in stale_institutions]
    
    data['metadata']['institution_breakdown']['filtered_total'] = len(filing_periods) - len(stale_institutions)
    data['metadata']['institution_breakdown']['filtered_current_quarter'] = len(current_institutions)
    data['metadata']['institution_breakdown']['excluded_stale'] = len(stale_institutions)
    
    # Convert securities to DataFrame
    df = pd.DataFrame(filtered_securities)
    
    if len(df) == 0:
        return df, data['metadata']
    
    # Convert from storage (millicents) to display (millions)
    df['value_usd'] = df['value_usd'] / SCALE_FACTOR
    
    # Add formatted columns
    df['value_formatted'] = df['value_usd'].apply(lambda x: f"${x:,.0f}")
    df['pct_formatted'] = df['pct_of_shares_outstanding'].apply(lambda x: f"{x:.2f}%")
    
    return df, data['metadata']

@st.cache_data(ttl=3600)  # Cache for 1 hour to allow for updates
def load_quarterly_adds_data(quarter, year):
    """Load and cache quarterly additions data from JSON file"""
    data_path = Path(f"output/{quarter}_{year}/quarterly_adds_data.json")
    
    if not data_path.exists():
        return None, None
    
    with open(data_path, 'r') as f:
        data = json.load(f)
    
    # Convert securities to DataFrame
    df = pd.DataFrame(data['securities'])
    
    if len(df) == 0:
        return df, data.get('metadata', {})
    
    # Convert from storage (millicents) to display (millions)
    df['value_usd'] = df['value_usd'] / SCALE_FACTOR
    
    # Add formatted columns
    df['value_formatted'] = df['value_usd'].apply(lambda x: f"${x:,.0f}")
    df['pct_formatted'] = df['pct_of_shares_outstanding'].apply(lambda x: f"{x:.2f}%")
    
    return df, data.get('metadata', {})

# Investor type consolidation mapping
# Methodology-based investor categorization
INVESTOR_TYPE_MAPPING = {
    # Quantitative/Systematic
    'Quant Fund': 'Quantitative',
    'Quant Value': 'Quantitative',
    
    # Hedge Funds
    'Hedge Fund': 'Hedge Funds',
    'Multi-Strategy': 'Hedge Funds',
    'Tech Hedge Fund': 'Hedge Funds',
    'Event-Driven': 'Hedge Funds',
    'Macro Fund': 'Hedge Funds',
    'Tiger Cub': 'Hedge Funds',
    
    # Value & Growth
    'Value Fund': 'Value & Growth',
    'Growth Fund': 'Value & Growth',
    'Activist Investor': 'Value & Growth',
    'Activist Hedge Fund': 'Value & Growth',
    'Conglomerate': 'Value & Growth',  # Berkshire
    
    # Alternative Assets
    'Private Equity': 'Alternative Assets',
    'Private Credit': 'Alternative Assets',
    'Distressed Debt': 'Alternative Assets',
    'Distressed Fund': 'Alternative Assets',
    'Credit Fund': 'Alternative Assets',
    
    # Banks
    'Bank': 'Banks',
    'Investment Bank': 'Banks',
    
    # Other
    'Pension Fund': 'Other',
    'REIT Fund': 'Other',
    'ESG Fund': 'Traditional Active',
    'Small Cap Fund': 'Traditional Active',
    
    # Asset Manager will be handled specially based on firm name
    'Asset Manager': 'Asset Manager'  # Placeholder, resolved in function
}

def get_methodology_category(firm_name, original_type):
    """Categorize institutions based on investment methodology."""
    # Special handling for Asset Managers based on firm name
    if original_type == 'Asset Manager':
        # Passive/Index firms
        passive_firms = ['BlackRock', 'Vanguard', 'State Street', 'Geode', 
                        'Dimensional', 'Northern Trust', 'BNY Mellon']
        for p in passive_firms:
            if p in firm_name:
                return 'Passive/Index'
        
        # Traditional Active managers
        return 'Traditional Active'
    
    # Use standard mapping for other types
    mapped = INVESTOR_TYPE_MAPPING.get(original_type, 'Other')
    
    # Don't return the placeholder
    if mapped == 'Asset Manager':
        return 'Traditional Active'
    
    return mapped

@st.cache_data
def load_investor_metadata():
    """Load investor metadata with consolidated types"""
    metadata_path = Path("data/cik_metadata.json")
    
    if not metadata_path.exists():
        return {}
    
    with open(metadata_path, 'r') as f:
        data = json.load(f)
    
    # Create investor name to type mapping with methodology-based consolidation
    investor_types = {}
    for cik, details in data['cik_details'].items():
        original_type = details.get('type', 'Unknown')
        firm_name = details['name']
        
        # Apply methodology-based categorization
        consolidated_type = get_methodology_category(firm_name, original_type)
        
        investor_types[firm_name] = {
            'type': consolidated_type,
            'original_type': original_type,
            'investor': details.get('investor', ''),
            'description': details.get('description', '')
        }
    
    return investor_types

@st.cache_data
def calculate_institution_portfolios(_df):
    """Calculate total portfolio value for each institution"""
    # Using _df with underscore prefix to avoid hashing issues with lists
    institution_totals = {}
    
    for _, row in _df.iterrows():
        if 'positions' in row and row['positions']:
            for institution, position in row['positions'].items():
                if institution not in institution_totals:
                    institution_totals[institution] = 0
                # Value is in millicents, convert to millions for consistency
                institution_totals[institution] += position['value'] / SCALE_FACTOR
    
    return institution_totals

def add_portfolio_percentages(df, selected_institution, institution_totals):
    """Add portfolio percentage column for a specific institution"""
    df = df.copy()
    
    if selected_institution and selected_institution in institution_totals:
        total_portfolio = institution_totals[selected_institution]
        
        def get_portfolio_pct(row):
            position_value = get_position_value(row, selected_institution)
            return calculate_portfolio_percentage(position_value, total_portfolio)
        
        df['portfolio_pct'] = df.apply(get_portfolio_pct, axis=1)
        df['portfolio_pct_formatted'] = df['portfolio_pct'].apply(lambda x: f"{x:.2f}%" if x > 0 else "")
        
        # Filter to only show holdings of this institution
        df = df[df['portfolio_pct'] > 0]
    
    return df

def filter_dataframe(df, filters, investor_metadata=None, institution_totals=None):
    """Apply filters to the dataframe"""
    filtered_df = df.copy()
    
    # Check if single institution is selected
    single_institution = None
    if filters['selected_investors'] and len(filters['selected_investors']) == 1:
        single_institution = filters['selected_investors'][0]
    
    # Text search filter - exact ticker match only
    if filters['search']:
        search_term = filters['search'].upper()
        filtered_df = filtered_df[filtered_df['ticker'] == search_term]
    
    # For single institution, filter by portfolio percentage if calculated
    if single_institution and institution_totals and 'portfolio_pct' not in filtered_df.columns:
        filtered_df = add_portfolio_percentages(filtered_df, single_institution, institution_totals)
    
    # For single institution, only show securities they hold
    if single_institution:
        # Filter to only show securities this institution holds
        filtered_df['position_value'] = filtered_df.apply(lambda row: get_position_value(row, single_institution), axis=1)
        # Only show securities this institution holds
        filtered_df = filtered_df[filtered_df['position_value'] > 0]
    
    # Number of holders filter (skip for single institution)
    if not single_institution:
        filtered_df = filtered_df[
            (filtered_df['num_holders'] >= filters['holders_range'][0]) &
            (filtered_df['num_holders'] <= filters['holders_range'][1])
        ]
    
    # Investor filter
    if filters['selected_investors'] and not single_institution:
        filtered_df = filtered_df[
            filtered_df['holders'].apply(
                lambda x: any(inv in x for inv in filters['selected_investors'])
            )
        ]
    
    # Investor type filter - recalculate values to show only filtered type holdings
    if filters['investor_types'] and investor_metadata and not single_institution:
        # First filter to securities that have at least one holder of the selected type
        filtered_df = filtered_df[
            filtered_df['holders'].apply(
                lambda holders: any(
                    investor_metadata.get(h, {}).get('type', 'Unknown') in filters['investor_types']
                    for h in holders
                )
            )
        ].copy()
        
        # Now recalculate values to only include positions from filtered investor types
        if len(filtered_df) > 0:
            # Store original values
            filtered_df['original_value_usd'] = filtered_df['value_usd']
            filtered_df['original_num_holders'] = filtered_df['num_holders']
            
            # Recalculate for each security
            new_values = []
            new_holders = []
            new_shares = []
            filtered_holders_list = []
            
            for idx, row in filtered_df.iterrows():
                type_specific_value = 0
                type_specific_shares = 0
                type_specific_holders = []
                
                if 'positions' in row and row['positions']:
                    for inst_name, position in row['positions'].items():
                        # Check if this institution is of the selected type
                        if investor_metadata.get(inst_name, {}).get('type', 'Unknown') in filters['investor_types']:
                            type_specific_value += position['value'] / SCALE_FACTOR  # Convert millicents to millions
                            type_specific_shares += position['shares']
                            type_specific_holders.append(inst_name)
                
                # Values already converted to millions in the loop above
                new_values.append(type_specific_value)
                new_shares.append(type_specific_shares)
                new_holders.append(len(type_specific_holders))
                filtered_holders_list.append(type_specific_holders)
            
            # Update the dataframe with filtered values (now in millions)
            filtered_df['value_usd'] = new_values
            filtered_df['shares_held'] = new_shares
            filtered_df['num_holders'] = new_holders
            filtered_df['holders'] = filtered_holders_list
            
            # Recalculate ownership percentage based on filtered shares
            filtered_df['pct_of_shares_outstanding'] = (
                filtered_df['shares_held'] / filtered_df['shares_outstanding'] * 100
            ).fillna(0)
            
            # Update formatted columns
            filtered_df['value_formatted'] = filtered_df['value_usd'].apply(lambda x: f"${x:,.0f}")
            filtered_df['pct_formatted'] = filtered_df['pct_of_shares_outstanding'].apply(lambda x: f"{x:.2f}%")
            
            # Remove securities with zero value after filtering
            filtered_df = filtered_df[filtered_df['value_usd'] > 0]
    
    return filtered_df

def create_ownership_scatter(df, top_n=100, single_institution=None, institution_totals=None, filtered_institutions=None):
    """Create scatter plot with portfolio % on x-axis and company ownership on y-axis"""
    if len(df) == 0:
        return None
    
    # For single institution, use portfolio percentage on x-axis and company ownership on y-axis
    if single_institution and 'portfolio_pct' in df.columns:
        plot_df = df.nlargest(min(top_n, len(df)), 'portfolio_pct')
        
        # Calculate institution's % ownership of each company
        plot_df['inst_company_ownership'] = plot_df.apply(
            lambda row: row['positions'][single_institution].get('pct_of_company_shares', 0) 
            if 'positions' in row and row['positions'] and single_institution in row['positions'] else 0,
            axis=1
        )
        
        x_col = 'portfolio_pct'
        y_col = 'inst_company_ownership'
        x_label = '% of Institution Portfolio'
        y_label = '% of Company Owned'
        x_title = '% of Portfolio'
        y_title = '% of Company Owned'
        title = f'{single_institution[:20]}... Portfolio vs Ownership' if len(single_institution) > 20 else f'{single_institution}: Portfolio vs Ownership'
        size_col = 'position_value' if 'position_value' in plot_df.columns else None
    else:
        # Calculate average portfolio % across filtered institutions
        if institution_totals and filtered_institutions:
            institutions_count = len(filtered_institutions)
            def calc_avg_portfolio_pct(row):
                if 'positions' not in row or not row['positions']:
                    return 0
                total_pct = 0
                for inst_name, position in row['positions'].items():
                    # Only include institutions that are in the filtered set
                    if inst_name in filtered_institutions and inst_name in institution_totals:
                        position_value_millions = position['value'] / 1_000_000_000  # Convert to millions
                        portfolio_pct = calculate_portfolio_percentage(position_value_millions, institution_totals[inst_name])
                        total_pct += portfolio_pct
                # Divide by total filtered institutions (not just holders)
                return total_pct / institutions_count if institutions_count > 0 else 0
            
            df = df.copy()
            df['avg_portfolio_pct'] = df.apply(calc_avg_portfolio_pct, axis=1)
            plot_df = df.nlargest(min(top_n, len(df)), 'avg_portfolio_pct')
            x_col = 'avg_portfolio_pct'
        else:
            # Calculate average portfolio % even without explicit institution totals
            # Use all institutions in the data
            all_institutions = set()
            for _, row in df.iterrows():
                if 'positions' in row and row['positions']:
                    all_institutions.update(row['positions'].keys())
            
            institutions_count = len(all_institutions) if all_institutions else 1
            
            def calc_avg_portfolio_pct_fallback(row):
                if 'positions' not in row or not row['positions']:
                    return 0
                # Sum the portfolio percentages (assuming equal weight for simplicity)
                total_value = sum(p['value'] for p in row['positions'].values())
                # This is a rough estimate when we don't have total portfolio values
                return (total_value / 1_000_000_000) / institutions_count * 0.1  # Scale factor
            
            df = df.copy()
            df['avg_portfolio_pct'] = df.apply(calc_avg_portfolio_pct_fallback, axis=1)
            plot_df = df.nlargest(min(top_n, len(df)), 'avg_portfolio_pct')
            x_col = 'avg_portfolio_pct'
        
        y_col = 'pct_of_shares_outstanding'
        x_label = '% of Institution Portfolio' if x_col == 'avg_portfolio_pct' else '% of Total Value'
        y_label = '% of Company Owned'
        x_title = 'Avg Portfolio %'
        y_title = '% Outstanding Held'
        title = f'Top {min(top_n, len(plot_df))} Holdings'
        size_col = 'num_holders'
    
    # Create hover data dynamically
    hover_data = ['ticker', 'name']
    if single_institution and 'portfolio_pct_formatted' in plot_df.columns:
        hover_data.append('portfolio_pct_formatted')
        if 'position_value' in plot_df.columns:
            plot_df['position_value_formatted'] = plot_df['position_value'].apply(lambda x: f"${x:.1f}M" if x < 1000 else format_large_number(x))
            hover_data.append('position_value_formatted')
    else:
        hover_data.extend(['value_formatted', 'pct_formatted'])
    
    fig = px.scatter(
        plot_df,
        x=x_col,
        y=y_col,
        size=size_col,
        hover_data=hover_data,
        text='ticker',
        title=title,
        labels={
            x_col: x_label,
            y_col: y_label,
            'num_holders': 'Number of Holders',
            'position_value': 'Position Value ($)'
        }
    )
    
    fig.update_traces(textposition='top center', textfont_size=7)
    
    # Both axes are percentages now, no log scale needed
    fig.update_layout(
        height=450,
        xaxis_title=x_title,
        yaxis_title=y_title,
        xaxis=dict(tickformat='.2f', ticksuffix='%'),
        yaxis=dict(tickformat='.1f', ticksuffix='%'),
        showlegend=False,
        dragmode=False,
        clickmode='none',
        hovermode=False
    )
    
    return fig

def create_top_holdings_bar(df, top_n=20, single_institution=None, institution_totals=None, filtered_institutions=None):
    """Create bar chart of top holdings"""
    if len(df) == 0:
        return None
    
    # For single institution, use portfolio percentage
    if single_institution and 'portfolio_pct' in df.columns:
        plot_df = df.nlargest(min(top_n, len(df)), 'portfolio_pct')
        x_col = 'portfolio_pct'
        display_inst = single_institution[:20] + '...' if len(single_institution) > 20 else single_institution
        title = f'{display_inst}: Top {min(top_n, len(plot_df))}'
        x_label = 'Portfolio %'
        
        # Color by portfolio percentage for single institution
        color_col = 'portfolio_pct'
        color_title = 'Portfolio %'
    else:
        # Calculate average portfolio % across filtered institutions
        if institution_totals and filtered_institutions:
            institutions_count = len(filtered_institutions)
            def calc_avg_portfolio_pct(row):
                if 'positions' not in row or not row['positions']:
                    return 0
                total_pct = 0
                for inst_name, position in row['positions'].items():
                    # Only include institutions that are in the filtered set
                    if inst_name in filtered_institutions and inst_name in institution_totals:
                        position_value_millions = position['value'] / 1_000_000_000  # Convert to millions
                        portfolio_pct = calculate_portfolio_percentage(position_value_millions, institution_totals[inst_name])
                        total_pct += portfolio_pct
                # Divide by total filtered institutions (not just holders)
                return total_pct / institutions_count if institutions_count > 0 else 0
            
            df = df.copy()
            df['avg_portfolio_pct'] = df.apply(calc_avg_portfolio_pct, axis=1)
            plot_df = df.nlargest(min(top_n, len(df)), 'avg_portfolio_pct')
            x_col = 'avg_portfolio_pct'
            title = f'Top {min(top_n, len(plot_df))} by Portfolio %'
            x_label = 'Average % of Portfolio'
        else:
            # Fallback to original behavior if data not available
            plot_df = df.nlargest(min(top_n, len(df)), 'pct_of_shares_outstanding')
            x_col = 'pct_of_shares_outstanding'
            title = f'Top {min(top_n, len(plot_df))} by Ownership %'
            x_label = 'Ownership %'
        
        color_col = 'num_holders'
        color_title = '# Holders'
    
    hover_data = ['name']
    if single_institution and 'portfolio_pct_formatted' in plot_df.columns:
        hover_data.append('portfolio_pct_formatted')
    else:
        hover_data.extend(['value_formatted', 'num_holders'])
    
    fig = px.bar(
        plot_df,
        x=x_col,
        y='ticker',
        orientation='h',
        hover_data=hover_data,
        title=title,
        labels={x_col: x_label, 'ticker': 'Ticker'},
        color=color_col,
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=500,
        yaxis={'categoryorder': 'total ascending'},
        coloraxis_colorbar_title=color_title,
        dragmode=False,
        clickmode='none',
        hovermode=False
    )
    
    return fig

# Helper functions to reduce duplication
def get_position_value(row, institution, scale=True):
    """Extract position value for an institution from a row."""
    if 'positions' in row and row['positions'] and institution in row['positions']:
        value = row['positions'][institution]['value']
        return value / SCALE_FACTOR if scale else value
    return 0

def calculate_portfolio_percentage(position_value, total_portfolio):
    """Calculate portfolio percentage for a position."""
    return (position_value / total_portfolio * 100) if total_portfolio > 0 else 0

def create_holdings_display_df(holdings_df, single_institution=None):
    """Create display dataframe for holdings table."""
    if len(holdings_df) == 0:
        return pd.DataFrame()
    
    if single_institution and 'portfolio_pct' in holdings_df.columns:
        return pd.DataFrame({
            'Ticker': holdings_df['ticker'],
            'Company': holdings_df['name'].apply(lambda x: x[:30] + '...' if len(x) > 30 else x),
            'Portfolio %': holdings_df['portfolio_pct'],
            'Position Value ($M)': holdings_df['position_value'] if 'position_value' in holdings_df.columns else holdings_df['value_usd'],
            'Shares': holdings_df.apply(
                lambda x: x['positions'][single_institution]['shares'] if single_institution in x.get('positions', {}) else 0, 
                axis=1
            ),
            '% of Company': holdings_df.apply(
                lambda x: x['positions'][single_institution].get('pct_of_company_shares', 0) if single_institution in x.get('positions', {}) else 0,
                axis=1
            )
        })
    else:
        return pd.DataFrame({
            'Ticker': holdings_df['ticker'],
            'Company': holdings_df['name'].apply(lambda x: x[:30] + '...' if len(x) > 30 else x),
            'Ownership %': holdings_df['pct_of_shares_outstanding'],
            'Value ($M)': holdings_df['value_usd'],
            '# Holders': holdings_df['num_holders'],
            'Net Adds': holdings_df['net_adds'] if 'net_adds' in holdings_df.columns else 0
        })

def create_table_column_config(single_institution=False):
    """Create column configuration for dataframe display."""
    if single_institution:
        return {
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Company": st.column_config.TextColumn("Company", width="medium"),
            "Portfolio %": st.column_config.NumberColumn(
                "Portfolio %",
                format="%.2f%%",
                width="small"
            ),
            "Position Value ($M)": st.column_config.NumberColumn(
                "Value",
                format="$%.1f M",
                width="small"
            ),
            "Shares": st.column_config.NumberColumn(
                "Shares",
                format="%,d",
                width="small"
            ),
            "% of Company": st.column_config.NumberColumn(
                "% of Co.",
                format="%.2f%%",
                width="small",
                help="Institution's ownership % of this company"
            )
        }
    else:
        return {
            "Ticker": st.column_config.TextColumn("Ticker", width="small"),
            "Company": st.column_config.TextColumn("Company", width="medium"),
            "Ownership %": st.column_config.NumberColumn(
                "Own %",
                format="%.2f%%",
                width="small"
            ),
            "Value ($M)": st.column_config.NumberColumn(
                "Value",
                format="$%.1f M",
                width="small"
            ),
            "# Holders": st.column_config.NumberColumn(
                "Holders",
                format="%d",
                width="small"
            ),
            "Net Adds": st.column_config.NumberColumn(
                "Net Adds",
                format="%+d",
                width="small",
                help="New institutions this quarter"
            )
        }

def render_overview_tab(filtered_df, single_institution, institution_totals, filtered_institutions):
    """Render the Overview tab with scatter and bar charts."""
    col1, col2 = st.columns(2)
    
    with col1:
        # Ownership scatter plot
        if len(filtered_df) > 0:
            fig_scatter = create_ownership_scatter(
                filtered_df, 
                single_institution=single_institution,
                institution_totals=institution_totals,
                filtered_institutions=filtered_institutions
            )
            if fig_scatter:
                st.plotly_chart(fig_scatter, use_container_width=True, config={'displayModeBar': False, 'staticPlot': True})
            else:
                st.info("No data available for scatter plot")
        else:
            st.info("No securities match current filters")
    
    with col2:
        # Top holdings bar chart
        if len(filtered_df) > 0:
            fig_bar = create_top_holdings_bar(
                filtered_df, 
                single_institution=single_institution,
                institution_totals=institution_totals,
                filtered_institutions=filtered_institutions
            )
            if fig_bar:
                st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False, 'staticPlot': True})
            else:
                st.info("No data available for bar chart")
        else:
            st.info("No securities match current filters")

def render_top_holdings_tab(filtered_df, single_institution, institution_totals, quarter, year, filtered_institutions):
    """Render the Top Holdings tab with sortable table."""
    if len(filtered_df) == 0:
        st.info("No holdings match current filters")
        return
    
    # Calculate average portfolio allocation for non-single institution view
    if not single_institution:
        institutions_count = len(filtered_institutions)
        def calc_avg_portfolio_pct(row):
            if 'positions' not in row or not row['positions']:
                return 0
            total_pct = 0
            for inst_name, position in row['positions'].items():
                # Only include institutions that are in the filtered set
                if inst_name in filtered_institutions and inst_name in institution_totals:
                    position_value_millions = position['value'] / 1_000_000_000  # Convert to millions
                    portfolio_pct = calculate_portfolio_percentage(position_value_millions, institution_totals[inst_name])
                    total_pct += portfolio_pct
            # Divide by total filtered institutions (not just holders)
            return total_pct / institutions_count if institutions_count > 0 else 0
        
        filtered_df = filtered_df.copy()
        filtered_df['avg_portfolio_pct'] = filtered_df.apply(calc_avg_portfolio_pct, axis=1)
        
        # Sort by average portfolio allocation
        top_holdings = filtered_df.nlargest(50, 'avg_portfolio_pct')
        
        # Create display dataframe
        display_df = pd.DataFrame({
            'Ticker': top_holdings['ticker'],
            'Company': top_holdings['name'].apply(lambda x: x[:25] + '...' if len(x) > 25 else x),
            'Avg Portfolio %': top_holdings['avg_portfolio_pct'],
            'Ownership %': top_holdings['pct_of_shares_outstanding'],
            'Value ($M)': top_holdings['value_usd'],
            '# Holders': top_holdings['num_holders']
        })
    else:
        # Single institution view - sort by portfolio percentage
        top_holdings = filtered_df.nlargest(50, 'portfolio_pct')
        display_df = create_holdings_display_df(top_holdings, single_institution)
    
    # Display table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=450
    )
    
    st.markdown("---")
    
    # Download button
    csv = top_holdings.to_csv(index=False)
    st.download_button(
        label="📥 Download Top Holdings CSV",
        data=csv,
        file_name=f"top_holdings_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# Main app
def main():
    # Get available quarters
    available_quarters = get_available_quarters()
    
    if not available_quarters:
        st.error("No data found in output directory. Please run the pipeline first.")
        return
    
    # Sidebar quarter selector
    st.sidebar.header("📅 Period")
    quarter_options = [f"{q[0]} {q[1]}" for q in available_quarters]
    selected_quarter_idx = st.sidebar.selectbox(
        "Quarter",
        range(len(quarter_options)),
        format_func=lambda x: quarter_options[x]
    )
    
    selected_quarter = available_quarters[selected_quarter_idx]
    quarter = selected_quarter[0]
    year = selected_quarter[1]
    
    # Load data for selected quarter
    df, metadata = load_holdings_data(quarter, year)
    df_adds, metadata_adds = load_quarterly_adds_data(quarter, year)
    investor_metadata = load_investor_metadata()
    
    if df is None:
        st.error(f"Unable to load data for {quarter} {year}. Please ensure the data files are in the correct location.")
        return
    
    # Merge net_adds data with main dataframe if available
    if df_adds is not None and len(df_adds) > 0:
        # Create a mapping of CUSIP to net_adds
        net_adds_map = df_adds.set_index('cusip')['net_adds'].to_dict()
        df['net_adds'] = df['cusip'].map(net_adds_map).fillna(0).astype(int)
    else:
        df['net_adds'] = 0
    
    # Calculate institution portfolios
    institution_totals = calculate_institution_portfolios(df)
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Search box
    search_term = st.sidebar.text_input("Ticker", "")
    
    # Get unique values for filters
    all_investors = set()
    for holders in df['holders']:
        all_investors.update(holders)
    all_investors = sorted(list(all_investors))
    
    # Get unique consolidated investor types
    investor_types = set()
    for inv in all_investors:
        if inv in investor_metadata:
            investor_types.add(investor_metadata[inv]['type'])
    investor_types = sorted(list(investor_types))
    
    # Filter controls
    selected_investor_types = st.sidebar.multiselect(
        "Types",
        options=investor_types,
        default=[]
    )
    
    selected_investors = st.sidebar.multiselect(
        "Investors",
        options=all_investors,
        default=[]
    )
    
    holders_range = st.sidebar.slider(
        "# Holders",
        min_value=int(df['num_holders'].min()),
        max_value=int(df['num_holders'].max()),
        value=(int(df['num_holders'].min()), int(df['num_holders'].max())),
        step=1
    )
    
    # Apply filters
    filters = {
        'search': search_term,
        'investor_types': selected_investor_types,
        'selected_investors': selected_investors,
        'holders_range': holders_range
    }
    
    # Data Coverage calculations - must be before metrics row and filter
    # Use the total from metadata (69 institutions that we actually have data for)
    total_expected_institutions = metadata.get('institution_breakdown', {}).get('total_institutions', 69)
    
    # Get actual values from metadata (after filtering)
    institutions_filed = metadata.get('institution_breakdown', {}).get('filtered_total', 
                                     metadata.get('institution_breakdown', {}).get('total_institutions', 0))
    current_quarter_filed = metadata.get('institution_breakdown', {}).get('filtered_current_quarter',
                                        metadata.get('institution_breakdown', {}).get('current_quarter_institutions', 0))
    excluded_stale = metadata.get('institution_breakdown', {}).get('excluded_stale', 0)
    
    unique_holders_in_data = len(all_investors)  # Unique holders across analyzed securities
    coverage_pct = (institutions_filed / total_expected_institutions) * 100 if total_expected_institutions > 0 else 0
    
    # Get pipeline metrics from actual data
    pipeline_metrics = {
        'filings_processed': institutions_filed,
        'total_filings': total_expected_institutions,
        'cusips_extracted': metadata.get('total_securities', 0),
        'securities_with_shares': len(df),
        'parse_success_rate': (institutions_filed / total_expected_institutions * 100) if total_expected_institutions > 0 else 0
    }
    
    # Check if single institution is selected
    single_institution = None
    if selected_investors and len(selected_investors) == 1:
        single_institution = selected_investors[0]
    
    # Apply filters with investor_metadata and institution totals
    filtered_df = filter_dataframe(df, filters, investor_metadata, institution_totals)
    
    # Calculate filtered institutions based on current filters
    filtered_institutions = set()
    if filters['selected_investors']:
        # If specific investors are selected, use only those
        filtered_institutions = set(filters['selected_investors'])
    elif filters['investor_types']:
        # If investor types are selected, get all institutions of those types
        for inst_name, inst_data in investor_metadata.items():
            if inst_data.get('type', 'Unknown') in filters['investor_types']:
                filtered_institutions.add(inst_name)
    else:
        # Otherwise use all institutions that have filed
        filtered_institutions = set(institution_totals.keys())
    
    # Filter to only institutions that have actually filed (are in institution_totals)
    filtered_institutions = filtered_institutions.intersection(set(institution_totals.keys()))
    
    # Check if user is searching for a specific ticker
    has_ticker_search = bool(search_term)
    
    # Metrics row - adjust for ticker search, single institution view, or general view
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if has_ticker_search and len(filtered_df) > 0:
            # Show security-specific metric
            security_data = filtered_df.iloc[0]
            st.metric("Inst. Own", f"{security_data['pct_of_shares_outstanding']:.1f}%")
        elif single_institution:
            display_inst = single_institution[:15] + '...' if len(single_institution) > 15 else single_institution
            st.metric(f"{display_inst} Holdings", f"{len(filtered_df):,}")
        else:
            st.metric("Securities", f"{len(filtered_df):,}")
    
    with col2:
        if has_ticker_search and len(filtered_df) > 0:
            # Show security-specific metric
            security_data = filtered_df.iloc[0]
            st.metric("Value", format_large_number(security_data['value_usd']))
        elif single_institution and single_institution in institution_totals:
            # Show filtered portfolio value for single institution
            if 'position_value' in filtered_df.columns:
                filtered_value = filtered_df['position_value'].sum()
                st.metric("Value", format_large_number(filtered_value))
            else:
                st.metric("Portfolio", format_large_number(institution_totals[single_institution]))
        else:
            total_value = filtered_df['value_usd'].sum()
            st.metric("Value", format_large_number(total_value))
    
    with col3:
        if has_ticker_search and len(filtered_df) > 0:
            # Show security-specific metric
            security_data = filtered_df.iloc[0]
            st.metric("# Holders", security_data['num_holders'])
        elif single_institution and 'portfolio_pct' in filtered_df.columns:
            # Show top 5 concentration from filtered results
            if len(filtered_df) > 0:
                top_5_pct = filtered_df.nlargest(min(5, len(filtered_df)), 'portfolio_pct')['portfolio_pct'].sum()
                st.metric("Top 5", f"{top_5_pct:.1f}%")
            else:
                st.metric("Top 5 Concentration", "0.0%")
        elif selected_investors and len(selected_investors) > 1:
            # Calculate portfolio overlap for multiple selected institutions
            if len(filtered_df) > 0:
                # Find securities held by ALL selected institutions
                securities_held_by_all = 0
                total_unique_securities = 0
                
                for _, row in filtered_df.iterrows():
                    if 'positions' in row and row['positions']:
                        # Check if all selected institutions hold this security
                        holders = set(row['positions'].keys())
                        if all(inst in holders for inst in selected_investors):
                            securities_held_by_all += 1
                        # Count if any selected institution holds it
                        if any(inst in holders for inst in selected_investors):
                            total_unique_securities += 1
                
                if total_unique_securities > 0:
                    overlap_pct = (securities_held_by_all / total_unique_securities) * 100
                    st.metric("Overlap", f"{overlap_pct:.1f}%")
                else:
                    st.metric("Portfolio Overlap", "0.0%")
            else:
                st.metric("Portfolio Overlap", "0.0%")
        else:
            # Show value-weighted consensus % - % of capital in securities held by >50% of institutions
            if len(filtered_df) > 0:
                # Determine active institution count based on filters
                if filters['selected_investors']:
                    active_institutions = len(filters['selected_investors'])
                elif filters['investor_types']:
                    # Count institutions matching selected types from filtered data
                    # Get all unique investors from the filtered dataframe
                    all_investors_in_data = set()
                    for holders in filtered_df['holders']:
                        all_investors_in_data.update(holders)
                    
                    active_institutions = len([inv for inv in all_investors_in_data 
                                              if investor_metadata.get(inv, {}).get('type', 'Unknown') in filters['investor_types']])
                else:
                    # Use institutions that filed this quarter
                    active_institutions = institutions_filed
                
                # Calculate consensus threshold (>50% of active institutions)
                consensus_threshold = active_institutions * 0.5
                
                # Calculate value in consensus positions (held by >50% of active institutions)
                consensus_mask = filtered_df['num_holders'] > consensus_threshold
                consensus_value = filtered_df[consensus_mask]['value_usd'].sum()
                total_value = filtered_df['value_usd'].sum()
                
                # Calculate percentage of value in consensus positions
                value_consensus_pct = (consensus_value / total_value) * 100 if total_value > 0 else 0
                
                # Format values for tooltip  
                consensus_value_str = format_large_number(consensus_value)
                total_value_str = format_large_number(total_value)
                
                st.metric("Consensus", f"{value_consensus_pct:.1f}%")
            else:
                st.metric("Capital Consensus", "0.0%")
    
    with col4:
        if has_ticker_search and len(filtered_df) > 0:
            # Show security-specific metric
            security_data = filtered_df.iloc[0]
            net_adds_val = security_data.get('net_adds', 0)
            st.metric("Net Adds", f"{net_adds_val:+d}" if net_adds_val != 0 else "0")
        elif filters['investor_types']:
            # Show filtered institutions count when type filter is applied
            st.metric("Inst.", f"{len(filtered_institutions)}")
        elif filters['selected_investors']:
            # Show selected institutions count when specific investors are selected
            st.metric("Selected", f"{len(filtered_institutions)}")
        else:
            st.metric("Filed", f"{institutions_filed}/{total_expected_institutions}")
    
    with col5:
        if has_ticker_search and len(filtered_df) > 0:
            # Show security-specific metric
            security_data = filtered_df.iloc[0]
            st.metric("Shares", 
                     f"{security_data['shares_held']/1e6:.0f}M/{security_data['shares_outstanding']/1e6:.0f}M")
        else:
            st.metric("Date", metadata.get('generated', 'Unknown')[:10])
    
    # Conditional display: Show Securities Detail when searching, otherwise show 3 tabs
    if has_ticker_search:
        # Direct Securities Detail view when searching for a ticker
        st.markdown("---")
        # Auto-display first security from filtered data
        if len(filtered_df) > 0:
            # Get the first security that matches filters
            security_data = filtered_df.iloc[0]
            
            # Truncate name if too long
            display_name = security_data['name'][:25] + '...' if len(security_data['name']) > 25 else security_data['name']
            st.subheader(f"📊 {security_data['ticker']} - {display_name}")
            
            # If single institution selected, highlight their position
            if single_institution and 'positions' in security_data and single_institution in security_data['positions']:
                inst_position = security_data['positions'][single_institution]
                display_inst = single_institution[:20] + '...' if len(single_institution) > 20 else single_institution
                st.info(f"**{display_inst}**: {inst_position['shares']:,} shares | {format_large_number(inst_position['value'] / SCALE_FACTOR)} | {inst_position.get('pct_of_company_shares', 0):.1f}%")
                if 'portfolio_pct' in security_data:
                    st.caption(f"{security_data['portfolio_pct']:.1f}% of portfolio")
            
            # If single institution, show comparison context
            if single_institution and single_institution in security_data.get('positions', {}):
                display_inst = single_institution[:20] + '...' if len(single_institution) > 20 else single_institution
                st.caption(f"vs other holders")
            
            # Get positions data if available
            positions = security_data.get('positions', {})
            
            # Get list of new holders and institution-level changes from quarterly adds data
            new_holders = []
            institution_changes = {}
            if df_adds is not None and len(df_adds) > 0:
                # Find this security in quarterly adds data
                security_adds = df_adds[df_adds['cusip'] == security_data['cusip']]
                if len(security_adds) > 0:
                    adds_data = security_adds.iloc[0]
                    new_holders = adds_data.get('new_holders', [])
                    institution_changes = adds_data.get('institution_changes', {})
            
            if positions:
                # Use real positions data
                holders_data = []
                for holder, position in positions.items():
                    # Get the quarter-over-quarter change for this institution
                    inst_change = institution_changes.get(holder, {})
                    shares_change = inst_change.get('shares_change', 0)
                    
                    # Calculate portfolio impact of the change
                    portfolio_impact = 0
                    if shares_change != 0 and holder in institution_totals:
                        # Calculate the value of the change (using current price)
                        if position['shares'] > 0:
                            price_per_share = position['value'] / position['shares']
                            change_value_millions = (shares_change * price_per_share) / SCALE_FACTOR
                            portfolio_impact = change_value_millions / institution_totals[holder] * 100
                    
                    row_data = {
                        'Institution': (holder[:20] + '...' if len(holder) > 20 else holder) + (" 🆕" if holder in new_holders else ""),
                        '% of Company': position.get('pct_of_company_shares', 0),  # Percentage of company's total shares
                        'Q/Q %': portfolio_impact,  # Raw value for sorting
                        'portfolio_impact_raw': portfolio_impact  # For sorting
                    }
                    # Add portfolio % for each institution
                    if holder in institution_totals:
                        position_value_millions = position['value'] / SCALE_FACTOR
                        row_data['% of Portfolio'] = calculate_portfolio_percentage(position_value_millions, institution_totals[holder])
                    holders_data.append(row_data)
                
                # Sort by portfolio impact (largest changes first)
                holders_df = pd.DataFrame(holders_data)
                holders_df = holders_df.sort_values('portfolio_impact_raw', ascending=False, key=abs)
                
                # Highlight the selected institution if present
                if single_institution and single_institution in holders_df['Institution'].values:
                    # Move selected institution to top
                    selected_row = holders_df[holders_df['Institution'] == single_institution]
                    other_rows = holders_df[holders_df['Institution'] != single_institution]
                    holders_df = pd.concat([selected_row, other_rows], ignore_index=True)
                
                # Format the display
                holders_df['% of Company'] = holders_df['% of Company'].apply(lambda x: f"{x:.2f}%")
                if '% of Portfolio' in holders_df.columns:
                    holders_df['% of Portfolio'] = holders_df['% of Portfolio'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                
                # Format Q/Q columns
                holders_df['Q/Q %'] = holders_df['Q/Q %'].apply(
                    lambda x: f"+{x:.2f}%" if x > 0 else f"{x:.2f}%" if x != 0 else "-"
                )
                
                # Remove raw sorting columns from display
                holders_df = holders_df.drop(columns=['portfolio_impact_raw'])
            else:
                # Fallback if no positions data (shouldn't happen with new pipeline)
                st.warning("Individual position data not available. Please re-run the pipeline to generate position-level data.")
                holders_df = pd.DataFrame()
            
            # Display chart and table side by side
            if positions:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Create a bar chart of holders by % of their portfolio with Q/Q changes
                    holders_with_pct = []
                    for holder, position in positions.items():
                        if holder in institution_totals:
                            position_value_millions = position['value'] / SCALE_FACTOR
                            portfolio_pct = calculate_portfolio_percentage(position_value_millions, institution_totals[holder])
                            # Get Q/Q change
                            inst_change = institution_changes.get(holder, {})
                            shares_change = inst_change.get('shares_change', 0)
                            prev_shares = inst_change.get('prev_shares', 0)
                            current_shares = position['shares']
                            
                            # Calculate the portfolio % for previous position
                            if current_shares > 0 and shares_change != 0:
                                # Estimate previous value (assuming same price)
                                price_per_share = position['value'] / current_shares
                                prev_value_millions = (prev_shares * price_per_share) / SCALE_FACTOR
                                prev_portfolio_pct = calculate_portfolio_percentage(prev_value_millions, institution_totals[holder])
                            else:
                                prev_portfolio_pct = portfolio_pct if shares_change == 0 else 0
                            
                            holders_with_pct.append((holder, portfolio_pct, prev_portfolio_pct, shares_change))
                    
                    # Sort by portfolio percentage
                    sorted_holders = sorted(holders_with_pct, key=lambda x: x[1], reverse=True)[:20]
                    
                    if sorted_holders:
                        holder_names = [h[0] for h in sorted_holders][::-1]  # Reverse to show largest at top
                        
                        # Create stacked bar data
                        base_values = []  # Gray base (previous or unchanged)
                        added_values = []  # Green additions
                        reduced_values = []  # Red reductions (shown as negative for proper stacking)
                        
                        for holder, current_pct, prev_pct, shares_change in sorted_holders[::-1]:
                            if shares_change > 0:
                                # Addition: gray base is previous, green is the increase
                                base_values.append(prev_pct)
                                added_values.append(current_pct - prev_pct)
                                reduced_values.append(0)
                            elif shares_change < 0:
                                # Reduction: gray base is current, red shows what was removed
                                base_values.append(current_pct)
                                added_values.append(0)
                                reduced_values.append(prev_pct - current_pct)
                            else:
                                # No change: all gray
                                base_values.append(current_pct)
                                added_values.append(0)
                                reduced_values.append(0)
                        
                        fig_holders = go.Figure()
                        
                        # Base bar (gray - current holdings after reductions or previous before additions)
                        fig_holders.add_trace(go.Bar(
                            name='Current Holdings',
                            y=holder_names,
                            x=base_values,
                            orientation='h',
                            marker_color='#e0e0e0',
                            showlegend=False
                        ))
                        
                        # Added portion (green)
                        fig_holders.add_trace(go.Bar(
                            name='Added',
                            y=holder_names,
                            x=added_values,
                            orientation='h',
                            marker_color='#28a745',
                            showlegend=False
                        ))
                        
                        # Reduced portion (red - shown to the right of base)
                        fig_holders.add_trace(go.Bar(
                            name='Reduced',
                            y=holder_names,
                            x=reduced_values,
                            orientation='h',
                            marker_color='#dc3545',
                            showlegend=False
                        ))
                        
                        fig_holders.update_layout(
                            barmode='stack',
                            height=400,
                            margin=dict(t=20, b=0, l=0, r=0),
                            xaxis_title='Portfolio %',
                            yaxis_title='',
                            title=dict(
                                text='<span style="color:#28a745">■</span> Added <span style="color:#dc3545">■</span> Reduced <span style="color:#e0e0e0">■</span> Base',
                                font=dict(size=12),
                                x=0.5,
                                xanchor='center'
                            ),
                            showlegend=False,
                            dragmode=False,
                            clickmode='none',
                            hovermode=False
                        )
                        fig_holders.update_xaxes(tickformat='.1f', ticksuffix='%')
                        st.plotly_chart(fig_holders, use_container_width=True, config={'displayModeBar': False, 'staticPlot': True})
                
                with col2:
                    # Display the holders table
                    st.dataframe(
                        holders_df,
                        use_container_width=True,
                        height=400,
                        hide_index=True
                    )
            else:
                # If no positions data, just show the table
                st.dataframe(
                    holders_df,
                    use_container_width=True,
                    height=350,
                    hide_index=True
                )
        else:
            st.info(f"No data found for ticker: {search_term.upper()}")
    else:
        # Normal 3-tab view when not searching
        tab1, tab2, tab3 = st.tabs([
            "📈 Overview", 
            "🏆 Holdings", 
            "🆕 Changes"
        ])
        
        with tab1:
            render_overview_tab(filtered_df, single_institution, institution_totals, filtered_institutions)
        
        with tab2:
            render_top_holdings_tab(filtered_df, single_institution, institution_totals, quarter, year, filtered_institutions)
        
        with tab3:
            if df_adds is not None and len(df_adds) > 0:
                # Calculate net portfolio % impact for each security
                # This is the average portfolio % allocation across filtered institutions
                institutions_filed_count = len(filtered_institutions) if filtered_institutions else current_quarter_filed
            
                def calculate_net_portfolio_change(row):
                    """Calculate the NET CHANGE in average portfolio % across all institutions"""
                    if 'institution_changes' not in row or not row['institution_changes']:
                        # If no changes data, fall back to simple calculation
                        return 0
                    
                    total_portfolio_change = 0
                    
                    # For each filtered institution, calculate their portfolio % change
                    for inst_name in filtered_institutions:
                        if inst_name not in institution_totals:
                            continue
                            
                        inst_change = row['institution_changes'].get(inst_name, {})
                        
                        if inst_change:
                            # Calculate portfolio % change for this institution
                            shares_change = inst_change.get('shares_change', 0)
                            current_shares = inst_change.get('current_shares', 0)
                            
                            if current_shares > 0 and shares_change != 0:
                                # Estimate the portfolio impact of this change
                                # Using current price as proxy
                                if 'positions' in row and inst_name in row['positions']:
                                    position = row['positions'][inst_name]
                                    price_per_share = position['value'] / position['shares']
                                    change_value_millions = (shares_change * price_per_share) / SCALE_FACTOR
                                    portfolio_change = change_value_millions / institution_totals[inst_name] * 100
                                    total_portfolio_change += portfolio_change
                        elif 'positions' in row and inst_name in row['positions']:
                            # Institution holds but didn't change - no contribution to net change
                            pass
                    
                    # Average across all filtered institutions
                    return total_portfolio_change / institutions_filed_count if institutions_filed_count > 0 else 0
            
                df_adds['net_portfolio_change'] = df_adds.apply(calculate_net_portfolio_change, axis=1)
            
                # Apply filters to quarterly adds data
                filtered_adds = filter_dataframe(df_adds, filters, investor_metadata, institution_totals)
            
                if len(filtered_adds) > 0:
                    # Split into additions and drops based on portfolio impact
                    # Sort by net portfolio change to show biggest consensus moves
                    additions_df = filtered_adds[filtered_adds['net_portfolio_change'] > 0].sort_values(by='net_portfolio_change', ascending=False)
                    drops_df = filtered_adds[filtered_adds['net_portfolio_change'] < 0].sort_values(by='net_portfolio_change', ascending=True)
                    
                    # Create two columns for side-by-side display
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"**🟢 Increases** ({len(additions_df)} securities)")
                        
                        if len(additions_df) > 0:
                            # Get top 50 additions by portfolio impact
                            top_additions = additions_df.head(50)
                        
                            # Create display dataframe for additions
                            display_adds = pd.DataFrame({
                                'Ticker': top_additions['ticker'],
                                'Company': top_additions['name'].apply(lambda x: x[:15] + '...' if len(x) > 15 else x),
                                'Avg Port Δ%': top_additions['net_portfolio_change'],
                                'Value ($M)': top_additions['value_usd'],
                                'Own %': top_additions['pct_of_shares_outstanding']
                            })
                        
                            # Display table
                            column_config_adds = {
                                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                                "Company": st.column_config.TextColumn("Company", width="medium"),
                                "Avg Port Δ%": st.column_config.NumberColumn(
                                    "Port Δ%",
                                    format="%+.4f%%",
                                    width="small",
                                    help="Average portfolio % change across all institutions"
                                ),
                                "Value ($M)": st.column_config.NumberColumn(
                                    "Value",
                                    format="$%.1f M",
                                    width="small"
                                ),
                                "Own %": st.column_config.NumberColumn(
                                    "Own %",
                                    format="%.2f%%",
                                    width="small"
                                )
                            }
                        
                            st.dataframe(
                                display_adds,
                                column_config=column_config_adds,
                                use_container_width=True,
                                hide_index=True,
                                height=400
                            )
                        else:
                            st.info("No securities with institutional additions")
                    
                    with col2:
                        st.markdown(f"**🔴 Decreases** ({len(drops_df)} securities)")
                        
                        if len(drops_df) > 0:
                            # Get top 50 drops by portfolio impact
                            top_drops = drops_df.head(50)
                        
                            # Create display dataframe for drops
                            display_drops = pd.DataFrame({
                                'Ticker': top_drops['ticker'],
                                'Company': top_drops['name'].apply(lambda x: x[:15] + '...' if len(x) > 15 else x),
                                'Avg Port Δ%': top_drops['net_portfolio_change'],
                                'Value ($M)': top_drops['value_usd'],
                                'Own %': top_drops['pct_of_shares_outstanding']
                            })
                        
                            # Display table
                            column_config_drops = {
                                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                                "Company": st.column_config.TextColumn("Company", width="medium"),
                                "Avg Port Δ%": st.column_config.NumberColumn(
                                    "Port Δ%",
                                    format="%.4f%%",
                                    width="small",
                                    help="Average portfolio % change across all institutions"
                                ),
                                "Value ($M)": st.column_config.NumberColumn(
                                    "Value",
                                    format="$%.1f M",
                                    width="small"
                                ),
                                "Own %": st.column_config.NumberColumn(
                                    "Own %",
                                    format="%.2f%%",
                                    width="small"
                                )
                            }
                        
                            st.dataframe(
                                display_drops,
                                column_config=column_config_drops,
                                use_container_width=True,
                                hide_index=True,
                                height=400
                            )
                        else:
                            st.info("No securities with institutional drops")
                else:
                    st.info("No institutional positions match the current filters.")
            else:
                st.info(f"Quarterly additions data not available for {quarter} {year}. This may be because it's the first quarter analyzed or the data hasn't been generated yet.")

if __name__ == "__main__":
    main()