"""
Streamlit app for Indian Stock Indices Analysis
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from modules.historical_fetcher import (
    get_indices_data_from_db, prepare_combined_dataframe, 
    sync_database, verify_download_status, INDICES, get_data_date_range_info
)
from modules.analysis import IndicesAnalyzer
from modules.report_generator import PDFReportGenerator
import os
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load app configuration
def load_app_config():
    """Load app configuration from config.json"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except:
        return {
            'app_title': 'Indian Stock Indices Analysis',
            'app_description': 'Real-time analysis of major Indian stock market indices (Last 26 Weeks / 6 Months)',
            'analysis_period_weeks': 26
        }

APP_CONFIG = load_app_config()

# Page configuration
st.set_page_config(
    page_title=APP_CONFIG.get('app_title', 'Indian Stock Indices Analysis'),
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        padding: 0rem 0rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.title(f"📈 {APP_CONFIG.get('app_title', 'Indian Stock Indices Analysis')}")
st.markdown(f"**{APP_CONFIG.get('app_description', 'Real-time analysis of major Indian stock market indices')}**")

# Display data collection date range
date_range = get_data_date_range_info()
if date_range['start_date'] and date_range['end_date']:
    st.info(f"📅 **Data Collection Period:** {date_range['start_date']} to {date_range['end_date']} (Last 6 Months)")
else:
    st.warning("⚠️ No data available. Click '🔄 Refresh Data' in the sidebar to download historical data.")

# Sidebar for controls
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Period selection
    period_selection = st.radio(
        "Select Analysis Period:",
        ("26 Weeks (6 Months)", "13 Weeks (3 Months)", "52 Weeks (1 Year)"),
        index=0
    )
    
    period_map = {
        "26 Weeks (6 Months)": 26,
        "13 Weeks (3 Months)": 13,
        "52 Weeks (1 Year)": 52
    }
    selected_weeks = period_map[period_selection]
    
    # Refresh button
    refresh_data = st.button("🔄 Refresh Data", use_container_width=True)
    
    st.divider()
    
    # Available indices
    st.subheader("Available Indices")
    if INDICES:
        for idx_name in INDICES.keys():
            st.write(f"• {idx_name}")
    else:
        st.warning("⚠️ No indices configured. Check config.json")
    
    st.divider()
    
    # Database status with download verification
    st.subheader("📊 Database Status")
    status = verify_download_status()
    
    if status:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Downloaded", status.get('total_downloaded', 0))
        with col2:
            st.metric("Missing", len(status.get('missing', [])))
        
        st.caption("✅ **Downloaded Indices:**")
        if status.get('downloaded'):
            for idx in status['downloaded']:
                st.text(f"  • {idx}")
        else:
            st.text("  None yet")
        
        if status.get('missing'):
            st.warning(f"⚠️ **Missing ({len(status['missing'])} indices):**")
            for idx in status['missing']:
                st.text(f"  • {idx}")
            st.info("Click 'Refresh Data' to attempt download of missing indices.")
    else:
        st.warning("No data in database yet. Click 'Refresh Data' to download.")

# Main content area
if refresh_data or 'indices_data' not in st.session_state:
    with st.spinner("📡 Syncing database and fetching indices data..."):
        # Sync database with latest data
        sync_database(force_refresh=refresh_data)
        
        # Retrieve data from local database
        st.session_state.indices_data = get_indices_data_from_db(period_days=180)
        st.session_state.last_refresh = datetime.now()
    
    if st.session_state.indices_data:
        st.success("✅ Data synced and retrieved successfully!")
    else:
        st.warning("⚠️ No data retrieved from database. Ensure data is downloaded.")

# Check if data is available
if st.session_state.get('indices_data'):
    indices_data = st.session_state.indices_data
    
    # Initialize analyzer
    analyzer = IndicesAnalyzer(indices_data)
    analysis_results = analyzer.calculate_metrics()
    performance_ranking = analyzer.get_performance_ranking()
    indices_strength = analyzer.get_indices_strength()
    
    # Prepare combined dataframe for charts
    combined_df = prepare_combined_dataframe(indices_data)
    
    # Tab structure
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "📈 Trends", "🏆 Rankings", "💪 Strength", "📄 Report"])
    
    # Tab 1: Overview
    with tab1:
        st.header("Key Metrics Summary")
        
        # Display current metrics
        if not analysis_results.empty:
            # Create columns for top metrics
            col1, col2, col3 = st.columns(3)
            
            top_performer = performance_ranking.iloc[0]
            worst_performer = performance_ranking.iloc[-1]
            
            with col1:
                st.metric(
                    "Top Performer",
                    top_performer['Index'],
                    f"{top_performer['Change (%)']}%"
                )
            
            with col2:
                st.metric(
                    "Worst Performer",
                    worst_performer['Index'],
                    f"{worst_performer['Change (%)']}%"
                )
            
            with col3:
                avg_volatility = analysis_results['Volatility (%)'].mean()
                st.metric(
                    "Avg Volatility",
                    f"{avg_volatility:.2f}%"
                )
            
            st.divider()
            
            # Display detailed metrics table
            st.subheader("Detailed Metrics")
            display_columns = ['Index', 'Current Price', 'Change (%)', 'Volatility (%)', 'MA-20', 'MA-50']
            
            st.dataframe(
                analysis_results[display_columns].style.format({
                    'Current Price': '{:.2f}',
                    'Change (%)': '{:.2f}%',
                    'Volatility (%)': '{:.2f}%',
                    'MA-20': '{:.2f}',
                    'MA-50': '{:.2f}'
                }),
                use_container_width=True,
                hide_index=True
            )
    
    # Tab 2: Trends
    with tab2:
        st.header("Trend Analysis (26 Weeks)")
        
        if not combined_df.empty:
            # Line chart for all indices
            fig = go.Figure()
            
            for column in combined_df.columns:
                if column != 'Date':
                    fig.add_trace(go.Scatter(
                        x=combined_df['Date'],
                        y=combined_df[column],
                        mode='lines',
                        name=column,
                        hovertemplate=f'{column}<br>Date: %{{x|%Y-%m-%d}}<br>Price: %{{y:.2f}}<extra></extra>'
                    ))
            
            fig.update_layout(
                title=f"Index Trends - Last {selected_weeks} Weeks",
                xaxis_title="Date",
                yaxis_title="Index Value",
                hovermode='x unified',
                height=500,
                template='plotly_white'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Normalized chart (percentage change from start)
            normalized_df = combined_df.copy()
            for column in normalized_df.columns:
                if column != 'Date':
                    start_val = normalized_df[column].iloc[0]
                    normalized_df[column] = ((normalized_df[column] / start_val) - 1) * 100
            
            fig_normalized = go.Figure()
            
            for column in normalized_df.columns:
                if column != 'Date':
                    fig_normalized.add_trace(go.Scatter(
                        x=normalized_df['Date'],
                        y=normalized_df[column],
                        mode='lines',
                        name=column,
                        hovertemplate=f'{column}<br>Date: %{{x|%Y-%m-%d}}<br>Change: %{{y:.2f}}%<extra></extra>'
                    ))
            
            fig_normalized.update_layout(
                title="Normalized Performance (% Change from Start)",
                xaxis_title="Date",
                yaxis_title="Percentage Change (%)",
                hovermode='x unified',
                height=500,
                template='plotly_white'
            )
            
            st.plotly_chart(fig_normalized, use_container_width=True)
        else:
            st.warning("⚠️ No trend data available.")
    
    # Tab 3: Rankings
    with tab3:
        st.header("Performance Rankings")
        
        if not performance_ranking.empty:
            # Ranking bar chart
            fig_ranking = px.bar(
                performance_ranking.sort_values('Change (%)', ascending=True),
                x='Change (%)',
                y='Index',
                orientation='h',
                color='Change (%)',
                color_continuous_scale='RdYlGn',
                title="Index Performance Comparison",
                labels={'Change (%)': 'Performance Change (%)'},
                height=400
            )
            
            fig_ranking.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                template='plotly_white'
            )
            
            st.plotly_chart(fig_ranking, use_container_width=True)
            
            # Ranking table
            st.subheader("Detailed Ranking")
            ranking_display = performance_ranking[['Rank', 'Index', 'Change (%)', 'Volatility (%)']].copy()
            
            st.dataframe(
                ranking_display.style.format({
                    'Change (%)': '{:.2f}%',
                    'Volatility (%)': '{:.2f}%'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("⚠️ No ranking data available.")
    
    # Tab 4: Strength
    with tab4:
        st.header("Indices Strength Comparison")
        
        if not indices_strength.empty:
            # Strength score gauge chart
            fig_strength = px.bar(
                indices_strength.sort_values('Strength Score', ascending=False),
                x='Strength Score',
                y='Index',
                orientation='h',
                color='Strength Score',
                color_continuous_scale='Blues',
                title="Indices Strength Score (0-100)",
                labels={'Strength Score': 'Strength Score'},
                height=400
            )
            
            fig_strength.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                template='plotly_white',
                xaxis_range=[0, 100]
            )
            
            st.plotly_chart(fig_strength, use_container_width=True)
            
            # Strength metrics table
            st.subheader("Strength Metrics")
            strength_display = indices_strength[['Index', 'Change (%)', 'Strength Score']].copy()
            
            st.dataframe(
                strength_display.style.format({
                    'Change (%)': '{:.2f}%',
                    'Strength Score': '{:.2f}'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Comparative text
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"**Strongest Index:** {indices_strength.iloc[0]['Index']} "
                       f"(Score: {indices_strength.iloc[0]['Strength Score']:.2f})")
            
            with col2:
                st.warning(f"**Weakest Index:** {indices_strength.iloc[-1]['Index']} "
                          f"(Score: {indices_strength.iloc[-1]['Strength Score']:.2f})")
        else:
            st.warning("⚠️ No strength data available.")
    
    # Tab 5: Report Generation
    with tab5:
        st.header("Generate PDF Report")
        
        st.info("Generate a comprehensive PDF report with all analysis metrics, rankings, and strength scores.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            report_title = st.text_input("Report Title", value="Indian Indices Analysis Report")
        
        with col2:
            include_chart = st.checkbox("Include Trend Chart", value=True)
        
        if st.button("📄 Generate PDF Report", use_container_width=True, type="primary"):
            with st.spinner("Generating PDF report..."):
                try:
                    # Initialize report generator
                    report_gen = PDFReportGenerator(output_path="reports")
                    
                    # Prepare chart if needed
                    chart_path = None
                    if include_chart and not combined_df.empty:
                        # Save normalized chart as image
                        fig_temp = go.Figure()
                        
                        normalized_df = combined_df.copy()
                        for column in normalized_df.columns:
                            if column != 'Date':
                                start_val = normalized_df[column].iloc[0]
                                normalized_df[column] = ((normalized_df[column] / start_val) - 1) * 100
                        
                        for column in normalized_df.columns:
                            if column != 'Date':
                                fig_temp.add_trace(go.Scatter(
                                    x=normalized_df['Date'],
                                    y=normalized_df[column],
                                    mode='lines',
                                    name=column
                                ))
                        
                        fig_temp.update_layout(
                            title="Normalized Performance",
                            height=400,
                            width=800
                        )
                        
                        # Save chart as HTML image
                        chart_path = "reports/temp_chart.html"
                        os.makedirs("reports", exist_ok=True)
                        fig_temp.write_html(chart_path)
                    
                    # Generate PDF
                    pdf_path = report_gen.create_report(
                        analysis_results,
                        performance_ranking,
                        indices_strength,
                        chart_path
                    )
                    
                    if pdf_path and os.path.exists(pdf_path):
                        st.success(f"✅ PDF report generated successfully!")
                        
                        # Display download button
                        with open(pdf_path, 'rb') as f:
                            pdf_data = f.read()
                        
                        st.download_button(
                            label="📥 Download PDF Report",
                            data=pdf_data,
                            file_name=os.path.basename(pdf_path),
                            mime="application/pdf",
                            use_container_width=True
                        )
                        
                        # Clean up temp chart
                        if chart_path and os.path.exists(chart_path):
                            os.remove(chart_path)
                    else:
                        st.error("❌ Failed to generate PDF report.")
                
                except Exception as e:
                    st.error(f"❌ Error generating report: {str(e)}")
        
        st.divider()
        
        # Report information
        st.subheader("Report Contents")
        st.markdown("""
        The PDF report includes:
        - **Key Metrics Summary**: Current prices, changes, volatility, moving averages
        - **Performance Ranking**: Ranked indices by performance over the selected period
        - **Indices Strength Score**: Normalized strength metrics (0-100 scale)
        - **Trend Analysis Chart**: Visual representation of index movements
        """)

else:
    st.warning("⚠️ Please click 'Refresh Data' to load indices data.")
    if st.button("🔄 Load Data Now"):
        with st.spinner("Fetching data from database..."):
            sync_database(force_refresh=True)
            st.session_state.indices_data = get_indices_data_from_db(period_days=180)
        st.rerun()

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #888; font-size: 12px;'>
    <p>Last Updated: <span id='timestamp'></span></p>
    <p>Data Source: yfinance | Analysis Period: Last 26 Weeks</p>
</div>
""", unsafe_allow_html=True)
