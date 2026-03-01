# Indian Stock Indices Analysis - Streamlit App

A comprehensive Streamlit application for analyzing major Indian stock market indices with real-time data visualization, technical analysis, and automated PDF report generation.

## Features

✨ **Key Features:**
- **Real-time Data Fetching**: Pulls live data for major Indian indices (NIFTY 50, NIFTY IT, NIFTY BANK, NIFTY PHARMA, NIFTY AUTO, SENSEX)
- **Technical Analysis**: Calculates volatility, moving averages, and performance metrics
- **Interactive Visualizations**: Trend charts, performance rankings, and strength comparisons
- **PDF Report Generation**: Automated report creation with tables and charts
- **Performance Metrics**: 
  - Price changes and percentage changes
  - Volatility analysis (annualized)
  - 20-day and 50-day moving averages
  - High/Low price ranges
  - Strength scoring (0-100 scale)

## Project Structure

```
testproject_copilot/
├── app.py                          # Main Streamlit application
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── modules/
│   ├── __init__.py
│   ├── data_fetcher.py            # Data fetching module
│   ├── analysis.py                # Analysis and metrics calculation
│   └── report_generator.py        # PDF report generation
└── reports/                       # Generated PDF reports (created on first run)
```

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Step 1: Create Virtual Environment (Optional but recommended)

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Run the Application

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Usage

### 1. **Main Dashboard (Overview Tab)**
   - View key metrics for all indices
   - Top and worst performers
   - Average volatility across indices
   - Detailed metrics table

### 2. **Trend Analysis (Trends Tab)**
   - Line charts showing index movements over 26 weeks
   - Normalized performance comparison
   - Interactive hover information

### 3. **Performance Rankings (Rankings Tab)**
   - Bar charts comparing index performance
   - Ranked table of indices
   - Performance percentage changes

### 4. **Strength Analysis (Strength Tab)**
   - Strength score comparison (0-100 scale)
   - Visual gauge representation
   - Strongest and weakest indices

### 5. **Report Generation (Report Tab)**
   - Generate professional PDF reports
   - Includes all metrics, rankings, and charts
   - Download and save reports locally

## Configuration Options

In the sidebar, you can:
- **Select Analysis Period**: 26 weeks (6 months), 13 weeks (3 months), or 52 weeks (1 year)
- **Refresh Data**: Manually update data from yfinance
- **View Available Indices**: List of tracked indices

## Supported Indices

| Index Name | Ticker |
|-----------|--------|
| NIFTY 50 | ^NSEI |
| NIFTY IT | ^NSMIT |
| NIFTY BANK | ^NSEBANK |
| NIFTY PHARMA | ^NSEMDCP50 |
| NIFTY AUTO | ^CNXIT |
| SENSEX | ^BSESN |

## Data Source

- **Primary Source**: yfinance (Yahoo Finance)
- **Data Type**: Historical OHLCV (Open, High, Low, Close, Volume)
- **Update Frequency**: Real-time (fetched on demand)

## PDF Report Contents

The generated PDF report includes:

1. **Title Page**: Report generation timestamp
2. **Key Metrics Table**: 
   - Current prices
   - Price changes
   - Volatility metrics
   - Moving averages
3. **Performance Ranking**: Indices ranked by performance
4. **Strength Score Table**: Normalized strength metrics
5. **Trend Analysis Chart**: Visual representation of indices performance

## Technical Metrics Explained

### Change (%)
**Formula**: `(Current Price - Start Price) / Start Price * 100`
- Positive: Index is up over the period
- Negative: Index is down over the period

### Volatility (%)
**Formula**: `Std Dev of Daily Returns * √252 * 100`
- Annualized volatility
- Higher value = more price fluctuations
- Useful for risk assessment

### Strength Score (0-100)
**Formula**: `((Index Change - Min Change) / (Max Change - Min Change)) * 100`
- Normalized comparison of all indices
- 100 = Best performing index
- 0 = Worst performing index

### Moving Averages
- **MA-20**: 20-day average (short-term trend)
- **MA-50**: 50-day average (medium-term trend)

## Troubleshooting

### Issue: "No data available"
- **Solution**: Click "Refresh Data" in the sidebar to fetch latest data
- **Cause**: Network connectivity or yfinance service unavailability

### Issue: PDF Report not generating
- **Solution**: Check if the `reports/` directory exists and has write permissions
- **Cause**: Permission issues or missing directory

### Issue: Missing data for some indices
- **Solution**: Some indices might have limited historical data
- **Workaround**: Try different time periods

## Dependencies

- **streamlit**: Web application framework
- **yfinance**: Financial data fetching
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computations
- **plotly**: Interactive visualizations
- **reportlab**: PDF generation

## Performance Notes

- Initial data fetch may take 30-60 seconds depending on internet speed
- PDF generation usually takes 5-10 seconds
- Cached data reduces subsequent load times

## Future Enhancements

- [ ] Sector-wise analysis
- [ ] Custom indicators (RSI, MACD, Bollinger Bands)
- [ ] Email report delivery
- [ ] Database integration for historical reports
- [ ] Multi-period comparison
- [ ] Alert system for threshold breaches
- [ ] Advanced charting options

## License

This project is provided as-is for educational and analytical purposes.

## Support

For issues or questions, please check:
1. Requirements are properly installed: `pip show streamlit`
2. You're using Python 3.8+: `python --version`
3. yfinance is accessible: `python -c "import yfinance"`

## Contact

Created for Indian stock market analysis and education.

---

**Last Updated**: March 2026
