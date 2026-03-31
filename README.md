# MRTS Retail Time Series Analysis

A comprehensive ETL pipeline and time-series analysis of the U.S. Census Bureau's 
Monthly Retail Trade Survey (MRTS) dataset covering 1992–2021.

**MIT xPRO Data Engineering Bootcamp — Capstone Project**  
**Author:** Kedren Reade Sitton

## Project Overview
Analyzes retail sales trends across 8+ business categories using:
- Excel ingestion directly via Pandas (no intermediate CSV export)
- MySQL schema creation and data loading via Python scripts
- Trend, seasonality, percentage change, and rolling window analyses

## Key Findings
- Grocery and sporting goods stores show the strongest growth trajectories
- Bookstores and office supply stores show consistent decline (digital economy)
- COVID-19 (2020) created notable anomalies, especially in hobby/toy/game stores
- Rolling window analysis detected emerging trends ahead of month-over-month comparisons

## Tech Stack
- Python, Pandas, NumPy, Matplotlib, Seaborn
- scikit-learn (LinearRegression), SciPy
- MySQL, SQLAlchemy, PyMySQL
- Jupyter Notebook

## How to Run
1. Clone this repo: `git clone https://github.com/readesie/mrts-retail-time-series.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Download the MRTS Excel file from the [Census Bureau](https://www.census.gov/retail/mrts/about_the_surveys.html)
4. Update file paths in the notebooks/scripts to match your local setup
5. Run the schema and data load scripts first, then open the Jupyter notebook

## Project Structure
```
mrts-retail-time-series/
│
├── notebooks/
│   └── mrts_analysis.ipynb          # Main analysis notebook
├── scripts/
│   ├── retail_sales_db_schema.py    # MySQL schema creation
│   └── retail_sales_db_data.py      # MySQL data load
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

## Data Source
United States Census Bureau — [Monthly Retail Trade Survey](https://www.census.gov/retail/mrts/about_the_surveys.html)

## References
See the References section of the notebook for full citations.
