#read the retail sales data in from MySQL db and MySQL pivot for time series 
#analyses

import pandas as pd
#pd.set_option('display.max_rows', 3000)
pd.reset_option('display.max_rows')
#pd.set_option('display.max_columns', 357)
pd.reset_option('display.max_columns')

import yaml
import sqlalchemy
from sqlalchemy import create_engine, text

import pymysql #or mysql-connector-python
import matplotlib.pyplot as plt
import datetime
#from datetime import datetime, date

#retail sales database structures
DATABASE_NAME = "retail_sales_m8"
TABLE_NAME = "retail_sales_1992_2021"


#MySQL connection
yaml_file = r'C:\Users\kedre\OneDrive\Desktop\sample\db_retail_sales_SQLAlchemy.yaml'
def get_db_engine(config_file=yaml_file):
    """
    Loads database configuration from a YAML file and creates a SQLAlchemy engine.
    """
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    db_config = config['database']
    db_url = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['db']}"
    )
    engine = create_engine(db_url)
    return engine

if __name__ == "__main__": #make this code modular (this block only executes if this program name)
    engine = get_db_engine()
    
    try:
        with engine.connect() as connection:
            #print("Successfully connected to the MySQL database!")
            query = text(f"""SELECT 
                    business_type,
                    sales,
                    DATE(CONCAT(CAST(Year AS CHAR), '-', 
                            CASE month_col 
                                WHEN '_1' THEN '01'
                                WHEN '_2' THEN '02'
                                WHEN '_3' THEN '03'
                                WHEN '_4' THEN '04'
                                WHEN '_5' THEN '05'
                                WHEN '_6' THEN '06'
                                WHEN '_7' THEN '07'
                                WHEN '_8' THEN '08'
                                WHEN '_9' THEN '09'
                                WHEN '_10' THEN '10'
                                WHEN '_11' THEN '11'
                                WHEN '_12' THEN '12'
                            END, '-01')) as date
                FROM (
                    SELECT business_type, Year, '_1' as month_col, _1 as sales FROM retail_sales_1992_2021 WHERE _1 IS NOT NULL AND _1 != ''
                    UNION ALL
                    SELECT business_type, Year, '_2' as month_col, _2 as sales FROM retail_sales_1992_2021 WHERE _2 IS NOT NULL AND _2 != ''
                    UNION ALL
                    SELECT business_type, Year, '_3' as month_col, _3 as sales FROM retail_sales_1992_2021 WHERE _3 IS NOT NULL AND _3 != ''
                    UNION ALL
                    SELECT business_type, Year, '_4' as month_col, _4 as sales FROM retail_sales_1992_2021 WHERE _4 IS NOT NULL AND _4 != ''
                    UNION ALL
                    SELECT business_type, Year, '_5' as month_col, _5 as sales FROM retail_sales_1992_2021 WHERE _5 IS NOT NULL AND _5 != ''
                    UNION ALL
                    SELECT business_type, Year, '_6' as month_col, _6 as sales FROM retail_sales_1992_2021 WHERE _6 IS NOT NULL AND _6 != ''
                    UNION ALL
                    SELECT business_type, Year, '_7' as month_col, _7 as sales FROM retail_sales_1992_2021 WHERE _7 IS NOT NULL AND _7 != ''
                    UNION ALL
                    SELECT business_type, Year, '_8' as month_col, _8 as sales FROM retail_sales_1992_2021 WHERE _8 IS NOT NULL AND _8 != ''
                    UNION ALL
                    SELECT business_type, Year, '_9' as month_col, _9 as sales FROM retail_sales_1992_2021 WHERE _9 IS NOT NULL AND _9 != ''
                    UNION ALL
                    SELECT business_type, Year, '_10' as month_col, _10 as sales FROM retail_sales_1992_2021 WHERE _10 IS NOT NULL AND _10 != ''
                    UNION ALL
                    SELECT business_type, Year, '_11' as month_col, _11 as sales FROM retail_sales_1992_2021 WHERE _11 IS NOT NULL AND _11 != ''
                    UNION ALL
                    SELECT business_type, Year, '_12' as month_col, _12 as sales FROM retail_sales_1992_2021 WHERE _12 IS NOT NULL AND _12 != ''
                ) pivoted_data
                ORDER BY business_type, Year, month_col""")     
            result = connection.execute(query)
            rows = result.fetchall()
            column_names = result.keys()
            retail_sales_df_to_ts = pd.DataFrame(rows, columns=column_names)
            #print("Pivot Successful!")
            retail_sales_df_to_ts['date'] = pd.to_datetime(retail_sales_df_to_ts['date']) #not successful in converting the date column to date type on the SELECT, so doing it after
            retail_sales_df_to_ts.sort_values(by='date', inplace=True)
    except Exception as e:
        print(f"Error connecting to the database: {e}")

#print("\nFinal DataFrame (first 5 rows):")
#print(retail_sales_df_to_ts.head())
#retail_sales_df_to_ts.info()
        
#Analyses and Visualizations (test the code here then run in the terminal window)
#1. Time Series plot
#retail_sales_df_to_ts.info()

#limit to the focus business_types and date range
#business_types_to_plot = ['Retail sales and food services excl motor vehicle and parts','Retail sales and food services excl gasoline stations','Retail sales and food services excl motor vehicle and parts and gasoline stations']
business_types_to_plot = ['Retail and food services sales, total']
filtered_df = retail_sales_df_to_ts[retail_sales_df_to_ts['business_type'].isin(business_types_to_plot)]

#end_date = '2021-03-01'given interpolation extending 2021 through EOY - don't want that
end_date = pd.to_datetime('2021-03-01')  # Changed to pandas datetime
#if not pd.api.types.is_datetime64_any_dtype(filtered_df['date']):
#filtered_df['date'] = pd.to_datetime(filtered_df['date'])
    
date_filtered_df = filtered_df[filtered_df['date'] < end_date]

plt.figure(figsize=(12, 8))

#plot each business type separately if > 1
for business_type in date_filtered_df['business_type'].unique():
    data = date_filtered_df[date_filtered_df['business_type'] == business_type]
    plt.plot(data['date'], data['sales'], marker='o', linewidth=2, 
             label=business_type, markersize=4)

#add some identifying info and show it
plt.title('Sales Time Series by Business Type (1/1992-2/2021)', fontweight='bold')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.legend()
plt.show()

# Print some basic statistics
print("Data Summary:")
print(date_filtered_df.groupby('business_type')['sales'].agg(['mean', 'min', 'max', 'std']).round(2))


#2. 2 side view, 3. seasonal plot, 4. plot by trend/seasonality/both (though that's already done in 1), 
#5. cyclic (not seasonal but according to the economy) if possible?, 6. additive or multiplicative decomp?