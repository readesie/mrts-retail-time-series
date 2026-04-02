#MySQL workbench validation run from the terminal window via Python in Visual Studio Code
#2. calculate year totals by business type using MySQL code

import pandas as pd
#pd.set_option('display.max_rows', 3000)
#pd.reset_option('display.max_rows')
#pd.set_option('display.max_columns', 357)
#pd.reset_option('display.max_columns')

import yaml
import sqlalchemy
from sqlalchemy import create_engine, text

import pymysql #or mysql-connector-python
#import matplotlib.pyplot as plt
#from datetime import datetime

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
                                YEAR(date) as year,
                                SUM(CAST(sales AS SIGNED)) as year_sales
                            FROM (
                                SELECT 
                                    business_type,
                                    sales,
                                    DATE(CONCAT(Year, '-', 
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
                            ) monthly_data
                            GROUP BY business_type, YEAR(date)
                            ORDER BY business_type, year""")     
            result = connection.execute(query)
            rows = result.fetchall()
            column_names = result.keys()
            retail_sales_df_to_ts = pd.DataFrame(rows, columns=column_names)
            print("Counts by business_type/year Successful!")

    except Exception as e:
        print(f"Error connecting to the database: {e}")
print(retail_sales_df_to_ts.head())