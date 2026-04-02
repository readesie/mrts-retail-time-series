#Module8 retail sales create db, create table, etc (modeled after bad-sakila-schema)
import yaml
import csv
import mysql.connector

yl_path=r'C:\Users\kedre\OneDrive\Desktop\sample\db_retail_sales.yaml'

#MySQL connection
db = yaml.safe_load(open(yl_path))
config_db = {
    'user':         db['user'],
    'password':     db['pwrd'],
    'host':         db['host'],
    'database':     db['db'],
    'auth_plugin':  'mysql_native_password'
}

#CSV file path and target table
csv_file_path = r'C:\Users\kedre\OneDrive\Desktop\sample\mrtssales92-present_final_preprocessed.csv'  # Replace with your CSV file name Module8_scratch.csv mrtssales92-present_TEST1_4.csv
table_name = "retail_sales_1992_2021"  # Replace with your MySQL table name

try:
    #connect to MySQL
    cnx = mysql.connector.connect(**config_db)
    cursor = cnx.cursor()

    #open the CSV
    with open(csv_file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        #skips header row
        header = next(csv_reader) 

        #build INSERT query
        #make sure # placeholders (%s) matches the # columns of CSV/table
        #table column names must = CSV header names
        #missing values must be zero for numerics
        num_columns = len(header)  #given csv header matches table columns including order
        placeholders = ', '.join(['%s'] * num_columns)
        insert_sql = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"
        # If your CSV doesn't have a header row matching table columns,
        # you'll need to manually specify column names:
        # insert_sql = f"INSERT INTO {table_name} (column1, column2, column3) VALUES ({placeholders})"

        #iterate row of CSV to
        for row in csv_reader:
            cursor.execute(insert_sql, tuple(row))

    #commit
    cnx.commit()
    print(f"Data from '{csv_file_path}' successfully loaded into '{table_name}'.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # Close the cursor and connection
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'cnx' in locals() and cnx.is_connected():
        cnx.close()