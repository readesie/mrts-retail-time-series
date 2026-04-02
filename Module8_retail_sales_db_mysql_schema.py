#Module8 retail sales create db, create table, etc (modeled after bad-sakila-schema)
#interactive check for initial schema drop
import yaml
import csv
import mysql.connector

yl_path=r'C:\Users\kedre\OneDrive\Desktop\sample\db_retail_sales.yaml'

#MySQL connection with 2 configs: 1 with a db to connect to and 1 without
#latter is so a db can be created, then used
db = yaml.safe_load(open(yl_path))
config_no_db = {
    'user':         db['user'],
    'password':     db['pwrd'],
    'host':         db['host'],
    'auth_plugin':  'mysql_native_password'
}
config_db = {
    'user':         db['user'],
    'password':     db['pwrd'],
    'host':         db['host'],
    'database':     db['db'],
    'auth_plugin':  'mysql_native_password'
}
#database structures to create
DATABASE_NAME = "retail_sales_m8"
PROPS = "UTF8MB4" #names and character set - properties
TABLE_NAME = "retail_sales_1992_2021"

#interactive check for full drop of the schema first
dropdbfirst = input(f"Do you want to drop the database '{DATABASE_NAME}' first (WARNING!!  WILL ERASE EVERYTHING!!) 'Y' if so: ")

try:
    #connect to MySQL server no db
    cnx = mysql.connector.connect(**config_no_db)
    cursor = cnx.cursor()

    #first drop the database if dropdbfirst='Y'
    if dropdbfirst == 'Y':
        createdorexist='created'
        cursor.execute(f"DROP SCHEMA IF EXISTS {DATABASE_NAME}")
        print(f"Schema '{DATABASE_NAME}' dropped before created on user request.")
    else:
        createdorexist='already exists'

    #create the database
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}")
    print(f"Schema '{DATABASE_NAME}' {createdorexist}.")

    #close the connection
    cursor.close()
    cnx.close()

    #reconnect - to new db just created
    cnx = mysql.connector.connect(**config_db)
    cursor = cnx.cursor()

    #set db properties
    cursor.execute(f"USE {DATABASE_NAME}")
    print(f"Schema '{DATABASE_NAME}' in use.")
    cursor.execute(f"SET NAMES {PROPS}")
    cursor.execute(f"SET character_set_client = {PROPS}")
    print(f"Schema '{DATABASE_NAME}' now has '{PROPS}' properties set.")

    #create the table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        business_type VARCHAR(255) NOT NULL,
        _1 INT,
        _2 INT,
        _3 INT,
        _4 INT,
        _5 INT,
        _6 INT,
        _7 INT,
        _8 INT,
        _9 INT,
        _10 INT,
        _11 INT,
        _12 INT,
        total INT,
        year INT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET={PROPS}
    """
    cursor.execute(create_table_query)
    print(f"Table '{TABLE_NAME}' {createdorexist} in '{DATABASE_NAME}'.")

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if 'cnx' in locals() and cnx.is_connected():
        cursor.close()
        cnx.close()
        print("MySQL connection closed.")