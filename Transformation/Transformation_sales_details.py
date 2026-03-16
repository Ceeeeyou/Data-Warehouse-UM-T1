import pyodbc
import pandas as pd


# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.sales_details',conn)

# Change date with 0 to null and set inconsistant date to the max date
date_cols = ['sls_ship_dt', 'sls_due_dt', 'sls_order_dt']
for col in date_cols:
    df[col] = pd.to_numeric(df[col], errors = 'coerce')
    df.loc[df[col] == 0, col] = pd.NA

for col in date_cols:
    df[col] = df.groupby('sls_ord_num')[col].transform('max')

# Change the date format to datetime
for col in date_cols:
    df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
    df[col] = df[col].dt.strftime('%Y-%m-%d')

# Change the sales, quantity, and price to numeric
sales_cols = ['sls_sales','sls_quantity','sls_price']

for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors = 'coerce')

bad_price_mask = (df['sls_price'] <= 0) | (df['sls_price'].isna())

# Apply the fallback: Price = Sales / Quantity
df.loc[bad_price_mask, 'sls_price'] = df['sls_sales'] / df['sls_quantity']

# Recalculate Sales based on correct Price and Quantity
df['sls_sales'] = df['sls_quantity'] * df['sls_price']

df['sls_quantity'] = df['sls_quantity'].astype(int)

print(df.head())

cursor.execute("IF OBJECT_ID('transformation.sales_details', 'U') IS NOT NULL DROP TABLE transformation.sales_details")
cursor.execute("""
    CREATE TABLE [transformation].[sales_details] (
        sls_ord_num  NVARCHAR(100),
        sls_prd_key  NVARCHAR(100),
        sls_cust_id  NVARCHAR(100),
        sls_order_dt DATE,    
        sls_ship_dt  DATE,    
        sls_due_dt   DATE,    
        sls_sales    DECIMAL(10, 2),
        sls_quantity INT,
        sls_price    DECIMAL(10, 2)
    )
""")

cursor.execute("TRUNCATE TABLE transformation.sales_details")

# Insert saving
df_to_load = df.where(df.notnull(), None)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.sales_details
    (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()