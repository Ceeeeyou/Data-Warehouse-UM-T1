import pyodbc
import pandas as pd

# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.prd_info',conn)

# Split the prd_key
parts = df['prd_key'].str.split('-')

cat_values = parts.str[0:2].str.join('-')

df.insert(2, 'cat_key', cat_values)

df['prd_key'] = parts.str[2:].str.join('-')

# Change - to _ in cat_key
df['cat_key'] = df['cat_key'].str.replace('-', '_', regex=False)

# Replace null in prd_cost with 0
df['prd_cost'] = df['prd_cost'].fillna(0)

# Replace the product line 
df['prd_line'] = df['prd_line'].str.replace('M', 'Mountain', regex=False)
df['prd_line'] = df['prd_line'].str.replace('T', 'Touring', regex=False)
df['prd_line'] = df['prd_line'].str.replace('S', 'Sport', regex=False)
df['prd_line'] = df['prd_line'].str.replace('R', 'Road', regex=False)
df['prd_line'] = df['prd_line'].fillna('na')


# Modify the date
df['prd_start_d'] = pd.to_datetime(df['prd_start_d'], errors='coerce')
df['prd_end_d'] = pd.to_datetime(df['prd_end_d'], errors='coerce')

df = df.sort_values(['prd_key', 'prd_start_d'])

df['new_end'] = df.groupby('prd_key')['prd_start_d'].shift(-1)
df['prd_end_d'] = df['new_end'] - pd.Timedelta(days=1)
df = df.drop(columns=['new_end'])

df = df.sort_index()

# Data formating
df['prd_start_d'] = df['prd_start_d'].dt.strftime('%Y-%m-%d').replace(['NaT', '0001-01-01'], None)
df['prd_end_d'] = df['prd_end_d'].dt.strftime('%Y-%m-%d').replace(['NaT', '0001-01-01'], None)
df['prd_id'] = pd.to_numeric(df['prd_id'], errors='coerce').astype(int)
df['prd_cost'] = pd.to_numeric(df['prd_cost'], errors='coerce').astype(float)

print(df.head())

cursor.execute("IF OBJECT_ID('transformation.prd_info', 'U') IS NOT NULL DROP TABLE transformation.prd_info")
cursor.execute("""
    CREATE TABLE [transformation].[prd_info] (
        prd_id      INT,              
        prd_key     NVARCHAR(100),    
        cat_key     NVARCHAR(100),
        prd_nm      NVARCHAR(500),    
        prd_cost    DECIMAL(18, 2),   
        prd_line    NVARCHAR(100),    
        prd_start_d DATE,             
        prd_end_d   DATE              
    )
""")
cursor.execute("TRUNCATE TABLE transformation.prd_info")

# Insert saving
df_to_load = df.where(df.notnull(), None)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.prd_info 
    (prd_id, prd_key, cat_key, prd_nm, prd_cost, prd_line, prd_start_d, prd_end_d) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()