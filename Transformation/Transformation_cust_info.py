import pyodbc
import pandas as pd

# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.cust_info',conn)

# Fix the encoding errors
def fix_mojibake(text):
    if not isinstance(text, str): return text
    try:
        return text.encode('gbk').decode('utf-8')
    except:
        return text

df['cst_firstnar'] = df['cst_firstnar'].apply(fix_mojibake)
df['cst_lastnan'] = df['cst_lastnan'].apply(fix_mojibake)

# Drop NAs in cst_id
df = df.dropna(subset='cst_id')
print('Number of NAs now:', df['cst_id'].isnull().sum())

# Drop duplicates in cst_id
df = df.drop_duplicates(subset='cst_id', keep='last')

# Drop whitespaces in first name and last name columns
df['cst_firstnar'] = df['cst_firstnar'].str.strip()
df['cst_lastnan'] = df['cst_lastnan'].str.strip()

# Change S and M
df['cst_marital'] = df['cst_marital'].str.replace('S', 'Single', regex=False)
df['cst_marital'] = df['cst_marital'].str.replace('M', 'Married', regex=False)

# Change M and F
df['cst_gndr'] = df['cst_gndr'].str.replace('M', 'Male', regex=False)
df['cst_gndr'] = df['cst_gndr'].str.replace('F', 'Female', regex=False)

# Replace null with na  

print(df)

# Create transformation schema
cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
BEGIN
    EXEC('CREATE SCHEMA [transformation]')
END""")

# Create cust_info table
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('transformation.cust_info') AND type = 'U')
        BEGIN
            CREATE TABLE [transformation].[cust_info] (
                cst_id           NVARCHAR(MAX),
                cst_key          NVARCHAR(MAX),
                cst_firstnar     NVARCHAR(MAX),
                cst_lastnan      NVARCHAR(MAX),
                cst_marital      NVARCHAR(MAX),
                cst_gndr         NVARCHAR(MAX),
                cst_create_date  NVARCHAR(MAX)
            )   
        END
    """)

cursor.execute("TRUNCATE TABLE transformation.cust_info")

# Insert saving
df_to_load = df.astype(str).replace('nan', '') 
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.cust_info 
    (cst_id, cst_key, cst_firstnar, cst_lastnan, cst_marital, cst_gndr, cst_create_date) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()
