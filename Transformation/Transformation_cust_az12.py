import pyodbc
import pandas as pd


# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.CUST_AZ12',conn)

# Extract the customer id
df['CID'] = df['CID'].astype(str).str[-5:]

# Replace whitespaces with null
df['GEN'] = df['GEN'].str.strip().replace('', None)

# Replace M and F
df['GEN'] = df['GEN'].replace({'F': 'Female', 'M': 'Male'})

# Change bdate which is later than now to null
df['BDATE'] = pd.to_datetime(df['BDATE'], errors='coerce')
df.loc[df['BDATE'] > pd.Timestamp.now(), 'BDATE'] = None

print(df.head())

cursor.execute("IF OBJECT_ID('transformation.cust_az12', 'U') IS NOT NULL DROP TABLE transformation.cust_az12")
cursor.execute("""
    CREATE TABLE [transformation].[cust_az12] (
        cid    INT,
        bdate  DATE,
        gen    NVARCHAR(20)
    )
""")

cursor.execute("TRUNCATE TABLE transformation.cust_az12")

# Insert saving
df_to_load = df.where(df.notnull(), None)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.cust_az12
    (cid, bdate, gen) 
    VALUES (?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()