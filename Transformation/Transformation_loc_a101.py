import pyodbc
import pandas as pd


# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.LOC_A101',conn)

# Extract the customer id
df['CID'] = df['CID'].astype(str).str[-5:]

# Change empty country to null
df['CNTRY'] = df['CNTRY'].str.strip().replace('', None)
# Change country names
df['CNTRY'] = df['CNTRY'].replace({'US': 'United States','USA': 'United States','DE': 'Germany'})

print(df.head())

cursor.execute("IF OBJECT_ID('transformation.loc_a101', 'U') IS NOT NULL DROP TABLE transformation.loc_a101")
cursor.execute("""
    CREATE TABLE [transformation].[loc_a101] (
        cid     INT,
        cntry   NVARCHAR(50)
    )
""")

cursor.execute("TRUNCATE TABLE transformation.loc_a101")

# Insert saving
df_to_load = df.where(df.notnull(), None)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.loc_a101
    (cid, cntry) 
    VALUES (?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()