import pyodbc
import pandas as pd


# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

df = pd.read_sql('SELECT * FROM ingestion.PX_CAT_G1V2',conn)

print(df.head())

cursor.execute("IF OBJECT_ID('transformation.px_cat_g1v2', 'U') IS NOT NULL DROP TABLE transformation.px_cat_g1v2")
cursor.execute("""
    CREATE TABLE [transformation].[px_cat_g1v2] (
        id           NVARCHAR(10), 
        cat          NVARCHAR(50),             
        subcat       NVARCHAR(50),             
        maintenance  NVARCHAR(10)                
    )
""")

cursor.execute("TRUNCATE TABLE transformation.px_cat_g1v2")

# Insert saving
df_to_load = df.where(df.notnull(), None)
data_list = [tuple(x) for x in df_to_load.values]

insert_query = """
    INSERT INTO transformation.px_cat_g1v2
    (id, cat, subcat, maintenance) 
    VALUES (?, ?, ?, ?)
"""
cursor.fast_executemany = True

print(f"Loading {len(data_list)} rows directly...")
cursor.executemany(insert_query, data_list)
conn.commit()
print("Success!") 



conn.close()