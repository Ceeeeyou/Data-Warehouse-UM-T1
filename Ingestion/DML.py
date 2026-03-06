import pyodbc

# Connect directly to DWH
server = r'.\SQLEXPRESS'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# Clean old data
# Ensures that the tables are empty each time we run
print("Cleaning old data from all tables...")
cursor.execute("TRUNCATE TABLE ingestion.cust_info")
cursor.execute("TRUNCATE TABLE ingestion.prd_info")
cursor.execute("TRUNCATE TABLE ingestion.sales_details")
cursor.execute("TRUNCATE TABLE ingestion.CUST_AZ12")
cursor.execute("TRUNCATE TABLE ingestion.LOC_A101")
cursor.execute("TRUNCATE TABLE ingestion.PX_CAT_G1V2")

# Bulk insert
print("Starting Bulk Inserts...")

cursor.execute("BULK INSERT ingestion.cust_info FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_crm/cust_info.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- cust_info loaded.")

cursor.execute("BULK INSERT ingestion.prd_info FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_crm/prd_info.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- prd_info loaded.")

cursor.execute("BULK INSERT ingestion.sales_details FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_crm/sales_details.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- sales_details loaded.")

cursor.execute("BULK INSERT ingestion.CUST_AZ12 FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- CUST_AZ12 loaded.")

cursor.execute("BULK INSERT ingestion.LOC_A101 FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_erp/LOC_A101.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- LOC_A101 loaded.")

cursor.execute("BULK INSERT ingestion.PX_CAT_G1V2 FROM 'D:/Maastricht University/Year3/DataEngineering/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT='CSV', FIRSTROW=2)")
print("- PX_CAT_G1V2 loaded.")

print("\nDML Complete")

conn.close()