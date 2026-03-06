import pyodbc

# Setup BASE connection
server = r'.\SQLEXPRESS'
base_conn = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;TrustServerCertificate=yes;'

# STEP 1: CREATE DATABASE (Connect to 'master')
print("Checking Database...")
# We add DATABASE=master just for this step in order to create a new data warehouse
conn = pyodbc.connect(base_conn + "DATABASE=master;", autocommit=True)
cursor = conn.cursor()

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DWH')
BEGIN
    CREATE DATABASE [DWH]
END""")
conn.close()

# STEP 2: CREATE SCHEMA & TABLE (Connect to DWH)
print("Connecting to DWH...")
# We add DATABASE=DWH here
conn = pyodbc.connect(base_conn + "DATABASE=DWH;", autocommit=True)
cursor = conn.cursor()

# Create Schema
print("Checking Schema 'ingestion'...")
# Create the schema if it hasn't yet been created
cursor.execute("""               
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ingestion')
BEGIN
    EXEC('CREATE SCHEMA [ingestion]')
END""")

# Create Table
print("Checking Table 'cust_info'...")
# Create the table if it hasn't yet been created
# type = 'U' specifies the type of object we are looking for is a table
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.cust_info') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[cust_info] (
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
print("Checking Table 'prd_info'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.prd_info') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[prd_info] (
                prd_id       NVARCHAR(MAX),
                prd_key      NVARCHAR(MAX),
                prd_nm       NVARCHAR(MAX),
                prd_cost     NVARCHAR(MAX),
                prd_line     NVARCHAR(MAX),
                prd_start_d  NVARCHAR(MAX),
                prd_end_d    NVARCHAR(MAX)
            )
        END
    """)

print("Checking Table 'sales_details'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.sales_details') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[sales_details] (
                sls_ord_num     NVARCHAR(MAX),
                sls_prd_key     NVARCHAR(MAX),
                sls_cust_id     NVARCHAR(MAX),
                sls_order_dt    NVARCHAR(MAX),    
                sls_ship_dt     NVARCHAR(MAX),    
                sls_due_dt      NVARCHAR(MAX),    
                sls_sales       NVARCHAR(MAX),
                sls_quantity    NVARCHAR(MAX),
                sls_price       NVARCHAR(MAX)
            )
        END
    """)
print("Checking Table 'CUST_AZ12'...")
    # Kept raw naming (CID, BDATE, GEN) to match source structure
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.CUST_AZ12') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[CUST_AZ12] (
                CID    NVARCHAR(MAX),
                BDATE  NVARCHAR(MAX),
                GEN    NVARCHAR(MAX)
            )
        END
    """)
print("Checking Table 'LOC_A101'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.LOC_A101') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[LOC_A101] (
                CID    NVARCHAR(MAX),
                CNTRY   NVARCHAR(MAX)
            )
        END
    """)

print("Checking Table 'PX_CAT_G1V2'...")
cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ingestion.PX_CAT_G1V2') AND type = 'U')
        BEGIN
            CREATE TABLE [ingestion].[PX_CAT_G1V2] (
                ID           NVARCHAR(MAX), 
                CAT          NVARCHAR(MAX),             
                SUBCAT       NVARCHAR(MAX),             
                MAINTENANCE  NVARCHAR(MAX)                
            )
        END
    """)

print("\nFinshed")

conn.close()