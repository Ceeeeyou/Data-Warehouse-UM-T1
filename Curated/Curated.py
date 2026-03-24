import pyodbc
import pandas as pd

def run():
    # Connect directly to DWH
    server = r'.\SQLEXPRESS'
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE=DWH;Trusted_Connection=yes;TrustServerCertificate=yes;'
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    # read customer data
    customer_crm_df = pd.read_sql('SELECT * FROM transformation.cust_info',conn)
    customer_erp_df = pd.read_sql('SELECT * FROM transformation.cust_az12',conn)
    loc_erp_df = pd.read_sql('SELECT * FROM transformation.loc_a101',conn)

    # Customer Dimension table
    # left join first two
    df = pd.merge(
        left= customer_crm_df,
        right= customer_erp_df,
        how= 'left',
        left_on= 'cst_id',
        right_on='cid'
    )
    # left join the last one
    df = pd.merge(
        left= df,
        right= loc_erp_df,
        how= 'left',
        left_on= 'cst_id',
        right_on='cid'
    )

    # choose the columns we want
    dim_customer = pd.DataFrame({
        'customer_id' : df['cst_id'],
        'fitst_name' : df['cst_firstnar'],
        'last_name': df['cst_lastnan'],
        'country': df['cntry'],
        'marital_status': df['cst_marital'],
        'gender': df['cst_gndr'],
        'birth_date': df['bdate'],
        'create_date': df['cst_create_date'] 
    })

    # insert the surrugate key as .index+1
    dim_customer.insert(0, 'customer_key', dim_customer.index+1)

    print(dim_customer.head())
    # Product Dimension table
    prd_crm_df = pd.read_sql('SELECT * FROM transformation.prd_info',conn)
    cat_erp_df = pd.read_sql('SELECT * FROM transformation.px_cat_g1v2',conn)

    df = pd.merge(
        left= prd_crm_df,
        right= cat_erp_df,
        how= 'left',
        left_on= 'cat_key',
        right_on='id'
    )

    dim_product = pd.DataFrame({
        'product_id': df['prd_key'],
        'product_name': df['prd_nm'],
        'category_id': df['cat_key'],
        'subcategory': df['subcat'],
        'maintenance': df['maintenance'],
        'product_cost': df['prd_cost'],
        'product_line': df['prd_line'],
        'start_date': df['prd_start_d'],
        'end_date': df['prd_end_d']
    })

    dim_product.insert(0, 'product_key', dim_product.index+1)
    print(dim_product.head())

    # Sale Fact table
    # left join the salels fact table by the new key and original number
    sales_crm_df = pd.read_sql('SELECT * FROM transformation.sales_details',conn)
    df = pd.merge(
        left= sales_crm_df,
        right= dim_customer[['customer_key','customer_id']],
        how= 'left',
        left_on= 'sls_cust_id',
        right_on='customer_id'
    )

    df = pd.merge(
        left= df,
        right= dim_product[['product_key','product_id']],
        how= 'left',
        left_on= 'sls_prd_key',
        right_on='product_id'
    )

    fact_sale = pd.DataFrame({
        'order_number':df['sls_ord_num'],
        'product_key':df['product_key'],
        'customer_key': df['customer_key'],
        'order_date': df['sls_order_dt'],
        'sls_ship_dt': df['sls_ship_dt'],
        'due_date': df['sls_due_dt'],
        'unit_price': df['sls_price'],
        'quantity': df['sls_quantity'],
        'sales_amount': df['sls_sales']
    })

    fact_sale.insert(0, 'order_key', fact_sale.index+1)

    print(fact_sale.head())

    cursor.execute("""               
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated')
    BEGIN
        EXEC('CREATE SCHEMA [curated]')
    END""")

    # --- DROP AND CREATE ---
    cursor.execute("IF OBJECT_ID('curated.dim_customer', 'U') IS NOT NULL DROP TABLE curated.dim_customer")
    cursor.execute("""
        CREATE TABLE curated.dim_customer (
            customer_key   INT PRIMARY KEY,
            customer_id    INT,
            first_name     NVARCHAR(100),
            last_name      NVARCHAR(100),
            country        NVARCHAR(100),
            marital_status NVARCHAR(50),
            gender         NVARCHAR(20),
            birth_date     DATE,
            create_date    DATE
        )
    """)

    # --- PREPARE DATA ---
    # Ensure columns match the SQL table order exactly
    cols_cust = ['customer_key', 'customer_id', 'fitst_name', 'last_name', 'country', 'marital_status', 'gender', 'birth_date', 'create_date']
    df_cust_load = dim_customer[cols_cust].where(dim_customer[cols_cust].notnull(), None)
    data_cust = [tuple(x) for x in df_cust_load.values]

    # --- INSERT ---
    insert_cust = "INSERT INTO curated.dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.fast_executemany = True
    cursor.executemany(insert_cust, data_cust)
    conn.commit()

    # --- DROP AND CREATE ---
    cursor.execute("IF OBJECT_ID('curated.dim_product', 'U') IS NOT NULL DROP TABLE curated.dim_product")
    cursor.execute("""
        CREATE TABLE curated.dim_product (
            product_key   INT PRIMARY KEY,
            product_id    NVARCHAR(100),
            product_name  NVARCHAR(200),
            category_id   NVARCHAR(100),
            subcategory   NVARCHAR(100),
            maintenance   NVARCHAR(50),
            product_cost  DECIMAL(18, 4),
            product_line  NVARCHAR(50),
            start_date    DATE,
            end_date      DATE
        )
    """)

    # --- PREPARE DATA ---
    cols_prd = ['product_key', 'product_id', 'product_name', 'category_id', 'subcategory', 'maintenance', 'product_cost', 'product_line', 'start_date', 'end_date']
    df_prd_load = dim_product[cols_prd].where(dim_product[cols_prd].notnull(), None)
    data_prd = [tuple(x) for x in df_prd_load.values]

    # --- INSERT ---
    insert_prd = "INSERT INTO curated.dim_product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.fast_executemany = True
    cursor.executemany(insert_prd, data_prd)
    conn.commit()

    # --- DROP AND CREATE ---
    cursor.execute("IF OBJECT_ID('curated.fact_sales', 'U') IS NOT NULL DROP TABLE curated.fact_sales")
    cursor.execute("""
        CREATE TABLE curated.fact_sales (
            order_key      INT PRIMARY KEY,
            order_number   NVARCHAR(100),
            product_key    INT,
            customer_key   INT,
            order_date     DATE,
            ship_date      DATE,
            due_date       DATE,
            unit_price     DECIMAL(18, 4),
            quantity       INT,
            sales_amount   DECIMAL(18, 4)
        )
    """)

    # --- PREPARE DATA ---
    # Assuming your sales dataframe is named 'sales_df'
    cols_sales = ['order_key','order_number', 'product_key', 'customer_key', 'order_date', 'sls_ship_dt', 'due_date', 'unit_price', 'quantity', 'sales_amount']
    df_sales_load = fact_sale[cols_sales].where(fact_sale[cols_sales].notnull(), None)
    data_sales = [tuple(x) for x in df_sales_load.values]

    # --- INSERT ---
    insert_sales = "INSERT INTO curated.fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.fast_executemany = True
    cursor.executemany(insert_sales, data_sales)
    conn.commit()

    print("Success!") 


    conn.close()


if __name__ == "__main__":
    run()