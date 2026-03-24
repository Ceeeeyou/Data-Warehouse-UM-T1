import schedule
import time
from datetime import datetime

def run_ddl():
    print("Running ddl job")
    import DDL
    DDL.run()
    
def run_dml():
    print("Running dml job")
    import DML
    DML.run()

def run_trans_cust():
    print("Running customer job")
    import Transformation_cust_az12
    Transformation_cust_az12.run()


def run_trans_loc():
    print("Running location job")
    import Transformation_loc_a101
    Transformation_loc_a101.run()


def run_trans_px():
    print("Running product job")
    import Transformation_px_cat_g1v2
    Transformation_px_cat_g1v2.run()
    
def run_trans_cust_info():
    print("Running customer info job")
    import Transformation_cust_info
    Transformation_cust_info.run()
    
def run_trans_prd():
    print("Running product job")
    import Transformation_prd_info
    Transformation_prd_info.run()
    
def run_trans_sale():
    print("Running sale job")
    import Transformation_sales_details
    Transformation_sales_details.run()

def run_curated():
    print("Running curated job")
    import Curated
    Curated.run()


# run once immediately
run_ddl()
run_dml()
run_trans_cust()
run_trans_loc()
run_trans_px()
run_trans_cust_info()
run_trans_prd()
run_trans_sale()
run_curated()

# then schedule recurring runs
schedule.every().hour.do(run_ddl)
schedule.every().hour.do(run_dml)
schedule.every().hour.do(run_trans_cust)
schedule.every().hour.do(run_trans_cust_info)
schedule.every().hour.do(run_trans_sale)
schedule.every().day.do(run_trans_loc)
schedule.every().day.do(run_trans_px)
schedule.every().day.do(run_trans_prd)
schedule.every().day.do(run_curated)
# schedule.every().day.at("02:00").do(run_loc)
# schedule.every().day.at("03:00").do(run_px)

print("Scheduler started...")

while True:
    schedule.run_pending()
    for job in schedule.jobs:
        print(job)
    time.sleep(5)
    print('running')












































# def run_cust():
#     import transformation_erp_cust_az12
#     transformation_erp_cust_az12.run()


# def run_loc():
#     import transformation_erp_loc_a101
#     transformation_erp_loc_a101.run()


# def run_px():
#     import transformation_erp_px_cat_g1v2
#     transformation_erp_px_cat_g1v2.run()


# def main():
#     print("Starting ETL pipeline...")

#     run_cust()
#     print("Finished customer step")

#     run_loc()
#     print("Finished location step")

#     run_px()
#     print("Finished product step")

#     print("Pipeline completed successfully!")


# if __name__ == "__main__":
#     main()