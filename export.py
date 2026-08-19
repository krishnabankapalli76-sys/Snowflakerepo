import oracledb
#import getpass
import sys
import os 
import csv

un = "APP_USER"
cs = "localhost:1521/xe"
# 1. Initialize Thick Mode to support older legacy databases (Oracle 11g)
try:
    # If on Windows, pass the path to your Oracle installation's 'bin' directory where oci.dll lives.
    # For example: r"C:\oraclexe\app\oracle\product\11.2.0\server\bin"
    if sys.platform.startswith("win"):
        oracledb.init_oracle_client(lib_dir=r"C:\oraclexe\app\oracle\instantclient_23_0")
    else:
        # On Linux/macOS, it reads system pathing variables automatically
        oracledb.init_oracle_client()
    print("Oracle Thick Mode initialized.")
except Exception as e:
    print(f"Thick Mode Init Failed: {e}")
    sys.exit(1)


try:    
    output_dir = os.path.dirname(r"C:\Users\BKrishna\Downloads\SNOWFLAKE_Project")
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        

    with oracledb.connect(user=un, password='1234', dsn=cs) as connection:
        with connection.cursor() as cursor:
         cursor.arraysize = 5000
         print("Executing query...")
         sql = "select EMPNO,ENAME,JOB,MGR,HIREDATE,SALARY,COMM,DEPTNO from EMPLOYEE"
         print("Executing after query...")
         for r in cursor.execute(sql):
            print("Executing after query loop...")
            with open(r"C:\Users\BKrishna\Downloads\SNOWFLAKE_Project\test2.csv", mode="w", newline="", encoding="utf-8") as csv_file:

                writer = csv.writer(csv_file)
                #print(r)
                
                if cursor.description:
                    headers = [column_meta[0]for column_meta in cursor.description]
                    
                    writer.writerow(headers)
                    writer.writerows(cursor)
            
except Exception as e:
        if 'connection' in locals():
            connection.rollback()
            print("CRITICAL ERROR: Transaction rolled back due to error.")
print(f"CRITICAL ERROR during execution: {e}")      
            
          
