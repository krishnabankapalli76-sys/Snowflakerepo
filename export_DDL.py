import os
import sys
import csv
import oracledb
from datetime import datetime

# ==========================================
# 1. AUTO-GENERATE FILENAME & PATH
# ==========================================
# Target folder where files will be stored
OUTPUT_DIRECTORY = r"C:\Users\BKrishna\Downloads\SNOWFLAKE_Project"

# Generates a timestamp string like: "20260624_120930" (YYYYMMDD_HHMMSS)
timestamp = datetime.now().strftime("%Y%m%m_%H%M%S")

# Combines folder, a descriptive prefix, and the timestamp into a unique filename
FILENAME = f"oracle_extract_{timestamp}.csv"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIRECTORY, FILENAME)

# ==========================================
# 2. ENVIRONMENT CONFIGURATION
# ==========================================
ORACLE_CLIENT_DIR = r"C:\oraclexe\app\oracle\instantclient_23_0" 
DB_USER = "APP_USER"
DB_PASSWORD = "1234"
DB_DSN = "localhost:1521/xe"

# Complex SQL query string 
SQL_QUERY = f"""createtable EMPLOYEE_{datetime.now().strftime("%Y%m%m")} AS
select EMPNO,ENAME,JOB,MGR,HIREDATE,SALARY,COMM,DEPTNO from EMPLOYEE
"""

SQL_QUERY2 = f"""
select EMPNO,ENAME,JOB,MGR,HIREDATE,SALARY,COMM,DEPTNO from EMPLOYEE_{datetime.now().strftime("%Y%m%m")}
where DEPTNO=:1 AND JOB=:2
"""


# ==========================================
# 3. INITIALIZE THICK MODE
# ==========================================
try:
    if sys.platform.startswith("win"):
        oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_DIR)
    else:
        oracledb.init_oracle_client()
    print("SUCCESS: Oracle Thick Mode initialized.")
except Exception as e:
    print(f"ERROR: Thick Mode Init Failed: {e}")
    sys.exit(1)

# ==========================================
# 4. CONNECT, EXECUTE AND BATCH EXTRACT
# ==========================================
try:
    # Auto-create directory structure if missing
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    print("Connecting to local Oracle Database...")
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as connection:
        with connection.cursor() as cursor:
            
            cursor.arraysize = 5000
            print("Executing query...")
            #cursor.execute(SQL_QUERY)
            #connection.commit()

            cursor.execute(SQL_QUERY2,[20,'ANALYST']) 
            # Open the newly auto-generated unique path
            print(f"Writing data to: {OUTPUT_FILE_PATH}")
            with open(OUTPUT_FILE_PATH, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                
                # Dynamic column header cleaner logic
                if cursor.description:
                    raw_headers = [column_meta[0] for column_meta in cursor.description]
                    
                    # Cleans Oracle syntax patterns out of the header name if present
                   # clean_headers = [
                    #    h.replace("||','||", ",").replace("||", "") for h in raw_headers
                    #]
                    
                    # Handle edge cases where Oracle returns raw concatenated string as a single item
                  #  if len(clean_headers) == 1 and "," in clean_headers[0]:
                   #     clean_headers = clean_headers[0].split(",")
                        
                    writer.writerow(raw_headers)
                
                # Safe data stream processing
                row_count = 0
                while True:
                    rows = cursor.fetchmany(size=5000)
                    if not rows:
                        break
                    writer.writerows(rows)
                    row_count += len(rows)
                
    print(f"SUCCESS: Created file '{FILENAME}' with {row_count} rows!")

except Exception as e:
    if 'connection' in locals():
            connection.rollback()
            print("CRITICAL ERROR: Transaction rolled back due to error.")
    print(f"CRITICAL ERROR during execution: {e}")
