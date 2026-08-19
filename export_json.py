import os
import sys
import csv
import json
import oracledb
from datetime import datetime

# ==========================================
# 1. AUTO-GENERATE FILENAME & PATH
# ==========================================
# Target folder where files will be stored


# Generates a timestamp string like: "20260624_120930" (YYYYMMDD_HHMMSS)
timestamp = datetime.now().strftime("%Y%m%m_%H%M%S")

# Combines folder, a descriptive prefix, and the timestamp into a unique filename
FILENAME = f"oracle_extract_{timestamp}.json"

OUTPUT_DIRECTORY = r"C:\Users\BKrishna\Downloads\SNOWFLAKE_Project"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIRECTORY, FILENAME)
# ==========================================
# 2. ENVIRONMENT CONFIGURATION
# ==========================================
ORACLE_CLIENT_DIR = r"C:\oraclexe\app\oracle\instantclient_23_0" 
DB_USER = "APP_USER"
DB_PASSWORD = "1234"
DB_DSN = "localhost:1521/xe"

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
            
            #cursor.arraysize = 5000
            print("Executing query...")
            #cursor.execute(SQL_QUERY)
            #connection.commit()
            out_cursor_var = cursor.var(oracledb.CURSOR)
            procedure_params = ['10', out_cursor_var]
            PROCEDURE_NAME='GET_EMP_DATA'
            print(f"calling Proc: {PROCEDURE_NAME}")
            cursor.callproc(PROCEDURE_NAME, procedure_params)
            print(f"complete calling Proc")
            ref_cursor = out_cursor_var.getvalue()
            print(f"complete calling Proc 1112727")
            ref_cursor.arraysize = 5000
            print(f"complete calling Proc 999999")
            # Open the newly auto-generated unique path
            print(f"Writing data to: {OUTPUT_FILE_PATH}")
            # with open(OUTPUT_FILE_PATH, mode="w", newline="", encoding="utf-8") as csv_file:
            #with open(OUTPUT_FILE_PATH, mode="w", encoding="utf-8") as file:
            #     print(f"open data to: {OUTPUT_FILE_PATH}")
                #writer = csv.writer(csv_file)
            raw_rows = ref_cursor.fetchmany(5000) 
            column_headers = [col[0].lower() for col in ref_cursor.description]
            json_compatible_data = [dict(zip(column_headers, row)) for row in raw_rows]
            
            with open(OUTPUT_FILE_PATH, 'w') as file:
                json.dump(json_compatible_data, file, indent=4, default=str)     
                #json.dump(json_compatible_data, file, indent=4)
                
                # Dynamic column header cleaner logic
              #  if ref_cursor.description:
               #     raw_headers = [column_meta[0] for column_meta in ref_cursor.description]
                    
                    # Cleans Oracle syntax patterns out of the header name if present
                   # clean_headers = [
                    #    h.replace("||','||", ",").replace("||", "") for h in raw_headers
                    #]
                    
                    # Handle edge cases where Oracle returns raw concatenated string as a single item
                  #  if len(clean_headers) == 1 and "," in clean_headers[0]:
                   #     clean_headers = clean_headers[0].split(",")
                        
                #    writer.writerow(raw_headers)
                
                # Safe data stream processing
                #row_count = 0
                #while True:
                    #rows = ref_cursor.fetchmany(size=5000)
                    #if not rows:
                     #   break


                    #writer.writerows(rows)
                    #row_count += len(rows)
       # row_count += ref_cursor.count
    print(f"SUCCESS: Created file '{FILENAME}' ")

except Exception as e:
    
    print(f"CRITICAL ERROR during execution: {e}")
  