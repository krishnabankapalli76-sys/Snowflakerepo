import oracledb
import getpass
import sys
import os 
import csv

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

un = "APP_USER"
cs = "localhost:1521/xe"

with oracledb.connect(user=un, password='1234', dsn=cs) as connection:
    with connection.cursor() as cursor:
        sql = "select * from EMPLOYEE"
        for r in cursor.execute(sql):
            print(r)