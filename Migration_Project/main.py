import psycopg2
import psycopg2.extras
import pymongo
import ssl
from bson.json_util import dumps, loads
import datetime
import json
from extractSchema import *
from migrateTable import *
from connection import *
from algo import *
from embedAndLinking import *

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'

    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'



# # fetches access paths from file
def getAccessPaths(file_paths) :
    accessPaths = []
    table = file_paths.split('\n' )

    for lines in table:
        lines=lines.strip()
        x = lines.split('/')
        accessPaths.append(x)
    return accessPaths


def pre_process(cursor,result,table_schema,table_name):
  cursor.execute("""set search_path to \'{}\';""".format(table_schema))
  cursor.execute("SELECT  column_name FROM information_schema.columns WHERE table_name = \'{}\' and (data_type='date' or data_type='numeric' or data_type='Decimal' or data_type='time without time zone')".format(table_name))
  ans=[]
  ans=cursor.fetchall()

  for check in result:
    for col in ans:
      check[col[0]]=(str)(check[col[0]])


# main function
def main(dbName, schema, mongodb_host, mongodb_dbname, file_paths):
    try:
        begin_time = datetime.datetime.now()
        print(f"{bcolors.HEADER}Script started at: {begin_time} {bcolors.ENDC}")

        # # DATABASE CONNECTIONS BEGINS  
        # dbName="Entertainment_Booking_System"
        # schema="main_db"
        # mongodb_host = "mongodb+srv://akshp:MongoDB005@cluster0.juxn7.mongodb.net/test"
        # mongodb_dbname = "pg2mongo"
        [myClient,cursor] = connectDB(dbName,mongodb_host)

        # # Get Access Paths
        print(file_paths)
        paths = getAccessPaths(file_paths)
        print(paths)
        # print(paths)

        # # SCHEMA EXTRACTION   
        [tables, tNo, pks,relations] = extractSchema(cursor,schema)

        # print("Tables are Listed Below:")
        # print(tables)
        # print()
        # print(tNo)
        # print()

        # print("Table PKS are Listed Below:")
        # print(pks)
        # print(json.dumps(pks, indent = 2))
        # print()

        # print("Table Relations are Listed Below:")
        # print("referred table : {[referring table, [FK in referring table]], ....}")
        # print(relations)
        # print(json.dumps(relations, indent = 2))
        # print()

        
        # # Algorithm starts 
        [currCollec, relax, work, adj2] = algo(tables, tNo, relations, paths)

        # print(currCollec)
        # print()
        # print(relax)
        # print()
        # print(work)
        # print()

        

        # # Embedding and Linking
        data = embedAndLinking(cursor, schema, tables, tNo, relax, pks, work, adj2, relations)

        for t in tables:
            pre_process(cursor,data[tNo[t]],schema,t)

        # # Data Migration
        migrateAll( mongodb_dbname,myClient, schema, cursor, data, tNo, currCollec)


        end_time = datetime.datetime.now()
        print(f"{bcolors.HEADER}Script completed at: {end_time} {bcolors.ENDC}")
        print(f"{bcolors.HEADER}Total execution time: {end_time-begin_time} {bcolors.ENDC}")
        return 1
    except:
        return -1

