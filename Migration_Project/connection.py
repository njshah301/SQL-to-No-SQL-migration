import psycopg2
import psycopg2.extras
import pymongo
import ssl
from bson.json_util import dumps, loads
import datetime
import json

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

def connectDB(dbName,mongodb_host):
    
    (f"{bcolors.HEADER}Initializing database connections...{bcolors.ENDC}")
    
    #Postgres connection
    print(f"{bcolors.HEADER}Connecting to PostgreSQL server...{bcolors.ENDC}")
    pgsqldb = psycopg2.connect(database=dbName,user="postgres",password="admin")
    cursor = pgsqldb.cursor()
    print(f"{bcolors.HEADER}Connection to Postgres db succeeded.{bcolors.ENDC}")

    #MongoDB connection
    print(f"{bcolors.HEADER}Connecting to MongoDB server...{bcolors.ENDC}")
    myClient = pymongo.MongoClient(mongodb_host,tls=True, tlsAllowInvalidCertificates=True)
    print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")
    print(f"{bcolors.HEADER}Database connections initialized successfully.{bcolors.ENDC}")  
    return [myClient,cursor]