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


# preprocess to aviod datatype violation error
def pre_process(cursor, result, table_schema, table_name):
    cursor.execute("""set search_path to \'{}\';""".format(table_schema))
    cursor.execute("SELECT  column_name FROM information_schema.columns WHERE table_name = \'{}\' and (data_type='date' or data_type='numeric' or data_type='Decimal' or data_type='time without time zone')".format(table_name))
    ans = []
    ans = cursor.fetchall()

    for check in result:
        for col in ans:
            check[col[0]] = (str)(check[col[0]])


# function to migrate a given tables
def migrate_table(mongodbname,myClient, cursor, data, table_name, table_schema):

    pre_process(cursor, data, table_schema, table_name)

    mydb = myClient[mongodbname]
    mycol = mydb[table_name]

    mycol.delete_many({})    # delete existing documents

    x = mycol.insert_many(data)  # insert the documents

    return len(x.inserted_ids)


# main function to migrate all data from postgres to mongodb
def migrateAll(mongodbname,myClient, schema, cursor, data, tNo, currCollec):
    success_count = 0
    fail_count = 0
    total_count = len(currCollec)
    for c in currCollec:
        try:
            print(f"{bcolors.OKCYAN}Processing table: {c}...{bcolors.ENDC}")
            inserted_count = migrate_table(
                mongodbname,myClient, cursor, data[tNo[c]], c, schema)
            print(
                f"{bcolors.OKGREEN}Processing table: {c} completed. {inserted_count} documents inserted.{bcolors.ENDC}")
            success_count += 1
        except Exception as e:
            print(f"{bcolors.FAIL} {e} {bcolors.ENDC}")
            fail_count += 1

    print("")
    print("Migration completed.")

    print(f"{bcolors.OKGREEN}{success_count} of {total_count} tables migrated successfully.{bcolors.ENDC}")

    if fail_count > 0:
        print(
            f"{bcolors.FAIL}Migration of {fail_count} tables failed. See errors above.{bcolors.ENDC}")
