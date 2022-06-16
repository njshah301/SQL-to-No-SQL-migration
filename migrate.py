import psycopg2
import psycopg2.extras
import pymongo
import ssl

from bson.json_util import dumps, loads
import datetime

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

delete_existing_documents = True

begin_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script started at: {begin_time} {bcolors.ENDC}")
print(f"{bcolors.HEADER}Initializing database connections...{bcolors.ENDC}")

#MySQL connection
print(f"{bcolors.HEADER}Connecting to PostgreSQL server...{bcolors.ENDC}")

pgsqldb = psycopg2.connect(host="localhost",database="Entertainment_Booking_System",user="postgres",password="admin")

print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")

cursor = pgsqldb.cursor()

# # Query 1
# ch="select * from main_db.manager order by m_id"
# print(ch)
# cursor.execute(ch)
# rows=cursor.fetchall()
# for r in rows:
#     print(r[0],r[1],r[2],r[3])

print(f"{bcolors.HEADER}Connecting to MongoDB server...{bcolors.ENDC}")

mongodb_host = "mongodb+srv://<id>:<password>@cluster0.juxn7.mongodb.net/test"
mongodb_dbname = "pg2mongo"

myclient = pymongo.MongoClient(mongodb_host)
mydb = myclient[mongodb_dbname]

print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")
print(f"{bcolors.HEADER}Database connections initialized successfully.{bcolors.ENDC}")   

#Start migration
print(f"Migration started...")
dblist = myclient.list_database_names()
print(dblist)
if mongodb_dbname in dblist:
    print(f"The database exists.")
else:
    print(f"The database does not exist, it is being created.")  


#Iterate through the list of tables in the schema
cursor = pgsqldb.cursor()
cursor.execute("""SELECT table_schema, table_name
                      FROM information_schema.tables
                      WHERE table_schema != 'pg_catalog' AND table_schema != 'information_schema' AND table_type='BASE TABLE'
                      ORDER BY table_schema, table_name""")

tables = cursor.fetchall()
for t_name_table in tables:
    print(t_name_table)
print("------------------------------------------")

for t in tables:
    where_dict = {"table_schema": t[0], "table_name": t[1]}
    cursor.execute("""SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length
                      FROM information_schema.columns
                      WHERE table_schema = %(table_schema)s
                      AND table_name   = %(table_name)s
                      ORDER BY ordinal_position""", where_dict)
    cols = cursor.fetchall()

    for c in cols:
        print(c)
    print("---------------------------------------------")

# function starts

#Function migrate_table 
def migrate_table(db, table_name, table_schema):
    cursor = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM " + table_schema + "." + table_name + ";")
    myresult = cursor.fetchall()
    # print(myresult)
    resDict = []
    for row in myresult:
        resDict.append(dict(row))
    # print(resDict)

    mycol = mydb[table_name]
    if delete_existing_documents:
        #delete all documents in the collection
        mycol.delete_many({})

    #insert the documents
    if len(myresult) > 0:
        x = mycol.insert_many(resDict)
        return len(x.inserted_ids)
    else:
        return 0

total_count = len(tables)
success_count = 0
fail_count = 0

for table in tables:
    try:
        print(f"{bcolors.OKCYAN}Processing table: {table[1]}...{bcolors.ENDC}")
        inserted_count = migrate_table(pgsqldb, table[1], table[0])
        print(f"{bcolors.OKGREEN}Processing table: {table[1]} completed. {inserted_count} documents inserted.{bcolors.ENDC}")
        success_count += 1
    except Exception as e:
        print(f"{bcolors.FAIL} {e} {bcolors.ENDC}")
        fail_count += 1
        
print("")
print("Migration completed.")

print(f"{bcolors.OKGREEN}{success_count} of {total_count} tables migrated successfully.{bcolors.ENDC}")

if fail_count > 0:
    print(f"{bcolors.FAIL}Migration of {fail_count} tables failed. See errors above.{bcolors.ENDC}")

end_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script completed at: {end_time} {bcolors.ENDC}")
print(f"{bcolors.HEADER}Total execution time: {end_time-begin_time} {bcolors.ENDC}")