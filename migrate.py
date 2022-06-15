import psycopg2
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

begin_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script started at: {begin_time} {bcolors.ENDC}")
print(f"{bcolors.HEADER}Initializing database connections...{bcolors.ENDC}")

#MySQL connection
print(f"{bcolors.HEADER}Connecting to PostgreSQL server...{bcolors.ENDC}")

pgsqldb = psycopg2.connect(
   database="Basketball", user='postgres', password='admin', host='127.0.0.1', port= '5432'
)

print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")


cursor = pgsqldb.cursor()
cursor.execute("select t.table_name,array_agg(c.column_name::text) as columns from information_schema.tables t inner join information_schema.columns c on t.table_name = c.table_name where t.table_schema = 'Sports_Training' and t.table_type= 'BASE TABLE'  and c.table_schema = 'Sports_Training' group by t.table_name; ")
# cursor.execute('''SELECT * FROM "Sports_Training"."Admin";''')
myresult = (cursor.fetchall())
print(myresult)

print(f"{bcolors.HEADER}Connecting to MongoDB server...{bcolors.ENDC}")

mongodb_host = "mongodb+srv://njshah301:*NILAy4564*@cluster0.lyugc.mongodb.net/test"
mongodb_dbname = "mymongodb"


myclient = pymongo.MongoClient(mongodb_host,tls=True, tlsAllowInvalidCertificates=True)



print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")

print(f"{bcolors.HEADER}Database connections initialized successfully.{bcolors.ENDC}")  
mydb=myclient["lab08"]
mycol=mydb["movies"]

query={"directors": {"$in": [ "King Vidor"]}}

x=mycol.find(query,{"title": 1, "_id":0})
print('Title of movies in which one of director is “King Vidor”')
for temp in x:
    print(temp)
#Start migration

# print(f"{bcolors.HEADER}Migration started...{bcolors.ENDC}")


# if mongodb_dbname in dblist:
#     print(f"{bcolors.OKBLUE}The database exists.{bcolors.ENDC}")
# else:
#     print(f"{bcolors.WARNING}The database does not exist, it is being created.{bcolors.ENDC}")  

#Function migrate_table 
# def migrate_table(db, col_name):
#     mycursor = db.cursor(dictionary=True)
#     mycursor.execute("SELECT * FROM " + col_name + ";")
#     myresult = mycursor.fetchall()

#     mycol = mydb[col_name]
    
#     if delete_existing_documents:
#         #delete all documents in the collection
#         mycol.delete_many({})

#     #insert the documents
#     if len(myresult) > 0:
#         x = mycol.insert_many(myresult)
#         return len(x.inserted_ids)
#     else:
#         return 0

#     #Iterate through the list of tables in the schema
# table_list_cursor = pgsqldb.cursor()
# table_list_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name LIMIT 15;", (mysql_schema,))
# tables = table_list_cursor.fetchall()

# total_count = len(tables)
# success_count = 0
# fail_count = 0

# for table in tables:
#     try:
#         print(f"{bcolors.OKCYAN}Processing table: {table[0]}...{bcolors.ENDC}")
#         inserted_count = migrate_table(pgsqldb, table[0])
#         print(f"{bcolors.OKGREEN}Processing table: {table[0]} completed. {inserted_count} documents inserted.{bcolors.ENDC}")
#         success_count += 1
#     except Exception as e:

#         print(f"{bcolors.FAIL} {e} {bcolors.ENDC}")
#         fail_count += 1
        
