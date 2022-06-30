
# # main extract schema function
def extractSchema(cursor,schema):
  tables = fetch_table(cursor,schema)

  tNo = getTableNumber(tables)

  pks = fetch_all_PKs(cursor,tables)

  relations = fetch_relations(cursor,tables, schema)
  
  return [tables,tNo,pks,relations]



# # function to get table numbers
def getTableNumber(tables):
  tNo={}
  i=0
  for t in tables:
      tNo[t]=i
      i=i+1
  return tNo



# function to fetch pk of given table
def fetch_PK(cursor,tableName) :
  cursor.execute("""SELECT  distinct c.column_name, c.data_type
    FROM information_schema.table_constraints tc 
    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
    WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = \'{}\';
    """.format(tableName))
  ans=cursor.fetchall()  
  pk = []
  for cols in ans :
    pk.append(cols[0])
  return pk



# function to fetch pks of all tables
def fetch_all_PKs(cursor,tables) :
    ans={}
    for i in tables:
      ans[i]=[]
    for i in tables:
      t=i
      extract_info=fetch_PK(cursor,t)
      ans[t] = extract_info
    return ans



# function to fetch fk of given table
def fetch_FK(cursor,tableName, schema):
  cursor.execute("""select * 
      from (select distinct
      (select r.relname from pg_class r where r.oid = c.conrelid) as table, 
      (select array_agg(attname) from pg_attribute 
      where attrelid = c.conrelid and ARRAY[attnum] <@ c.conkey) as col, 
      (select r.relname from pg_class r where r.oid = c.confrelid) as ftable 
      from pg_constraint c 
      where c.confrelid in (select oid from pg_class where relname = \'{}\')) as imp
	    where imp.table in (select table_name from information_schema.tables where table_schema = \'{}\') 
	    and imp.ftable in (select table_name from information_schema.tables where table_schema = \'{}\') ;
      """.format(tableName, schema, schema))
  ans=cursor.fetchall()  
  return ans



# function to fetch fks of all tables    
def fetch_relations(cursor,tables, schema):
    ans={}
    for i in tables:
      ans[i]=[]
    for i in tables:
      t=i
      extract_info=fetch_FK(cursor,t, schema)

      if(len(extract_info)):
        for elements in extract_info:
          ans[t].append([elements[0],elements[1]])
    return ans



# function to fetch all the tables
def fetch_table(cursor,schema):
  cursor.execute("""SELECT table_name, table_schema
                      FROM information_schema.tables
                      WHERE table_schema = \'{}\' AND table_type='BASE TABLE'
                      """.format(schema))
  myresult = (cursor.fetchall())
  ans=[]
  ans=[i[0] for i in myresult]
  return ans