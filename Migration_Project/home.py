import pywebio.output as pwo
from pywebio.input import *
from urllib.parse import urlparse
from pywebio import start_server
from pywebio.output import put_buttons, put_markdown
from home import *
import pywebio.input as pwi
import pandas as pd
import pywebio.platform
import subprocess as sp
import os
from main import *

def show_help():
    help_content = """
 
# Core Idea:
In this project, we have made an application which takes name and access path as input and can get mongoDB collections as an atlas in mongoDB compass. The algorithm can extract all the needed schema information such as fetch tables, primary key, referred table,reference table etc for given schema. After schema extraction, algorithm will embed or the bases of set of rules. After migration completed, collections are populated in mongoDB compass.

## Requirements: 
#### 1>  PostGreSQL 
#### 2> MongoDB Compass
 
## How it Works ?

### Example Database  [BasketBall Training Manangement System] 
![image](https://user-images.githubusercontent.com/58663029/176962556-5141254e-7265-4717-b614-725eda6a5b98.png)


#### We have 17 table in SQL Schema.

### Schema Diagram

![image](https://user-images.githubusercontent.com/58663029/176962667-bf4f4f28-8a2c-4e9c-ad99-97ab417d7bb0.png)


### HomePage of the Application:

![image](https://github.com/njshah301/SQL-to-No-SQL-migration/assets/58663029/5fa75648-3f62-4063-afac-329ae5a8169c)


## MongoDB Compass View:
![image](https://user-images.githubusercontent.com/58663029/176962890-6f49f266-920c-4c60-aea3-baffdbc999b2.png)


### Input Parameters:
 
![image](https://github.com/njshah301/SQL-to-No-SQL-migration/assets/58663029/33f43d05-97cc-4461-9a4e-c11067298bda)
 
 
 #### Note: Access path is a text file, which is given to the input to prioritized some collections which are frequently queried out. The migration Algorithm takes care about these acess paths.
 #### connection String Format: postgres://username:password@hostname:port/database

### Example of Acesss path for our BasketBall Training Management System:

![image](https://user-images.githubusercontent.com/58663029/176963591-0eb75e2a-2b98-4fda-84cc-0525906e03c7.png)



### Processing of the Algorithm

![image](https://user-images.githubusercontent.com/58663029/176963662-388f91c6-1191-4bc8-bbb1-bfb83cc7b63d.png)



### Migration Done Sucessfully

![image](https://user-images.githubusercontent.com/58663029/176963697-7d83bb4d-3764-4896-af84-ec8cb4a25a53.png)

Now, if we see in the MongoDB compass, Database (Sports_Training_Demo) is created automatically and migration has been done.


## Results:

### Database Created Sucessfully

![image](https://user-images.githubusercontent.com/58663029/176963965-0871c651-68fc-4313-9301-a5403eebb73a.png)

### Migration has been Done

![image](https://user-images.githubusercontent.com/58663029/176964045-6b10cb17-8542-4a96-a336-5dde3d54435d.png)

#### Note: As you can see that, in SQL we have 17 tables and in MongoDB we got 7 collections. Thus, Denormalization has been performed.

### Player Collection View

![image](https://user-images.githubusercontent.com/58663029/176964225-4f98968c-db0f-4113-8838-2604c7c0b599.png)


    """

    put_markdown(help_content)
    pwo.put_button('Go Back',onclick=home_page)


def home_page():
    # import front
    import pywebio.output as pwo
    from pywebio.input import input
    pwo.clear() 
    pwo.put_markdown(""" # SQL to No-SQL migration""")


    pwo.put_button("Help",show_help,color='success', outline=True)
    arr = input_group("Fill the following fields to migrate",[
    input('Connection String', name='connection_string'),
    input('Schema Name',name='schema'),
    input('MongoDB Host', name='mongoHost'),
    input('MongoDB Name', name='mongo_name'),
    file_upload('Choose your file for access path', name='userfile')])

    userfile=arr['userfile']
    open(userfile['filename'],'wb').write(userfile['content'])
    df=pd.read_csv(userfile['filename'])
    paths=df.to_string(index=False)
    pwo.clear()
    pwo.put_markdown(""" # SQL to No-SQL migration""")
    pwo.put_html('<h3>⏳⏳⏳Migrating data please wait...⏳⏳⏳</h3>')
    result = urlparse(arr['connection_string'])
    remote_username=result.username
    remote_password=result.password
    remote_port=result.port
    remote_host_name=result.hostname
    Dbname = result.path[1:]
    isSuccessful=main(Dbname,arr['schema'],arr['mongoHost'],arr['mongo_name'],paths,remote_username,remote_password,remote_host_name,remote_port)
    if(isSuccessful==-1):
          pwo.clear()
          pwo.popup('Error','There is an error.')
          img=open("Migration_Project/something-went-wrong_f.gif",'rb').read()
          pwo.put_image(img)
          pwo.put_button('Migrate Another',onclick=home_page)

    else:
        pwo.clear()
        
        pwo.popup('Successful migration','👍🥳🔥Migration done successfully.🔥🥳👍')
        img=open("Migration_Project/inset_successful_f.gif",'rb').read()
        pwo.put_image(img)
        pwo.put_button('Migrate Another',onclick=home_page)
    while(True):
        pass
    
if __name__ == '__main__':
    start_server(home_page, port=27010,open_webbrowser=True)


