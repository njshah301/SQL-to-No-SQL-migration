import pywebio.output as pwo
from pywebio.input import *
from home import *
import pywebio.input as pwi
import pandas as pd
import subprocess as sp
import os
from main import *


def home_page():
    # import front
    import pywebio.output as pwo
    from pywebio.input import input
    pwo.clear() 
    pwo.put_markdown(""" # SQL to No-SQL migration""")
    
    arr = input_group("Fill Following fields to migrate",[
    # input('Connection String', name='connection_string'),
    input('Database Name', name='Dbname'),
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
    

    isSuccessful=main(arr['Dbname'],arr['schema'],arr['mongoHost'],arr['mongo_name'],paths)
    if(isSuccessful==-1):
          pwo.clear()
          pwo.popup('Error','There is an error.')
          img=open("something-went-wrong_f.gif",'rb').read()
          pwo.put_image(img)
          pwo.put_button('Migrate Another',onclick=home_page)

    else:
        pwo.clear()
        
        pwo.popup('Successful migration','👍🥳🔥Migration done successfully.🔥🥳👍')
        img=open("inset_successful_f.gif",'rb').read()
        pwo.put_image(img)
        pwo.put_button('Migrate Another',onclick=home_page)
    while(True):
        pass
    
if __name__ == '__main__':
    home_page()
