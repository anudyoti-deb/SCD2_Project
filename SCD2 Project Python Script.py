#importing all modules & libraries
import pandas as pd
import sqlalchemy as sal
import cx_Oracle
from sqlalchemy.dialects.oracle import (
                                        FLOAT,
                                        NUMBER,
                                        VARCHAR2,
                                        DATE
                                        )

#connecting to database (oracle)
engine = sal.create_engine('oracle+oracledb://system:admin@localhost:1521/?service_name=XEPDB1')
conn = engine.connect()

#-----------------------
#defining functions

# 1. extract() function :
#it pulls data from both the tables into python pandas ecosystem i.e. input data table and database table
def extract():

    #EXTRACTING & FILTERING EXTERNAL TABLE
    #to fetch the item_id of the model that needs to be removed from the incoming table before processing further.
    #First fetch both the tables.
    df_lcl = pd.read_csv('new_items.txt')
    df_db = pd.read_sql_query("select * from items where expiry_date = '31-12-49'",conn)

    #concatenate item_id & item_price column on both the tables.
    df_db ['concat'] = df_db['item_id'] + df_db['item_price']
    df_lcl ['concat'] = df_lcl['item_id'] + df_lcl['item_price']
    
    #get the item_id & the new concat column from both the table.
    df_db1 = df_db[['item_id','concat',]]
    df_lcl1 = df_lcl[['item_id','concat',]]
    #this will make both the table structure same in order to concatenate
    
    #concat both the temp df vertically on a new temp df.
    df1 = pd.concat([df_db1,df_lcl1])
    
    #create a filter to keep only the duplicate values
    fc = df1.duplicated(keep=False)
    # assign to a new df
    df2 = df1[fc]
    
    #keep the unique values only from the last df
    a = df2['item_id'].unique()
    
    #convert the array into list
    b = a.tolist()
    
    # #finally remove the row from the main df fetched from external table.
    df_lcl0 = pd.read_csv('new_items.txt')
    c = df_lcl0[df_lcl0['item_id'].isin(b)].index
    df_lcl = df_lcl0.drop(c)
    df_lcl

    #EXTRACTING & FILTERING DATABASE TABLE
    df_db = pd.read_sql_query("select * from items where expiry_date = '31-12-49'",conn)
    
    return df_lcl,df_db

#2. insert() function : 
#it takes the entire external file as input extracted from the extract() function.
#adds 2 new columns with name effective_date & expiry_date.
#adds values into those columns for each row. current date & a fixed end data  '31-12-49'
#inserts & appends the entire dataframe with 5 columns now into the database table 'items'
#the first column is not added as the sql will auto generte key as per sequence.
def inserts(df_lcl):
    df_lcl['effective_date'] = pd.to_datetime('now').strftime('%d-%m-%y')
    df_lcl['expiry_date'] = '31-12-49'
    df_lcl.to_sql('items',con=conn, index= False,if_exists = 'append')
    conn.commit()

#3. transform() function :
#it will take the updated df of external file with 5 columns coming from insert() function.
#it will merge both the tables.external table & database table based on item id.
#since all the data from external table was already inserted based on previous function,the database table will already have rows that need to be updated.
#based on this we apply inner join to filter out only the data that are present on external file and exclude those that are not in external file.
#coz we need to update only the records of item_ids that are incoming. Remaining item_ids wont be touched as there is no update.
#post merging we will fetch the item keys of those updated items and put it into the next updates() function.
def transform(df_lcl,df_db):
    df_merged = pd.merge(df_lcl, df_db, how='inner',on = 'item_id')
    update_rows = df_merged['item_key']
    keys = update_rows.to_list()
    item_keys = ','.join([str(key) for key in keys])
    return item_keys
    

#4. updates() funnction :
#it will update the expiry date to previous date for the previous active items based on the item_keys
def updates(item_keys):
    query = sal.text("update items set expiry_date = (select to_char(current_date-1,'DD-MM-YY') from dual) where item_key in (" + item_keys + ")")
    p = conn.execute(query)
    conn.commit()

#------------------------------

#executinng all functions
    #extract
df_lcl,df_db = extract()

#insert
inserts(df_lcl)

#transform
item_keys = transform(df_lcl,df_db)
item_keys

#update
#to avoid error in sql. has to be at least one value to complete the query.
if item_keys != '':
    updates(item_keys)
