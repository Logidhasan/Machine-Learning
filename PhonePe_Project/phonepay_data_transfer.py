import mysql.connector
import pandas as pd
import plotly.express as px                         #pip install plotly_express==0.4.0
import requests
import json
from PIL import Image
import os
#sql connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()


#aggregated insurance data


localpath1 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/aggregated/insurance/country/india/state/"
insurance = os.listdir (localpath1)     #Access the path, if we print the transaction it will show the list of states


First_coll = {"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Insurance_count":[], "Insurance_amount":[]}


for state in insurance:                 #Accessing each states seperatly and get the file
    states = localpath1+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)  #till Json file we got access
        
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            insurance_list = json.load(data)   


            for A in insurance_list ["data"]["transactionData"]:
                name = A["name"]
                count = A["paymentInstruments"][0]["count"]
                amount = A["paymentInstruments"][0]["amount"]
                First_coll["Transaction_type"].append(name)
                First_coll["Insurance_count"].append(count)
                First_coll["Insurance_amount"].append(amount)
                First_coll["States"].append(state)
                First_coll["Years"].append(year)
                First_coll["Quarter"].append(int(files.strip(".json")))

Total_insurance_data = pd.DataFrame(First_coll)

Total_insurance_data ["States"] = Total_insurance_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")

Total_insurance_list = Total_insurance_data.values.tolist()       #(converting dataframe into list)

#Transfering data to mysql

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists aggregated_insurance (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Insurance_count bigint,
                                                                      Insurance_amount bigint
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


agg_insur = """insert into aggregated_insurance values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(agg_insur,Total_insurance_list)
mydb.commit()
#aggregated Transaction data

localpath2 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/aggregated/transaction/country/india/state/"
Transaction = os.listdir (localpath2)  


Second_coll = {"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}


for state in Transaction:                 #Accessing each states seperatly and get the file
    states = localpath2+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access

    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            Transaction_list = json.load(data)
            for B in Transaction_list ["data"]["transactionData"]:
                name = B["name"]
                count = B["paymentInstruments"][0]["count"]
                amount = B["paymentInstruments"][0]["amount"]

                Second_coll["Transaction_type"].append(name)
                Second_coll["Transaction_count"].append(count)
                Second_coll["Transaction_amount"].append(amount)
                Second_coll["States"].append(state)
                Second_coll["Years"].append(year)
                Second_coll["Quarter"].append(int(files.strip(".json")))

Total_transaction_data = pd.DataFrame(Second_coll)

Total_transaction_data ["States"] = Total_transaction_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")

Total_transaction_list = Total_transaction_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


agg_trans = """insert into aggregated_transaction values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(agg_trans,Total_transaction_list)
mydb.commit()
#aggregated User data

localpath3 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/aggregated/user/country/india/state/"
User = os.listdir (localpath3)  


Third_coll = {"States":[], "Years":[], "Quarter":[], "Brands":[], "Counts":[], "Total_Percentage":[]}


for state in User:                 #Accessing each states seperatly and get the file
    states = localpath3+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files, "r")  
            user_list = json.load(data) 

            
            try:                                                            #usersByDevice contain None value hence try and except block used to ignore that none value
                for C in user_list ["data"]["usersByDevice"]:
                    brand = C["brand"]
                    count = C["count"]
                    percentage = C["percentage"]

                    Third_coll["Brands"].append(brand)
                    Third_coll["Counts"].append(count)
                    Third_coll["Total_Percentage"].append(percentage)
                    Third_coll["States"].append(state)
                    Third_coll["Years"].append(year)
                    Third_coll["Quarter"].append(int(files.strip(".json")))
            except:
                pass

Total_user_data = pd.DataFrame(Third_coll)  
Total_user_data ["States"] = Total_user_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")

Total_user_list = Total_user_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists aggregated_user(States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Brands varchar(50),
                                                                      Counts bigint,
                                                                      Total_Percentage float
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


agg_user = """insert into aggregated_user values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(agg_user,Total_user_list)
mydb.commit() 

#map insurance data

map_path1 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/map/insurance/hover/country/india/state/"
insurance = os.listdir (map_path1) 


First_collumn = {"States":[], "Years":[], "Quarter":[], "District":[], "Insurance_count":[], "Insurance_amount":[]}


for state in insurance:                 #Accessing each states seperatly and get the file
    states = map_path1+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            insurance_list = json.load(data)
            for A in insurance_list ["data"]["hoverDataList"]:
                name = A["name"]
                count = A["metric"][0]["count"]
                amount = A["metric"][0]["amount"]
                First_collumn["District"].append(name)
                First_collumn["Insurance_count"].append(count)
                First_collumn["Insurance_amount"].append(amount)
                First_collumn["States"].append(state)
                First_collumn["Years"].append(year)
                First_collumn["Quarter"].append(int(files.strip(".json")))

Distric_insurance_data = pd.DataFrame(First_collumn)
Distric_insurance_data ["States"]= Distric_insurance_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")
Distric_insurance_data ["District"]= Distric_insurance_data ["District"].str.capitalize().str.replace('-', " ").str.replace('district', "")

Dist_insurance_list = Distric_insurance_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists map_insurance (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Insurance_count bigint,
                                                                      Insurance_amount float
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


map_insur = """insert into map_insurance values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(map_insur,Dist_insurance_list)
mydb.commit()
#map Transaction data

map_path2 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/map/transaction/hover/country/india/state/"
Transaction = os.listdir (map_path2)  


Second_collumn = {"States":[], "Years":[], "Quarter":[], "District":[], "Transaction_count":[], "Transaction_amount":[]}


for state in Transaction:                 #Accessing each states seperatly and get the file
    states = map_path2+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            Transaction_list = json.load(data)
            for B in Transaction_list ["data"]["hoverDataList"]:
                name = B["name"]
                count = B["metric"][0]["count"]
                amount = B["metric"][0]["amount"]
                Second_collumn["District"].append(name)
                Second_collumn["Transaction_count"].append(count)
                Second_collumn["Transaction_amount"].append(amount)
                Second_collumn["States"].append(state)
                Second_collumn["Years"].append(year)
                Second_collumn["Quarter"].append(int(files.strip(".json")))

Distric_Transaction_data = pd.DataFrame(Second_collumn)

Distric_Transaction_data ["States"]= Distric_Transaction_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")
Distric_Transaction_data ["District"]= Distric_Transaction_data ["District"].str.capitalize().str.replace('-', " ").str.replace('district', "")


Distric_Transaction_list = Distric_Transaction_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount float
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


map_trans = """insert into map_transaction values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(map_trans,Distric_Transaction_list)
mydb.commit() 
#map User data

map_path3 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/map/user/hover/country/india/state/"
User = os.listdir (map_path3)  


Third_colloumn = {"States":[], "Years":[], "Quarter":[], "District":[], "Registered_Users":[], "AppOpens":[]}


for state in User:                 #Accessing each states seperatly and get the file
    states = map_path3+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files, "r")  
            dist_user_list = json.load(data) 
            for C in dist_user_list ["data"]["hoverData"].items():

                    name = C[0]
                    user = C[1]["registeredUsers"]
                    app = C[1]["appOpens"]

                    Third_colloumn["District"].append(name)
                    Third_colloumn["Registered_Users"].append(user)
                    Third_colloumn["AppOpens"].append(app)
                    Third_colloumn["States"].append(state)
                    Third_colloumn["Years"].append(year)
                    Third_colloumn["Quarter"].append(int(files.strip(".json")))

map_user_data = pd.DataFrame(Third_colloumn)

map_user_data ["States"]= map_user_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")
map_user_data ["District"]= map_user_data ["District"].str.capitalize().str.replace('-', " ").str.replace('district', "")

map_user_list = map_user_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists map_user (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Registered_Users bigint,
                                                                      AppOpens bigint
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


map_users = """insert into map_user values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(map_users,map_user_list)
mydb.commit()   
#top insurance data

top_path1 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/top/insurance/country/india/state/"
insurance = os.listdir (top_path1) 


First_collumn_top = {"States":[], "Years":[], "Quarter":[], "District":[], "Insurance_type":[], "Insurance_count":[], "Insurance_amount":[]}


for state in insurance:                 #Accessing each states seperatly and get the file
    states = top_path1+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            top_insur = json.load(data)
            for A in top_insur ["data"]['districts']:
                    dist = A['entityName']
                    Type = A['metric']['type']
                    count = A['metric']['count']
                    amount = A['metric']['amount']
                    First_collumn_top["District"].append(dist)
                    First_collumn_top["Insurance_type"].append(Type)
                    First_collumn_top["Insurance_count"].append(count)
                    First_collumn_top["Insurance_amount"].append(amount)
                    First_collumn_top["States"].append(state)
                    First_collumn_top["Years"].append(year)
                    First_collumn_top["Quarter"].append(int(files.strip(".json")))

Top_insurance_data = pd.DataFrame(First_collumn_top)

Top_insurance_data.drop('Insurance_type', axis=1, inplace=True)   #Insurance_type uniqe value (Total) provided for all rows, hence droped

Top_insurance_data ["States"]= Top_insurance_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")

Top_insurance_data ["District"]= Top_insurance_data ["District"].str.capitalize().str.replace('-', " ")


Top_insurance_list = Top_insurance_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists top_insurance (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Insurance_count bigint,
                                                                      Insurance_amount float
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


top_insur = """insert into top_insurance values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(top_insur,Top_insurance_list)
mydb.commit()
#top Transaction data

top_path2 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/top/transaction/country/india/state/"
Transaction = os.listdir (top_path2)  


Top_Second_collumn = {"States":[], "Years":[], "Quarter":[], "District":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}


for state in Transaction:                 #Accessing each states seperatly and get the file
    states = top_path2+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files,"r")  
            Transaction_list = json.load(data)
            for B in Transaction_list ["data"]["districts"]:
                dist = B["entityName"]
                type = B["metric"]['type']
                count = B["metric"]["count"]
                amount = B["metric"]["amount"]
                Top_Second_collumn["District"].append(dist)
                Top_Second_collumn["Transaction_type"].append(type)
                Top_Second_collumn["Transaction_count"].append(count)
                Top_Second_collumn["Transaction_amount"].append(amount)
                Top_Second_collumn["States"].append(state)
                Top_Second_collumn["Years"].append(year)
                Top_Second_collumn["Quarter"].append(int(files.strip(".json")))

Top_Transaction_data = pd.DataFrame(Top_Second_collumn)

Top_Transaction_data.drop('Transaction_type', axis=1, inplace=True)   #Transaction_type uniqe value (Total) provided for all rows, hence droped

Top_Transaction_data ["States"]= Top_Transaction_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")

Top_Transaction_data ["District"]= Top_Transaction_data ["District"].str.capitalize().str.replace('-', " ")

Top_Transaction_list = Top_Transaction_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount float
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


top_insur = """insert into top_transaction values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(top_insur,Top_Transaction_list)
mydb.commit()       

#top User data

top_path3 = "E:/Guvi AI and ML/Capstone Projects/Phonepay/phonepay/pulse/data/top/user/country/india/state/"
User = os.listdir (top_path3)  


top_third_colloumn = {"States":[], "Years":[], "Quarter":[], "District":[], "Registered_Users":[], "AppOpens":[]}


for state in User:                 #Accessing each states seperatly and get the file
    states = map_path3+state+"/"             #"/" is used to access all the folders automaticly
    years = os.listdir (states)             #till years we got access
    for year in years:
        year_list = states+year+"/"
        file = os.listdir(year_list)
        for files in file:
            total_files = year_list+files
            data = open(total_files, "r")  
            top_user_list = json.load(data) 
            for C in dist_user_list ["data"]["hoverData"].items():
                dist = C[0]
                user = C[1]["registeredUsers"]
                app = C[1]["appOpens"]
                top_third_colloumn["District"].append(dist)
                top_third_colloumn["Registered_Users"].append(user)
                top_third_colloumn["AppOpens"].append(app)
                top_third_colloumn["States"].append(state)
                top_third_colloumn["Years"].append(year)
                top_third_colloumn["Quarter"].append(int(files.strip(".json")))

top_user_data = pd.DataFrame(top_third_colloumn)
 
top_user_data ["States"]= top_user_data ["States"].str.capitalize().str.replace('-', " ").str.replace('islands', "")
top_user_data ["District"]= top_user_data ["District"].str.capitalize().str.replace('-', " ").str.replace('district', "")
top_user_list = top_user_data.values.tolist()


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="XXXXXXXX",
  database="phonepay"
)
mycursor = mydb.cursor()

#aggregated insurance table
create_table= '''CREATE TABLE if not exists top_user (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      District varchar(50),
                                                                      Registered_Users bigint,
                                                                      AppOpens bigint
                                                                      )'''
mycursor.execute(create_table)
mydb.commit()


top_insur = """insert into top_user values (%s, %s, %s, %s, %s, %s)"""

mycursor.executemany(top_insur,top_user_list)
mydb.commit()
