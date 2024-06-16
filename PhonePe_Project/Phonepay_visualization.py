import streamlit as st
import pandas as pd
import json 
import plotly.express as px
import plotly.io as pio
from sqlalchemy import create_engine,text
from pandas.io import sql
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np
from streamlit_option_menu import option_menu
import json
import streamlit as st
import pandas as pd
import requests
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

#Fetch the data from mysql
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Password@123",
  database="phonepay"
)
mycursor = mydb.cursor(buffered=True)

#Aggregated_insurance
list1 = "SELECT * FROM aggregated_insurance"
list2 = "SELECT * FROM aggregated_transaction"
list3 = "SELECT * FROM aggregated_user"
list4 = "SELECT * FROM map_insurance"
list5 = "SELECT * FROM map_transaction"
list6 = "SELECT * FROM map_user"
list7 = "SELECT * FROM top_insurance"
list8 = "SELECT * FROM top_transaction"
list9 = "SELECT * FROM top_user"


mycursor.execute(list1)
mydb.commit()
list1 = mycursor.fetchall()

mycursor.execute(list2)
mydb.commit()
list2 = mycursor.fetchall()

mycursor.execute(list3)
mydb.commit()
list3 = mycursor.fetchall()

mycursor.execute(list4)
mydb.commit()
list4 = mycursor.fetchall()

mycursor.execute(list5)
mydb.commit()
list5 = mycursor.fetchall()

mycursor.execute(list6)
mydb.commit()
list6 = mycursor.fetchall()

mycursor.execute(list7)
mydb.commit()
list7 = mycursor.fetchall()

mycursor.execute(list8)
mydb.commit()
list8 = mycursor.fetchall()

mycursor.execute(list9)
mydb.commit()
list9 = mycursor.fetchall()

Aggregated_Insurance = pd.DataFrame(list1, columns=("States", "Years", "Quarter", "Transaction_type", "Insurance_count", "Insurance_amount"))


Aggregated_transaction = pd.DataFrame(list2, columns=("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))


Aggregated_user = pd.DataFrame(list3, columns=("States", "Years", "Quarter", "Brands", "Counts", "Total_Percentage"))




Map_Insurance = pd.DataFrame(list4, columns=("States", "Years", "Quarter", "District", "Insurance_count", "Insurance_amount"))


Map_transaction = pd.DataFrame(list5, columns=("States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"))


Map_user = pd.DataFrame(list6, columns=("States", "Years", "Quarter", "District", "Registered_Users", "AppOpens"))



Top_Insurance = pd.DataFrame(list7, columns=("States", "Years", "Quarter", "District", "Insurance_count", "Insurance_amount"))


Top_transaction = pd.DataFrame(list8, columns=("States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"))


Top_user = pd.DataFrame(list9, columns=("States", "Years", "Quarter", "District", "Registered_Users", "AppOpens"))


#Visualisation of Yearly data of Insurance count and amount

def Agg_insurance (data, year):
    
    ED = data[data["Years"] == year]
    ED.reset_index(drop= True, inplace=True)
    EDA = ED.groupby("States")[["Insurance_count", "Insurance_amount"]].sum()
    EDA = EDA.reset_index()   

    col1,col2= st.columns(2)

    with col1:

        fig_amount= px.bar(EDA, x="States", y= "Insurance_amount",title= f"{year} TRANSACTION AMOUNT",
                           width=600, height= 650, color_discrete_sequence=px.colors.sequential.Aggrnyl)
        st.plotly_chart(fig_amount)
    with col2:

        fig_count= px.bar(EDA, x="States", y= "Insurance_count",title= f"{year} TRANSACTION COUNT",
                          width=600, height= 650, color_discrete_sequence=px.colors.sequential.Bluered_r)
        st.plotly_chart(fig_count)

    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()
        

        fig_india_1= px.choropleth(EDA, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Insurance_amount", color_continuous_scale= "Sunsetdark",
                                 range_color= (EDA["Insurance_amount"].min(),EDA["Insurance_amount"].max()),
                                 hover_name= "States",title = f"{year} TRANSACTION AMOUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_1.update_geos(visible =False)
        
        st.plotly_chart(fig_india_1)

    with col2:

        fig_india_2= px.choropleth(EDA, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Insurance_count", color_continuous_scale= "Sunsetdark",
                                 range_color= (EDA["Insurance_count"].min(),EDA["Insurance_count"].max()),
                                 hover_name= "States",title = f"{year} TRANSACTION COUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_2.update_geos(visible =False)
        
        st.plotly_chart(fig_india_2)

    return ED


def Agg_insurance1(data,quarter):
    EDAI= data[data["Quarter"] == quarter]
    EDAI.reset_index(drop= True, inplace= True)

    EDAI1= EDAI.groupby("States")[["Insurance_count", "Insurance_amount"]].sum()
    EDAI1.reset_index(inplace= True)

    col1,col2= st.columns(2)

    with col1:
        fig_q_amount= px.bar(EDAI1, x= "States", y= "Insurance_amount", 
                            title= f"{EDAI['Years'].min()} AND {quarter} TRANSACTION AMOUNT",width= 600, height=650,
                            color_discrete_sequence=px.colors.sequential.Burg_r)
        st.plotly_chart(fig_q_amount)

    with col2:
        fig_q_count= px.bar(EDAI1, x= "States", y= "Insurance_count", 
                            title= f"{EDAI['Years'].min()} AND {quarter} TRANSACTION COUNT",width= 600, height=650,
                            color_discrete_sequence=px.colors.sequential.Cividis_r)
        st.plotly_chart(fig_q_count)

    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()

        fig_india_1= px.choropleth(EDAI1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Insurance_amount", color_continuous_scale= "Sunsetdark",
                                 range_color= (EDAI1["Insurance_amount"].min(),EDAI1["Insurance_amount"].max()),
                                 hover_name= "States",title = f"{EDAI['Years'].min()} AND {quarter} TRANSACTION AMOUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_1.update_geos(visible =False)
        
        st.plotly_chart(fig_india_1)
    with col2:

        fig_india_2= px.choropleth(EDAI1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Insurance_count", color_continuous_scale= "Sunsetdark",
                                 range_color= (EDAI1["Insurance_count"].min(),EDAI1["Insurance_count"].max()),
                                 hover_name= "States",title = f"{EDAI['Years'].min()} AND {quarter} TRANSACTION COUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_2.update_geos(visible =False)
        
        st.plotly_chart(fig_india_2)
    
    return EDAI

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
def Agg_transaction (data, year):
    
    trns = data[data["Years"] == year]
    trns.reset_index(drop= True, inplace=True)
    trns1 = trns.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    trns1 = trns1.reset_index()   

    col1,col2= st.columns(2)

    with col1:

        fig_amount= px.bar(trns1, x="States", y= "Transaction_amount",title= f"{year} TRANSACTION AMOUNT",
                           width=600, height= 650, color_discrete_sequence=px.colors.sequential.Aggrnyl)
        st.plotly_chart(fig_amount)
    with col2:

        fig_count= px.bar(trns1, x="States", y= "Transaction_count",title= f"{year} TRANSACTION COUNT",
                          width=600, height= 650, color_discrete_sequence=px.colors.sequential.Bluered_r)
        st.plotly_chart(fig_count)

    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()
        

        fig_india_1= px.choropleth(trns1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Transaction_amount", color_continuous_scale= "Sunsetdark",
                                 range_color= (trns1["Transaction_amount"].min(),trns1["Transaction_amount"].max()),
                                 hover_name= "States",title = f"{year} TRANSACTION AMOUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_1.update_geos(visible =False)
        
        st.plotly_chart(fig_india_1)

    with col2:

        fig_india_2= px.choropleth(trns1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Transaction_count", color_continuous_scale= "Sunsetdark",
                                 range_color= (trns1["Transaction_count"].min(),trns1["Transaction_count"].max()),
                                 hover_name= "States",title = f"{year} TRANSACTION COUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_2.update_geos(visible =False)
        
        st.plotly_chart(fig_india_2)

    return trns


def Agg_transaction1(data,quarter):
    trans= data[data["Quarter"] == quarter]
    trans.reset_index(drop= True, inplace= True)

    trans1= trans.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    trans1.reset_index(inplace= True)

    col1,col2= st.columns(2)

    with col1:
        fig_q_amount= px.bar(trans1, x= "States", y= "Transaction_amount", 
                            title= f"{trans['Years'].min()} AND {quarter} TRANSACTION AMOUNT",width= 600, height=650,
                            color_discrete_sequence=px.colors.sequential.Burg_r)
        st.plotly_chart(fig_q_amount)

    with col2:
        fig_q_count= px.bar(trans1, x= "States", y= "Transaction_count", 
                            title= f"{trans['Years'].min()} AND {quarter} TRANSACTION COUNT",width= 600, height=650,
                            color_discrete_sequence=px.colors.sequential.Cividis_r)
        st.plotly_chart(fig_q_count)

    col1,col2= st.columns(2)
    with col1:

        url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response= requests.get(url)
        data1= json.loads(response.content)
        states_name_tra= [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()

        fig_india_1= px.choropleth(trans1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Transaction_amount", color_continuous_scale= "Sunsetdark",
                                 range_color= (trans1["Transaction_amount"].min(),trans1["Transaction_amount"].max()),
                                 hover_name= "States",title = f"{trans['Years'].min()} AND {quarter} TRANSACTION AMOUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_1.update_geos(visible =False)
        
        st.plotly_chart(fig_india_1)
    with col2:

        fig_india_2= px.choropleth(trans1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                 color= "Transaction_count", color_continuous_scale= "Sunsetdark",
                                 range_color= (trans1["Transaction_count"].min(),trans1["Transaction_count"].max()),
                                 hover_name= "States",title = f"{trans['Years'].min()} AND {quarter} TRANSACTION COUNT",
                                 fitbounds= "locations",width =600, height= 600)
        fig_india_2.update_geos(visible =False)
        
        st.plotly_chart(fig_india_2)
    
    return trans

def Aggre_Transaction_type(data, state):
    df_state= data[data["States"] == state]
    df_state.reset_index(drop= True, inplace= True)

    trans= df_state.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    trans.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:

        fig_hbar_1= px.bar(trans, x= "Transaction_count", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, width= 600, 
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION COUNT",height= 500)
        st.plotly_chart(fig_hbar_1)

    with col2:

        fig_hbar_2= px.bar(trans, x= "Transaction_amount", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Greens_r, width= 600,
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION AMOUNT", height= 500)
        st.plotly_chart(fig_hbar_2)

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
def Aggre_user_plot_1(data,year):
    EDA1= data[data["Years"] == year]
    EDA1.reset_index(drop= True, inplace= True)
    
    EDA12= pd.DataFrame(EDA1.groupby("Brands")["Counts"].sum())
    EDA12.reset_index(inplace= True)

    fig_line_1= px.bar(EDA12, x="Brands",y= "Counts", title=f"{year} BRANDS AND TRANSACTION COUNT",
                    width=1000,color_discrete_sequence=px.colors.sequential.haline_r)
    st.plotly_chart(fig_line_1)

    return EDA1

def Aggre_user_plot_2(data,quarter):
    EDA2= data[data["Quarter"] == quarter]
    EDA2.reset_index(drop= True, inplace= True)

    fig_pie_1= px.pie(data_frame=EDA2, names= "Brands", values="Counts", hover_data= "Total_Percentage",
                      width=1000,title=f"{quarter} QUARTER TRANSACTION COUNT PERCENTAGE",hole=0.5, color_discrete_sequence= px.colors.sequential.Magenta_r)
    st.plotly_chart(fig_pie_1)

    return EDA2

def Aggre_user_plot_3(data,state):
    EDA3= data[data["States"] == state]
    EDA3.reset_index(drop= True, inplace= True)

    aguqyg= pd.DataFrame(EDA3.groupby("Brands")["Counts"].sum())
    aguqyg.reset_index(inplace= True)

    fig_scatter_1= px.line(aguqyg, x= "Brands", y= "Counts", markers= True,width=1000)
    st.plotly_chart(fig_scatter_1)


def map_insure_plot_1(data,state):
    EDA4= data[data["States"] == state]
    EDA4= EDA4.groupby("District")[["Insurance_count","Insurance_amount"]].sum()
    EDA4.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_bar_1= px.bar(EDA4, x= "District", y= "Insurance_amount",
                              width=600, height=500, title= f"{state.upper()} DISTRICT INSURANCE AMOUNT",
                              color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_map_bar_1)

    with col2:
        fig_map_bar_1= px.bar(EDA4, x= "District", y= "Insurance_count",
                              width=600, height= 500, title= f"{state.upper()} DISTRICT INSURANCE COUNT",
                              color_discrete_sequence= px.colors.sequential.Mint)
        
        st.plotly_chart(fig_map_bar_1)

def map_insure_plot_2(data,state):
    EDA5= data[data["States"] == state]
    EDA5= EDA5.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    EDA5.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_pie_1= px.pie(EDA5, names= "District", values= "Transaction_amount",
                              width=600, height=500, title= f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
                              hole=0.5,color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_map_pie_1)

    with col2:
        fig_map_pie_1= px.pie(EDA5, names= "District", values= "Transaction_count",
                              width=600, height= 500, title= f"{state.upper()} DISTRICT TRANSACTION COUNT",
                              hole=0.5,  color_discrete_sequence= px.colors.sequential.Oranges_r)
        
        st.plotly_chart(fig_map_pie_1)

def map_user_plot_1(data, year):
    EDA6= data[data["Years"] == year]
    EDA6.reset_index(drop= True, inplace= True)
    EDA6= EDA6.groupby("States")[["Registered_Users", "AppOpens"]].sum()
    EDA6.reset_index(inplace= True)

    fig_map_user_plot_1= px.line(EDA6, x= "States", y= ["Registered_Users","AppOpens"], markers= True,
                                width=1000,height=800,title= f"{year} REGISTERED USER AND APPOPENS", color_discrete_sequence= px.colors.sequential.Viridis_r)
    st.plotly_chart(fig_map_user_plot_1)

    return EDA6

def map_user_plot_2(data, quarter):
    EDA7= data[data["Quarter"] == quarter]
    EDA7.reset_index(drop= True, inplace= True)
    muyqg= EDA7.groupby("States")[["Registered_Users", "AppOpens"]].sum()
    muyqg.reset_index(inplace= True)

    fig_map_user_plot_1= px.line(muyqg, x= "States", y= ["Registered_Users","AppOpens"], markers= True,
                                title= f"{EDA7['Years'].min()}, {quarter} QUARTER REGISTERED USER AND APPOPENS",
                                width= 1000,height=800,color_discrete_sequence= px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_map_user_plot_1)

    return EDA7

def map_user_plot_3(data, state):
    EDA8= data[data["States"] == state]
    EDA8.reset_index(drop= True, inplace= True)
    EDA8= EDA8.groupby("District")[["Registered_Users", "AppOpens"]].sum()
    EDA8.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_user_plot_1= px.bar(EDA8, x= "Registered_Users",y= "District",orientation="h",
                                    title= f"{state.upper()} REGISTERED USER",height=800,
                                    color_discrete_sequence= px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_map_user_plot_1)

    with col2:
        fig_map_user_plot_2= px.bar(EDA8, x= "AppOpens", y= "District",orientation="h",
                                    title= f"{state.upper()} APPOPENS",height=800,
                                    color_discrete_sequence= px.colors.sequential.Rainbow)
        st.plotly_chart(fig_map_user_plot_2)

def top_user_plot_1(data,year):
    EDA9= data[data["Years"] == year]
    EDA9.reset_index(drop= True, inplace= True)

    EDA91= pd.DataFrame(EDA9.groupby(["States","Quarter"])["Registered_Users"].sum())
    EDA91.reset_index(inplace= True)

    fig_top_plot_1= px.bar(EDA91, x= "States", y= "Registered_Users", barmode= "group", color= "Quarter",
                            width=1000, height= 800, color_continuous_scale= px.colors.sequential.Burgyl)
    st.plotly_chart(fig_top_plot_1)

    return EDA9

def top_user_plot_2(data,state):
    EDA10= data[data["States"] == state]
    EDA10.reset_index(drop= True, inplace= True)

    tuysg= pd.DataFrame(EDA10.groupby("Quarter")["Registered_Users"].sum())
    tuysg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(EDA10, x= "Quarter", y= "Registered_Users",barmode= "group",
                           width=1000, height= 800,color= "Registered_Users",hover_data="AppOpens",
                            color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_plot_1)


#--------------------------------------------------------------------------------------------------------------------------------------
def ques1():
    brand= Aggregated_user[["Brands","Counts"]]
    brand1= brand.groupby("Brands")["Counts"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "Counts", names= "Brands", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands of Counts")
    return st.plotly_chart(fig_brands)

def ques2():
    lt= Aggregated_transaction[["States", "Transaction_amount"]]
    lt1= lt.groupby("States")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "States", y= "Transaction_amount",title= "LOWEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques3():
    htd= Map_transaction[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "District", title="TOP 10 DISTRICT OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def ques4():
    htd= Map_transaction[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "District", title="TOP 10 DISTRICT OF LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_htd)


def ques5():
    sa= Map_user[["States", "AppOpens"]]
    sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "States", y= "AppOpens", title="Top 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)

def ques6():
    sa= Map_user[["States", "AppOpens"]]
    sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=True)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "States", y= "AppOpens", title="lowest 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.dense_r)
    return st.plotly_chart(fig_sa)

def ques7():
    stc= Aggregated_transaction[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH LOWEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Jet_r)
    return st.plotly_chart(fig_stc)

def ques8():
    stc= Aggregated_transaction[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def ques9():
    ht= Aggregated_transaction[["States", "Transaction_amount"]]
    ht1= ht.groupby("States")["Transaction_amount"].sum().sort_values(ascending= False)
    ht2= pd.DataFrame(ht1).reset_index().head(10)

    fig_lts= px.bar(ht2, x= "States", y= "Transaction_amount",title= "HIGHEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques10():
    dt= Map_transaction[["District", "Transaction_amount"]]
    dt1= dt.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    dt2= pd.DataFrame(dt1).reset_index().head(50)

    fig_dt= px.bar(dt2, x= "District", y= "Transaction_amount", title= "DISTRICT WITH LOWEST TRANSACTION AMOUNT",
                color_discrete_sequence= px.colors.sequential.Mint_r)
    return st.plotly_chart(fig_dt)

#-------------------------------------------------------------------------------------------------------------------------------
st.set_page_config(layout= "wide")

st.title("Phonepay Pulse")
st.write("")

with st.sidebar:
    select= option_menu("Main Menu",["Home", "Data Exploration", "Top Charts"])


if select == "Home":

    col1,col2= st.columns(2)

    with col1:
        st.subheader("INDIA'S BEST TRANSACTION APP")
        st.markdown("PhonePe  is an Indian digital payments and financial technology company")
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")
    with col2:
        dsgn = Image.open("Phonepay.jpg")
        st.image(dsgn,use_column_width=True)

    col3,col4= st.columns(2)
    
    with col3:
        dsgn = Image.open("p.jpeg")
        st.image(dsgn,use_column_width=True)

    with col4:
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    col5,col6= st.columns(2)

    with col5:
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.markdown(" ")
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")

    with col6:
        dsgn = Image.open("logo.jpg")
        st.image(dsgn,use_column_width=True)

#==========================================================================================================================================
if select == "Data Exploration":
    tab1, tab2, tab3= st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:
        method = st.radio("**Select the Analysis Method**",["Insurance Analysis", "Transaction Analysis", "User Analysis"])

        if method == "Insurance Analysis":
            col1,col2= st.columns(2)
            with col1:
                years= st.slider("**Select the Year**", Aggregated_Insurance["Years"].min(), Aggregated_Insurance["Years"].max(),Aggregated_Insurance["Years"].min())

            df_agg_insur_Y= Agg_insurance(Aggregated_Insurance,years)
            
            col1,col2= st.columns(2)
            with col1:
                quarters= st.slider("**Select the Quarter**", df_agg_insur_Y["Quarter"].min(), df_agg_insur_Y["Quarter"].max(),df_agg_insur_Y["Quarter"].min())

            Agg_insurance1(df_agg_insur_Y, quarters)
            
   
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
            
        elif method == "Transaction Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_at= st.slider("**Select the Year**", Aggregated_transaction["Years"].min(), Aggregated_transaction["Years"].max(),Aggregated_transaction["Years"].min())

            df_agg_tran_Y= Agg_transaction(Aggregated_transaction,years_at)
            
            col1,col2= st.columns(2)
            with col1:
                quarters_at= st.slider("**Select the Quarter**", df_agg_tran_Y["Quarter"].min(), df_agg_tran_Y["Quarter"].max(),df_agg_tran_Y["Quarter"].min())

            df_agg_tran_Y_Q= Agg_transaction1(df_agg_tran_Y, quarters_at)
            
            #Select the State for Analyse the Transaction type
            state_Y_Q= st.selectbox("**Select the State**",df_agg_tran_Y_Q["States"].unique())

            Aggre_Transaction_type(df_agg_tran_Y_Q,state_Y_Q)

#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
        elif method == "User Analysis":
            year_au= st.selectbox("Select the Year",Aggregated_user["Years"].unique())
            agg_user_Y= Aggre_user_plot_1(Aggregated_user,year_au)

            quarter_au= st.selectbox("Select the Quarter",agg_user_Y["Quarter"].unique())
            agg_user_Y_Q= Aggre_user_plot_2(agg_user_Y,quarter_au)

            state_au= st.selectbox("**Select the State**",agg_user_Y["States"].unique())
            Aggre_user_plot_3(agg_user_Y_Q,state_au)    
#xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    with tab2:
        method_map = st.radio("**Select the Analysis Method(MAP)**",["Map Insurance Analysis", "Map Transaction Analysis", "Map User Analysis"])

        if method_map == "Map Insurance Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_m1= st.slider("**Select the Year_mi**", Map_Insurance["Years"].min(), Map_Insurance["Years"].max(),Map_Insurance["Years"].min())

            df_map_insur_Y= Agg_insurance(Map_Insurance,years_m1)

            col1,col2= st.columns(2)
            with col1:
                state_m1= st.selectbox("Select the State_mi", df_map_insur_Y["States"].unique())

            map_insure_plot_1(df_map_insur_Y,state_m1)
            
            col1,col2= st.columns(2)
            with col1:
                quarters_m1= st.slider("**Select the Quarter_mi**", df_map_insur_Y["Quarter"].min(), df_map_insur_Y["Quarter"].max(),df_map_insur_Y["Quarter"].min())

            df_map_insur_Y_Q= Agg_insurance1(df_map_insur_Y, quarters_m1)

            col1,col2= st.columns(2)
            with col1:
                state_m2= st.selectbox("Select the State_miy", df_map_insur_Y_Q["States"].unique())            
            
            map_insure_plot_2(df_map_insur_Y_Q, state_m2)

        elif method_map == "Map Transaction Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_m2= st.slider("**Select the Year_mi**", Map_transaction["Years"].min(), Map_transaction["Years"].max(),Map_transaction["Years"].min())

            df_map_tran_Y= Agg_insurance(Map_transaction, years_m2)

            col1,col2= st.columns(2)
            with col1:
                state_m3= st.selectbox("Select the State_mi", df_map_tran_Y["States"].unique())

            map_insure_plot_1(df_map_tran_Y,state_m3)
            
            col1,col2= st.columns(2)
            with col1:
                quarters_m2= st.slider("**Select the Quarter_mi**", df_map_tran_Y["Quarter"].min(), df_map_tran_Y["Quarter"].max(),df_map_tran_Y["Quarter"].min())

            df_map_tran_Y_Q= Agg_insurance1(df_map_tran_Y, quarters_m2)

            col1,col2= st.columns(2)
            with col1:
                state_m4= st.selectbox("Select the State_miy", df_map_tran_Y_Q["States"].unique())            
            
            map_insure_plot_2(df_map_tran_Y_Q, state_m4)

        elif method_map == "Map User Analysis":
            col1,col2= st.columns(2)
            with col1:
                year_mu1= st.selectbox("**Select the Year_mu**",Map_user["Years"].unique())
            map_user_Y= map_user_plot_1(Map_user, year_mu1)

            col1,col2= st.columns(2)
            with col1:
                quarter_mu1= st.selectbox("**Select the Quarter_mu**",map_user_Y["Quarter"].unique())
            map_user_Y_Q= map_user_plot_2(map_user_Y,quarter_mu1)

            col1,col2= st.columns(2)
            with col1:
                state_mu1= st.selectbox("**Select the State_mu**",map_user_Y_Q["States"].unique())
            map_user_plot_3(map_user_Y_Q, state_mu1)

    with tab3:
        method_top = st.radio("**Select the Analysis Method(TOP)**",["Top Insurance Analysis", "Top Transaction Analysis", "Top User Analysis"])

        if method_top == "Top Insurance Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_t1= st.slider("**Select the Year_ti**", Top_Insurance["Years"].min(), Top_Insurance["Years"].max(),Top_Insurance["Years"].min())
 
            df_top_insur_Y= top_user_plot_1(Top_Insurance,years_t1)

            
            col1,col2= st.columns(2)
            with col1:
                quarters_t1= st.slider("**Select the Quarter_ti**", df_top_insur_Y["Quarter"].min(), df_top_insur_Y["Quarter"].max(),df_top_insur_Y["Quarter"].min())

            df_top_insur_Y_Q= top_user_plot_2(df_top_insur_Y, quarters_t1)

        
        elif method_top == "Top Transaction Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_t2= st.slider("**Select the Year_tt**", Top_transaction["Years"].min(), Top_transaction["Years"].max(),Top_transaction["Years"].min())
 
            df_top_tran_Y= Agg_insurance1(Top_transaction,years_t2)

            
            col1,col2= st.columns(2)
            with col1:
                quarters_t2= st.slider("**Select the Quarter_tt**", df_top_tran_Y["Quarter"].min(), df_top_tran_Y["Quarter"].max(),df_top_tran_Y["Quarter"].min())

            df_top_tran_Y_Q= Agg_insurance1(df_top_tran_Y, quarters_t2)

        elif method_top == "Top User Analysis":
            col1,col2= st.columns(2)
            with col1:
                years_t3= st.selectbox("**Select the Year_tu**", Top_user["Years"].unique())

            df_top_user_Y= top_user_plot_1(Top_user,years_t3)

            col1,col2= st.columns(2)
            with col1:
                state_t3= st.selectbox("**Select the State_tu**", df_top_user_Y["States"].unique())

            df_top_user_Y_S= top_user_plot_2(df_top_user_Y,state_t3)


if select == "Top Charts":

    ques= st.selectbox("**Select the Question**",('Top Brands Of Mobiles Used','States With Lowest Trasaction Amount',
                                  'District With Highest Transaction Amount','Top 10 District With Lowest Transaction Amount',
                                  'Top 10 States With AppOpens','Least 10 States With AppOpens','States With Lowest Trasaction Count',
                                 'States With Highest Trasaction Count','States With Highest Trasaction Amount',
                                 'Top 50 District With Lowest Transaction Amount'))
    
    if ques=="Top Brands Of Mobiles Used":
        ques1()

    elif ques=="States With Lowest Trasaction Amount":
        ques2()

    elif ques=="District With Highest Transaction Amount":
        ques3()

    elif ques=="Top 10 District With Lowest Transaction Amount":
        ques4()

    elif ques=="Top 10 States With AppOpens":
        ques5()

    elif ques=="Least 10 States With AppOpens":
        ques6()

    elif ques=="States With Lowest Trasaction Count":
        ques7()

    elif ques=="States With Highest Trasaction Count":
        ques8()

    elif ques=="States With Highest Trasaction Amount":
        ques9()

    elif ques=="Top 50 District With Lowest Transaction Amount":
        ques10()

