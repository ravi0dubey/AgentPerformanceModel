import mysql.connector as connection
import pandas as pd
import pymongo

def read_agentdataset():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_performance = pd.read_sql("select * from projectdb.Agent_Performance_Report", mydb)
        print(df_performance)
        df_login = pd.read_sql("select * from projectdb.Agent_Login_Report", mydb)
        print(df_login)
        return(df_performance,df_login)
    except Exception as e:
        print(str(e))
        return (str(e))

def agent_working_days():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_login = pd.read_sql("select * from projectdb.Agent_Login_Report", mydb)
        print(df_login.groupby('Agent')['Date'].nunique())
    except Exception as e:
        print(str(e))
        return (str(e))

def agent_total_query():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_performance = pd.read_sql("select * from projectdb.Agent_Performance_Report", mydb)
        print(f" Total Query solved by Each Agents: {df_performance.groupby('Agent_Name')['Total_Chats'].sum()}")
    except Exception as e:
        print(str(e))
        return (str(e))

def agent_total_feedback():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",
                                  use_pure=True)
        df_performance = pd.read_sql("select * from projectdb.Agent_Performance_Report", mydb)
        print(f" Total Feedback Received for Each Agents: {df_performance.groupby('Agent_Name')['Total_Feedback'].sum()}")
    except Exception as e:
        print(str(e))
        return (str(e))

def agent_Average_Rating_between_3_4():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",use_pure=True)
        df_performance = pd.read_sql("select ap.Agent_Name, avg(ap.average_rating) as Average_Rating from agent_performance_report ap join agent_login_report al on ap.Agent_Name = al.agent where ap.total_chats <> 0 group by Ap.Agent_name having avg(ap.average_rating) between 3.5 and 4.0", mydb)
        print("Average Rating per Agent\n")
        print(df_performance)
    except Exception as e:
        print(str(e))
        return (str(e))

def agent_Average_Rating_lessthan_3():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",use_pure=True)
        df_performance = pd.read_sql("select ap.Agent_Name, avg(ap.average_rating) as Average_Rating from agent_performance_report ap join agent_login_report al on ap.Agent_Name = al.agent where ap.total_chats <> 0 group by Ap.Agent_name having avg(ap.average_rating) < 3.5", mydb)
        print("Average Rating Less than 4.5 \n")
        print(df_performance)
    except Exception as e:
        print(str(e))
        return (str(e))


def agent_Average_Rating_greaterthan_4():
    try:
        mydb = connection.connect(host="localhost", database="projectdb", user="devuser", passwd="Logitech1234#",use_pure=True)
        df_performance = pd.read_sql("select ap.Agent_Name, avg(ap.average_rating) as Average_Rating from agent_performance_report ap join agent_login_report al on ap.Agent_Name = al.agent where ap.total_chats <> 0 group by Ap.Agent_name having avg(ap.average_rating) >= 4.5", mydb)
        print("Average Rating greater than equal to 4.5\n")
        print(df_performance)
    except Exception as e:
        print(str(e))
        return (str(e))