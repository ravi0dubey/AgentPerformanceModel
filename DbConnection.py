import mysql.connector as connection
import pandas as pd
import csv

def connect_db():
    try:
        mydb = connection.connect(host="localhost", user="devuser", passwd="Logitech1234#", use_pure=True)
        show_query = "SHOW DATABASES"
        cursor = mydb.cursor()  # create a cursor to execute queries
        cursor.execute(show_query)
        return "connection success"
    except Exception as e:
            mydb.close()
            return (str(e))

def create_table():
    try:
        mydb = connection.connect(host="localhost", database= "projectdb",user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries
        create_table_query1 = "CREATE TABLE if not exists projectdb.Agent_Login_Report(	`SL_No` INT NOT NULL, `Agent` VARCHAR(22) NOT NULL, `Date` VARCHAR(9) NOT NULL, `Login_Time` VARCHAR(11) NOT NULL, `Logout_Time` VARCHAR(11) NOT NULL, `Duration` VARCHAR(10) NOT NULL);"
        cursor.execute(create_table_query1)
        create_table_query2 = "create table if not exists projectdb.Agent_Performance_Report (`SL_No` INT NOT NULL, `Date` DATE NOT NULL, `Agent_Name` VARCHAR(22) NOT NULL, `Total_Chats` INT NOT NULL, `Average_Response_Time` VARCHAR(10) NOT NULL, `Average_Resolution_Time` VARCHAR(10) NOT NULL, `Average_Rating` DECIMAL(5, 2) NOT NULL, `Total_Feedback` INT NOT NULL);"
        cursor.execute(create_table_query2)
        return "connection success"
    except Exception as e:
        print("issue hai")
        print(str(e))
        return (str(e))


def one_time_load():
    try:
        mydb = connection.connect(host="localhost", database= "projectdb",user="devuser", passwd="Logitech1234#", use_pure=True)
        cursor = mydb.cursor()  # create a cursor to execute queries

        load_data_query2 = "load data infile 'D://Study//Data Science//Python//ineuron//Data_Set//Agent_Performance_Report.csv'  into table projectdb.Agent_Performance_Report fields terminated by ',' Enclosed by '""' IGNORE 1 ROWS ;"
        print(load_data_query2)
        cursor.execute(load_data_query2)
        load_data_query1 = "load data infile 'D://Study//Data Science//Python//ineuron//Data_Set//Agent_Login_Report.csv'  into table projectdb.Agent_Login_Report fields terminated by ',' Enclosed by '""' IGNORE 1 ROWS ;"
        print(load_data_query1)
        cursor.execute(load_data_query1)
        mydb.commit()
        print("load successful")
        return "Load success"
    except Exception as e:
        print("issue hai")
        print(str(e))
        return (str(e))

