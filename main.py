import DbConnection
import Agent_Report_sql
import Agent_report_pandas


while True:
    choice = int(input("\n1. Create a table attribute dataset and dress dataset\n2. Do a bulk load for these two table for respective dataset using code\n3. Read Agent Dataset\n4. Total working days for each agents\n5. Total query Agents have taken \n6. Total Feedback Agents received \n7. Agent name who have average rating between 3.5 to 4  \n8.Agent name who have rating lesss then 3.5 \n9. Agent name who have rating more then 4.5 "))

    if choice   == 1:
        DbConnection.create_table()
    elif choice == 2:
        DbConnection.one_time_load()
    elif choice == 3:
        df_performance,df_login= Agent_Report_sql.read_agentdataset()
    elif choice == 4:
        Agent_report_pandas.agent_working_days()
    elif choice == 5:
        Agent_report_pandas.agent_total_query()
    elif choice == 6:
        Agent_report_pandas.agent_total_feedback()
    elif choice == 7:
        Agent_report_pandas.agent_Average_Rating_between_3_4()
    elif choice == 8:
        Agent_report_pandas.agent_Average_Rating_lessthan_3()
    elif choice == 9:
        Agent_report_pandas.agent_Average_Rating_greaterthan_4()
    else:
        print("Have a great day")