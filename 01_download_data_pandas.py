# Databricks notebook source
import pyodbc

# COMMAND ----------

import pyodbc

# Define the connection parameters
server = '172.31.38.24'
database = 'LUXBOOK'
username = 'smalluser'
password = 'SUPERfreak788'

# Establish the connection
conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')


# COMMAND ----------


# Create a cursor
cursor = conn.cursor()

# Execute an SQL query
sql_query = "SELECT * FROM your_table_name"
cursor.execute(sql_query)

# Fetch the data
data = cursor.fetchall()

# Iterate through the results
for row in data:
    print(row)

# Close the cursor and connection
cursor.close()
conn.close()


# COMMAND ----------

rbhq_con = (spark.read
  .format("sqlserver")
  .option("host", "172.31.38.24")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "smalluser")
  .option("password", "SUPERfreak788")
  .option("database", "LUXBOOK")
  .option("encrypt","true")
  .option("trustServerCertificate","true")
)

# COMMAND ----------

colossalbet_con = (spark.read
  .format("sqlserver")
  .option("host", "colossalbet.calle8u3juvv.ap-southeast-2.rds.amazonaws.com")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "admin")
  .option("password", "zxkxAQgQ97F4f1zvXdZx")
  .option("database", "colossalbet")
  .option("encrypt","true")
  .option("trustServerCertificate","true")
)

# COMMAND ----------

meeting_tab = rbhq_con.option("dbtable", "MEETING_TAB").load()
event_tab = rbhq_con.option("dbtable", "EVENT_TAB").load()
runner_tab = rbhq_con.option("dbtable", "RUNNER_TAB").load()
meetings_mapping_table = rbhq_con.option("dbtable", "mts_meetings_mapping_table").load()

# COMMAND ----------

from datetime import date

today = date.today()

# COMMAND ----------

today

# COMMAND ----------

type(meetings_mapping_table)

# COMMAND ----------



# COMMAND ----------

country_list = ["AU","NZ"]
rs_meetings_data_filtered = (meetings_mapping_table
                             .filter(meetings_mapping_table.rbhq_meeting_date == today)
                             .filter(meetings_mapping_table.rs_meeting_country_code.isin(country_list))
                             .select('rbhq_meeting_date', 'rbhq_meeting_venue','rbhq_meeting_id', 'rs_meeting_id','rs_meeting_country_code')
                             .collect()
)

# COMMAND ----------

type(rs_meetings_data_filtered)

# COMMAND ----------



# COMMAND ----------


