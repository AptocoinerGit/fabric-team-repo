# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "389307b6-caa4-4f21-b8cb-3c60df1d0c90",
# META       "default_lakehouse_name": "VLakehouse",
# META       "default_lakehouse_workspace_id": "0548e0b4-eaba-4f2e-bfe7-dba445249bdb",
# META       "known_lakehouses": [
# META         {
# META           "id": "389307b6-caa4-4f21-b8cb-3c60df1d0c90"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
print("hello ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/Raw Data/Scriptf/git.json")
# df now is a Spark DataFrame containing JSON data from "Files/Raw Data/Scriptf/git.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/Raw Data/Scriptf/git.json")
# df now is a Spark DataFrame containing JSON data from "Files/Raw Data/Scriptf/git.json".
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM VLakehouse.adventureworks_calendar_table LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
