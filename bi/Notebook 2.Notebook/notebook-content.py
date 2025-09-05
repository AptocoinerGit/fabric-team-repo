# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0c137be7-63e7-4497-b122-378ba3ff3aea",
# META       "default_lakehouse_name": "VLakeHouseSilverLayer",
# META       "default_lakehouse_workspace_id": "0548e0b4-eaba-4f2e-bfe7-dba445249bdb",
# META       "known_lakehouses": [
# META         {
# META           "id": "0c137be7-63e7-4497-b122-378ba3ff3aea"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ***checking shortcutdata***

# CELL ********************

df = spark.read.format("csv").option("header",True).load("Files/RawDataAdventureWorks_Customers/AdventureWorks_Customers.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading Customer Data and Applying Transformations And Write the data at end**

# CELL ********************

df1 = spark.read.format("csv")\
                    .option("header",True)\
                    .option("inferschema",True)\
                    .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/RawDataAdventureWorks_Customers")
                    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df1.withColumn("FullName",concat(col("Prefix"),lit(" "),col("FirstName"),lit(" "),col("LastName")))
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = df1.withColumn("Domain",split("EmailAddress","@")[1])
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Customers")\
         .saveAsTable("Customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading Sales Data and Applying Transformations And Write the data at end**

# CELL ********************

dff1 = spark.read.format("csv")\
                    .option("header",True)\
                    .option("inferschema",True)\
                    .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/RawDataAdventureWorks_Sales_2016")
                    
dff2 = spark.read.format("csv")\
                    .option("header",True)\
                    .option("inferschema",True)\
                    .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/RawDataAdventureWorks_Sales_2017")
                                        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dff2 = dff1.union(dff2)
display(dff2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dff2.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Sales")\
         .saveAsTable("Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading cat and subcat Data and Applying Transformations And Write the data at end**

# CELL ********************

df3= spark.read.format("delta")\
                .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Tables/Merge")
display(df3)           

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Subcat")\
         .saveAsTable("Subcat")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading Calendar Data and Applying Transformations And Write the data at end**

# MARKDOWN ********************


# CELL ********************

df4 = spark.read.format("csv")\
                    .option("header",True)\
                    .option("inferschema",True)\
                    .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/RawDataAdventureWorks_Calendar")
display(df4)                                      

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 

# CELL ********************

df4.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Calendar")\
         .saveAsTable("Calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading Products Data and Applying Transformations And Write the data at end**

# CELL ********************

df5 = spark.read.format("csv")\
                    .option("header",True)\
                    .option("inferschema",True)\
                    .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Files/RawDataAdventureWorks_Products")
display(df5)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df5=df5.withColumn("ProductSkU",split("ProductSKU","-")[0])
display(df5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df5.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Products")\
         .saveAsTable("Products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Reading Returns Data and Applying Transformations And Write the data at end**

# CELL ********************

df6= spark.read.format("delta")\
                .load("abfss://Vamshi_A@onelake.dfs.fabric.microsoft.com/VLakehouse.Lakehouse/Tables/newraw")
display(df6)           

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df6.write.format("delta")\
         .mode("Append")\
         .option("Path","Files/Returns")\
         .saveAsTable("Returns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.mkdirs("Files/Mkdirs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.cp("Files/Products","Files/Mkdirs",True)

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
