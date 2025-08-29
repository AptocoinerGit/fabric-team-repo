# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0ed532c9-477a-4655-8b95-865523ddaf84",
# META       "default_lakehouse_name": "Broze",
# META       "default_lakehouse_workspace_id": "a100f46b-7947-44ff-a54b-319ba50ce9fe",
# META       "known_lakehouses": [
# META         {
# META           "id": "0ed532c9-477a-4655-8b95-865523ddaf84"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%html
# MAGIC 
# MAGIC Module1

# METADATA ********************

# META {
# META   "language": "html",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders = spark.read.csv("Files/orders.csv", header=True, inferSchema=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales = orders.withColumn("net_amount", F.col("quantity") * F.col("unit_price"))

sales.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders =( orders
               .withColumn('order_ts', F.to_timestamp('order_ts'))
               .withColumn('quantity',F.col('quantity').cast('int'))
               .withColumn('unit_price',F.col('unit_price').cast('double')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_50 = (orders
                    .filter(F.col('unit_price')>200))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_50.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = spark.read.option('header',True).csv('Files/products.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_products = orders_50.join(F.broadcast(products), 'product_id','inner')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_products = (
    orders_50.alias("o")
    .join(
        F.broadcast(products).alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "inner"
    )
    .drop("product_id")   # drops duplicate from right side (products)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_products.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_summary = (
    order_products
    .groupBy("order_ts", "product_id")
    .agg(
        F.round(F.sum("unit_price"), 2).alias("total")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_summary.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(sales_summary
.repartition("order_ts") # partition for write parallelism
.write
.format("delta")
.mode("overwrite")
.partitionBy("order_ts")
.save("Files/sales_summary"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = spark.read.format("delta").load("Files/sales_summary")
result.orderBy("order_ts", "product_id").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%html
# MAGIC module-2

# METADATA ********************

# META {
# META   "language": "html",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("abfss://pyspark_learning_by_venky@onelake.dfs.fabric.microsoft.com/Broze.Lakehouse/Files/customers.json")
# df now is a Spark DataFrame containing JSON data from "abfss://pyspark_learning_by_venky@onelake.dfs.fabric.microsoft.com/Broze.Lakehouse/Files/customers.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
StructField("CustomerID", IntegerType(), True),
StructField("Name", StringType(), True),
StructField("Age", IntegerType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nested_schema = StructType([
StructField("id", IntegerType(), True),
StructField("address", StructType([
StructField("city", StringType(), True),
StructField("state", StringType(), True)
]))
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df = spark.read.parquet("/data/sales")
# Transformation
agg_df = sales_df.groupBy("region").agg({"amount": "sum"})
# Action
agg_df.show()


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
