from __future__ import print_function
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()


#Chapter 15 Pg-264 of E-book Stages Tip (Concept covered in detail in Chapter 19 as well)

spark.conf.set("spark.sql.shuffle.partitions",20)

df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://namenode/output/itmd-521/drp/2000/valid-records-temp")

df.registerTempTable('Table')

#using select query to filter out min max for each month
df1=spark.sql('SELECT month(Observation_Date) as Month,Min(Air_Temperature) as Min,Max(Air_Temperature) as Max FROM Table where Air_Temperature between -73 and 46 group by month(Observation_Date) order by month(Observation_Date)')

#Write to csv
df1.write.csv(path="hdfs://namenode/output/itmd-521/drp/2000/Min_Max", mode="overwrite",header="true")