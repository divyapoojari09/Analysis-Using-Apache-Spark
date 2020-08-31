from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()
 
df2=spark.read.text("hdfs://namenode/user/controller/ncdc-orig/2000-2018.txt") 


df3=df2.withColumn('Weather_Station', df2['value'].substr(5, 6))\
.withColumn('WBAN', df2['value'].substr(11, 5))\
.withColumn('Observation_Date',to_date(df2['value'].substr(16,8),'yyyyMMdd'))\
.withColumn('Observation_Hour', df2['value'].substr(24, 4).cast(IntegerType()))\
.withColumn('Latitude', df2['value'].substr(29, 6).cast('float') / 1000)\
.withColumn('Longitude', df2['value'].substr(35, 7).cast('float') / 1000)\
.withColumn('Elevation', df2['value'].substr(47, 5).cast(IntegerType()))\
.withColumn('Wind_Direction', df2['value'].substr(61, 3).cast(IntegerType()))\
.withColumn('WD_Quality_Code', df2['value'].substr(64, 1).cast(IntegerType()))\
.withColumn('Sky_Ceiling_Height', df2['value'].substr(71, 5).cast(IntegerType()))\
.withColumn('SC_Quality_Code', df2['value'].substr(76, 1).cast(IntegerType()))\
.withColumn('Visibility_Distance', df2['value'].substr(79, 6).cast(IntegerType()))\
.withColumn('VD_Quality_Code', df2['value'].substr(86, 1).cast(IntegerType()))\
.withColumn('Air_Temperature', df2['value'].substr(88, 5).cast('float') /10)\
.withColumn('AT_Quality_Code', df2['value'].substr(93, 1).cast(IntegerType()))\
.withColumn('Dew_Point', df2['value'].substr(94, 5).cast('float'))\
.withColumn('DP_Quality_Code', df2['value'].substr(99, 1).cast(IntegerType()))\
.withColumn('Atmospheric_Pressure', df2['value'].substr(100, 5).cast('float')/ 10)\
.withColumn('AP_Quality_Code', df2['value'].substr(105, 1).cast(IntegerType()))\
.drop('value')

df3.registerTempTable('Table')

#Chapter 02 - Page 32 - Dataframes and SQL of E-book

#Using Select query and where clause to get minimum and maximum temperature of each month over the decade 2000-2010
df1=spark.sql('SELECT year(Observation_Date),month(Observation_Date) as Month,Min(Air_Temperature) as Min,Max(Air_Temperature) as Max FROM Table where Air_Temperature between -73 and 46 and year(Observation_Date) between 2000 and 2010 group by year(Observation_Date),month(Observation_Date) order by month(Observation_Date)')

#Writing it into a csv file
df1.write.csv(path="hdfs://namenode/output/itmd-521/drp/decade2000-2010/valid-records-temperature/Min_Max", mode="overwrite",header="true")