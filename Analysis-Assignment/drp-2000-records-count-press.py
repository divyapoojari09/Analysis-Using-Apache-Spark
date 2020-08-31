#Page 228 of E-book
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.types import StructField,StructType, StringType, LongType, FloatType

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()
 
df1 = spark.read.text("hdfs://namenode/output/itmd-521/drp/2000/csv-file")

df2 = df1.withColumn('Weather_Station', df1['value'].substr(5, 6))\
.withColumn('WBAN', df1['value'].substr(11, 5))\
.withColumn('Observation_Date',to_date(df1['value'].substr(16,8),"yyyyMMdd"))\
.withColumn('Observation_Hour', df1['value'].substr(24, 4).cast(IntegerType()))\
.withColumn('Latitude', df1['value'].substr(29, 6).cast('float') / 1000)\
.withColumn('Longitude', df1['value'].substr(35, 7).cast('float') / 1000)\
.withColumn('Elevation', df1['value'].substr(47, 5).cast(IntegerType()))\
.withColumn('Wind_Direction', df1['value'].substr(61, 3).cast(IntegerType()))\
.withColumn('WD_Quality_Code', df1['value'].substr(64, 1).cast(IntegerType()))\
.withColumn('Sky_Ceiling_Height', df1['value'].substr(71, 5).cast(IntegerType()))\
.withColumn('SC_Quality_Code', df1['value'].substr(76, 1).cast(IntegerType()))\
.withColumn('Visibility_Distance', df1['value'].substr(79, 6).cast(IntegerType()))\
.withColumn('VD_Quality_Code', df1['value'].substr(86, 1).cast(IntegerType()))\
.withColumn('Air_Temperature', df1['value'].substr(88, 5).cast('float') /10)\
.withColumn('AT_Quality_Code', df1['value'].substr(93, 1).cast(IntegerType()))\
.withColumn('Dew_Point', df1['value'].substr(94, 5).cast('float'))\
.withColumn('DP_Quality_Code', df1['value'].substr(99, 1).cast(IntegerType()))\
.withColumn('Atmospheric_Pressure', df1['value'].substr(100, 5).cast('float')/ 10)\
.withColumn('AP_Quality_Code', df1['value'].substr(105, 1).cast(IntegerType()))\
.drop('value')

#Filtering bad records
df1_BadRecords_press =df2.filter(col("Atmospheric_Pressure")== 9999.9)

#Calculating percentage of bad records
df1_Percent_press= (df1_BadRecords_press.count()*100.0/df2.count())



Schema = StructType([StructField("BadData_Cnt_Press",StringType(),True),StructField("TotalData_Cnt",StringType(),True),StructField("Percentage",FloatType(),True)])

#First Row
Row1 = Row(df1_BadRecords_press.count(),df2.count(),df1_Percent_press)

mydf=spark.createDataFrame([Row1],Schema)

mydf.show()

#Writing bad records percent to csv
mydf.write.format("csv").mode("overwrite").option("header","true").save("hdfs://namenode/output/itmd-521/drp/2000/Bad_Data_Percent_Press")