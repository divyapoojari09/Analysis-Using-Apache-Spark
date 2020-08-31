
# divyapoojari09
Private repo for ITM
# Divya Poojari

# ITMD-521 Cluster Read & Write Assignment 

## Cluster Command

#### Chapter 16 Page 277 of E-book Launching Applications- Adding --num-executors to launch more number of executors
#parquet 
```spark-submit --verbose --name drp-read-write-2000-parquet.py --master yarn --deploy-mode cluster --num-executors 11 drp-read-write-2000-parquet.py``` 

#csv
```spark-submit --verbose --name drp-read-write-2000-csv.py --master yarn --deploy-mode cluster --num-executors 11 drp-read-write-2000-csv.py```


### Code


df_2000=df2.filter(df2["Observation_Date"].between('2000-01-01','2000-12-31'))

#df_1920.show(5)

#Writing csv file
df_2000.write.format("csv").mode("overwrite").save("hdfs://namenode/output/itmd-521/drp/2000/csv-file")

#writing parquet file
#df_2000.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/drp/2000/parquet-file")


#### Explanation

I have referred Chapter 05- Basic Structured Operations pg-81 of E-book under the section of `Filtering Rows`
To filter rows, I first created an expression that evaluates to true or false which will then filter out the rows that evaluate to false. In this case, I have used column Observation_Date in the filter which will return only those rows whose Observation_date falls between '2000-01-01' and '2000-12-31' (inclusive of its ends) and filter out the remaining rows that do not fall in this range.

### Regarding Dealing with Bad Data

We would be using an SQL query which will filter out all trecords containing any bad records e.g. 99999.

