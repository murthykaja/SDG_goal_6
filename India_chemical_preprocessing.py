"""
Project : CSE 545: Big Data Analytics Project Report
Aim : SDG 6 - Clean Water and Sanitation
Team Memabers:
1. Murthy Kaja
2. Yashwanth M
3. Kowshik S
4. Nithin Reddy
System : Azure Databricks 
Databricks Runtime Version : 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)
Worker Nodes :  4 (28 GB and 8 cores)
Driver Nodes :  1 (28 GB and 8 cores)
Code Description : Using Spark Data frames and Rdd for data preprocessing. This includes columnar data to row format and replacing missingvalues with median. For Inida data
"""


from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark import SparkContext
# sc =SparkContext.getOrCreate()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
spark = SparkSession(sc)
import pandas as pd
pd.set_option("display.max_columns",None)
# File location and type
file_location = "hdfs:///data/india_samples.csv"
columns_input=['Station','date_reported','time','depth','chemical','test','temp','value','metric','validity']
df_india_input = spark.read.load(file_location,
                     format="csv", sep=";", inferSchema="true", header="false",col=columns_input)
i=0
for cc in columns_input:
    first ='_c'+str(i)
    i+=1
    df_india_input = df_india_input.withColumnRenamed(first,cc)
df_india_input.createOrReplaceTempView("table_afica")
print(df_india_input.limit(10).toPandas())
"""
0 - pH - pH
1 - Phosphate - TP
2 -Iron - Fe-Tot
3 -Nitrate - NO3N
4 -Oxygen_saturation - O2-Dis-Sat
5 -Sodium -Na-Dis
6 -Dissolved_oxygen - O2-Dis
7 -Chloride- Cl-Dis
8 -Water_temperature - TEMP
9 -Total_suspended_solids - TSS
10 -Non_ionised_ammonia - NH4N
11 -Conductivity - EC
12- biological_dissolved_oxygen - BOD
"""
list_chemicals = ["pH","TP","Fe-Tot","NO3N","O2-Dis-Sat","Na-Dis","O2-Dis","Cl-Dis","TEMP","TSS","NH4N","EC","BOD"]
df_india_input_f = df_india_input.filter(df_india_input.chemical.isin(list_chemicals))
df_india_input_f.createOrReplaceTempView("Table_Filtered_Chem")
print(df_india_input_f.limit(10).toPandas())
# ??g/l to mg/l	
df_metrics_change = spark.sql("select Station,date_reported,chemical,case true when metric=='??g/l' then value*0.001 else value end as value,case true when metric=='??g/l' then 'mg/l' else metric end as metric from Table_Filtered_Chem")
df_metrics_change.createOrReplaceTempView("Table_metrics_change")
df_metrics_change.printSchema()
df_per_day_avg = spark.sql("select Station,date_reported,chemical,sum(value)/count(1) as value from Table_metrics_change group by Station,date_reported,chemical")
df_per_day_avg.createOrReplaceTempView("Table_per_day_avg")
print(df_per_day_avg.limit(10).toPandas())
"""
Converting Columnar data to row format. i.e converting data in row like chemical name, value to chemical name as column
"""
sql_case=""
case_str_1 = " case true when chemical == '"
case_str_2 = "' then value else 0 end "
col_names = ["Station","date_reported","BOD","Chloride","Conductivity","Iron","Non_ionised_ammonia","Nitrate","Sodium","Dissolved_oxygen","Water_temperature","Phosphate","Total_suspended_solids","pH"]
chem_names = ["BOD","Cl-Dis","EC","Fe-Tot","NH4N","NO3N","Na-Dis","O2-Dis","TEMP","TP","TSS","pH"]
i=2
for chem in chem_names:
    sql_case+=case_str_1+chem+case_str_2+" as "+col_names[i]+" ,"
    i+=1
sql_case = sql_case[:-1]
sql_all = "select Station,date_reported,"+sql_case+" from Table_per_day_avg"
df_cols_rows = spark.sql(sql_all)
df_cols_rows.createOrReplaceTempView("Table_cols_rows")
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select Station,date_reported,"+sql2_case+" from Table_cols_rows group by Station,date_reported"
df_per_day = spark.sql(sql2)
df_per_day.withColumn('date_reported', to_timestamp('date_reported').cast('string')).withColumn('date_reported', substring('date_reported', 1,7)).createOrReplaceTempView("Table_row_month")
sql3=""
sql3_case = ""
for cols in col_names[2:]:
    sql3_case += " sum("+cols+") as "+cols+" ,"
sql3_case = sql3_case[:-1]
sql3 = "select Station,date_reported,"+sql3_case+" from Table_row_month group by Station,date_reported"
df_per_month = spark.sql(sql3)
df_per_month.createOrReplaceTempView("Table_per_month")
"""
Location Data
"""
df_india_location = spark.read.load("hdfs:///data/meta_data.csv",
                     format="csv", sep=",", inferSchema="true", header="true",col=columns_input)
df_india_location = df_india_location.select(col("GEMS Station Number"),col("Country Name")).withColumnRenamed("GEMS Station Number","Station").withColumnRenamed("Country Name","Country")
df_india_location.createOrReplaceTempView("Table_Location")
df_india_location.limit(10).toPandas()
"""
Joining location Data with Chemical Data
"""
df_chem_location = spark.sql("select a.*,b.Country from Table_per_month a join Table_Location b on a.Station = b.Station")
df_chem_location.limit(10).toPandas()

"""
Replacing null values with median value
"""
col_repalce_mean = ['BOD','Chloride','Conductivity','Iron','Non_ionised_ammonia','Nitrate','Sodium','Dissolved_oxygen','Water_temperature','Phosphate','Total_suspended_solids','pH']
df_chem_location = df_chem_location.select([when(col(c)==0,None).otherwise(col(c)).alias(c) for c in df_chem_location.columns])
for column_mean in col_repalce_mean:
    df_mean = df_chem_location.fillna(value=0, subset=[column_mean]).select(mean(col(column_mean)).alias('avg')).collect()
    avg = df_mean[0]['avg']
    df_chem_location = df_chem_location.withColumn(column_mean, F.udf(lambda x: avg if x is None else x)(F.col(column_mean)))
    df_chem_location = df_chem_location.withColumn(column_mean, F.round(df_chem_location[column_mean], 2))
# df.show()
df_chem_location.createOrReplaceTempView("Table_chem_loc_nonull")
df_chem_location.limit(10).toPandas()
df_chem_location.coalesce(1).write.mode('overwrite').option('header','true').option("compression", "gzip").csv('hdfs:///data/india_wqi.csv')
