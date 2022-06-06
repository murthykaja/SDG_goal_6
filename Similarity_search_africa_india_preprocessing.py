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
Code Description : Data Preprocessing for Similarity Search. Aggregrating Chemical data to per year readings. Joining Chemical data and Other details(like population, GDP, GNI) data for similarity serach for India and Africa regions.
"""

from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark import SparkContext
# sc =SparkContext.getOrCreate()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
spark = SparkSession(sc)
import pandas as pd
pd.set_option("display.max_columns",None)
india_wqi_path = "/FileStore/tables/india_wqi.csv"
africa_wqi_path = "/FileStore/tables/africa_wqi.csv"
africa_india_other_path = "/FileStore/tables/Africa_and_India.csv"
df_india_wqi_input = spark.read.load(india_wqi_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_india_wqi_input.createOrReplaceTempView("table_india_wqi_input")
print(df_india_wqi_input.limit(10).toPandas())
df_india_chem = spark.sql("select Country,cast(from_unixtime(unix_timestamp(date_reported,'yyyy-mm-dd'),'yyyy') as int) as date_reported,BOD,Chloride,Conductivity,Iron,Non_ionised_ammonia,Nitrate,Sodium,Dissolved_oxygen,Water_temperature,Phosphate,Total_suspended_solids,pH,WQI from table_india_wqi_input")
df_india_chem.createOrReplaceTempView("table_india_chem")
chem_cols= ["BOD","Chloride","Conductivity","Iron","Non_ionised_ammonia","Nitrate","Sodium","Dissolved_oxygen","Water_temperature","Phosphate","Total_suspended_solids","pH","WQI"]
sql_agv_formula = "select Country,date_reported, "
for chem in chem_cols:
    sql_agv_formula+="sum("+chem+")/(count("+chem+")) as "+chem+" ,"
sql_agv_formula = sql_agv_formula[:-1]
sql_agv_formula+=" from table_india_chem group by Country,date_reported order by Country,date_reported"
df_india_water_chem_year = spark.sql(sql_agv_formula)
df_india_water_chem_year.createOrReplaceTempView("Table_india_chem_year_avg")
print(df_india_water_chem_year.limit(10).toPandas())
df_africa_wqi_input = spark.read.load(africa_wqi_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_africa_wqi_input.createOrReplaceTempView("table_africa_wqi_input")
df_africa_wqi_input.limit(10).toPandas()
df_africa_chem = spark.sql("select Country,cast(from_unixtime(unix_timestamp(date_reported,'yyyy-mm-dd'),'yyyy') as int) as date_reported,BOD,Chloride,Conductivity,Iron,Non_ionised_ammonia,Nitrate,Sodium,Dissolved_oxygen,Water_temperature,Phosphate,Total_suspended_solids,pH,WQI from table_africa_wqi_input")
df_africa_chem.createOrReplaceTempView("table_africa_chem")
chem_cols= ["BOD","Chloride","Conductivity","Iron","Non_ionised_ammonia","Nitrate","Sodium","Dissolved_oxygen","Water_temperature","Phosphate","Total_suspended_solids","pH","WQI"]
sql_agv_formula = "select Country,date_reported, "
for chem in chem_cols:
    sql_agv_formula+="sum("+chem+")/(count("+chem+")) as "+chem+" ,"
sql_agv_formula = sql_agv_formula[:-1]
sql_agv_formula+=" from table_africa_chem group by Country,date_reported order by Country,date_reported"
df_africa_water_chem_year = spark.sql(sql_agv_formula)
df_africa_water_chem_year.createOrReplaceTempView("Table_africa_chem_year_avg")
df_africa_water_chem_year.limit(10).toPandas()
df_africa_india_water_chem_year = df_africa_water_chem_year.union(df_india_water_chem_year)
df_africa_india_water_chem_year = df_africa_india_water_chem_year.withColumn("Country", when(df_africa_india_water_chem_year.Country == "Congo (the Democratic Republic of the)","Congo").when(df_africa_india_water_chem_year.Country == "Sudan (the)","Sudan").otherwise(df_africa_india_water_chem_year.Country))
df_africa_india_water_chem_year.createOrReplaceTempView("Table_africa_india_chem_year_avg")
df_africa_india_water_chem_year.limit(10).toPandas()
africa_india_other_path = "/FileStore/tables/Africa_and_India.csv"
df_africa_india_other_input = spark.read.load(africa_india_other_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_africa_india_other_input = df_africa_india_other_input.withColumnRenamed("Indicator name","Indicator_name").withColumnRenamed("Geographical area name","Country").withColumnRenamed("Indicator Code","Indicator_Code")
df_africa_india_other_input.createOrReplaceTempView("table_africa_india_other_input")
df_africa_india_other = spark.sql("select Indicator_Code,Indicator_name,Country,Year,Value from table_africa_india_other_input")
df_africa_india_other.createOrReplaceTempView("table_africa_india_other")
df_africa_india_other.limit(10).toPandas()
attribute_names = ["SP.DYN.LE00.IN","SP.POP.TOTL","NY.GDP.MKTP.PP.KD","NY.GNP.PCAP.PP.KD","EN.ATM.PM25.MC.ZS","NY.GNP.MKTP.PP.KD","EN.ATM.CO2E.PC","AG.LND.AGRI.ZS","NY.GDP.PCAP.PP.KD","HDI"]
col_names = ["Country","Year","Life_expectancy","Population","GDP","GNI_per_capita","air_pollution","GNI","CO2_emissions","Agricultural_land","GPD_per_capita","HDI"]
df_africa_india_other_req_vals = df_africa_india_other.filter(df_africa_india_other.Indicator_Code.isin(attribute_names))
df_africa_india_other_req_vals.createOrReplaceTempView("table_africa_india_other_req_vals")
sql_case=""
case_str_1 = " case true when Indicator_Code == '"
case_str_2 = "' then Value else 0 end"

i=2
for attribute in attribute_names:
    sql_case+=case_str_1+attribute+case_str_2+" as "+col_names[i]+" ,"
    i+=1
sql_case = sql_case[:-1]
sql_all = "select Country,Year,"+sql_case+" from table_africa_india_other_req_vals"
print(sql_all)
df_africa_india_other_req_vals = spark.sql(sql_all)

df_africa_india_other_req_vals.createOrReplaceTempView("table_africa_india_other_split")
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select Country,Year,"+sql2_case+" from table_africa_india_other_split group by Country,Year"
# print(sql2)
df_africa_india_other_per_year = spark.sql(sql2)
df_africa_india_other_per_year.createOrReplaceTempView("table_africa_india_other_per_year")
df_africa_india_other_per_year.limit(10).toPandas()
"""
Joining table_africa_india_other_per_year and Table_africa_india_chem_year_avg tables
"""
joined_table =spark.sql("select Table_africa_india_chem_year_avg.Country as country,Year,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Water_temperature,Total_suspended_solids,Conductivity,Phosphate,pH,WQI,Life_expectancy,Population,GDP,GNI_per_capita,air_pollution,GNI,CO2_emissions,Agricultural_land,GPD_per_capita,HDI from Table_africa_india_chem_year_avg join table_africa_india_other_per_year  on table_africa_india_other_per_year.Country = Table_africa_india_chem_year_avg.Country and Table_africa_india_chem_year_avg.date_reported=table_africa_india_other_per_year.Year order by table_africa_india_other_per_year.Country,table_africa_india_other_per_year.Year")
joined_table.createOrReplaceTempView("table_joined")
joined_table.coalesce(1).write.mode('overwrite').option('header','true').option("compression", "gzip").csv('dbfs:/FileStore/africa_india_similarity_data_WQI')
