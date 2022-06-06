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
Code Description : Data Preprocessing for Similarity Search. Aggregrating Chemical data to per year readings. Joining Chemical data and Other details(like population, GDP, GNI) data for similarity serach for euro region.
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
euro_water_chem_path = "/FileStore/tables/euro_wqi.csv"

df_euro_water_chem_input = spark.read.load(euro_water_chem_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_euro_water_chem_input.createOrReplaceTempView("table_euro_water_chem")
df_euro_water_chem_input.limit(10).toPandas()
chem_cols= ["pH","Phosphate","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Oxygen_saturation","Water_temperature","Total_suspended_solids","Non_ionised_ammonia","Conductivity","WQI"]
df_euro_water_chem = df_euro_water_chem_input.withColumn('phenomenonTimeSamplingDate', to_timestamp('phenomenonTimeSamplingDate').cast('string')).withColumn('phenomenonTimeSamplingDate', substring('phenomenonTimeSamplingDate', 1,4))
df_euro_water_chem.createOrReplaceTempView("Table_euro_water_chem_year")
df_euro_water_chem.limit(10).toPandas()
chem_cols= ["pH","Phosphate","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Oxygen_saturation","Water_temperature","Total_suspended_solids","Non_ionised_ammonia","Conductivity","WQI"]
sql_agv_formula = "select country,phenomenonTimeSamplingDate, "
for chem in chem_cols:
    sql_agv_formula+="sum("+chem+")/(count("+chem+")) as "+chem+" ,"
sql_agv_formula = sql_agv_formula[:-1]
sql_agv_formula+=" from Table_euro_water_chem_year group by country,phenomenonTimeSamplingDate"
df_euro_water_chem_year = spark.sql(sql_agv_formula)
df_euro_water_chem_year.createOrReplaceTempView("Table_euro_water_chem_year_avg")
df_euro_water_chem_year.limit(10).toPandas()
europe_path = "/FileStore/tables/Europe.csv"
africa_path = "/FileStore/tables/Africa_and_India.csv"
euro_water_chem_path = "/FileStore/tables/euro_wqi.csv"

df_euro_other_input = spark.read.load(europe_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_euro_other_input.createOrReplaceTempView("table_euro_other_input")
df_africa_other_input = spark.read.load(africa_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_africa_other_input.createOrReplaceTempView("table_africa_other_input")
df_africa_other_input.limit(10).toPandas()
df_euro_other = df_euro_other_input.withColumnRenamed("Indicator name","Indicator_name").withColumnRenamed("Geographical area name","Country_name").withColumnRenamed("Indicator Code","Indicator_Code")
df_euro_other.createOrReplaceTempView("table_euro_other")
df_euro_other_req_cols = spark.sql("select Indicator_Code,Indicator_name,Country_name,Year,Value from table_euro_other")
df_euro_other_req_cols.createOrReplaceTempView("table_euro_other_req_cols")
df_africa_other_input = df_africa_other_input.withColumnRenamed("Indicator name","Indicator_name").withColumnRenamed("Geographical area name","Country_name").withColumnRenamed("Indicator Code","Indicator_Code")
df_africa_other_input.createOrReplaceTempView("table_africa_other")
df_africa_other_req_cols = spark.sql("select Indicator_Code,Indicator_name,Country_name,Year,Value from table_africa_other")
df_africa_other_req_cols.createOrReplaceTempView("table_africa_other_req_cols")
spark.sql("select * from table_africa_other_req_cols").limit(10).toPandas()
attribute_names = ["SP.DYN.LE00.IN","SP.POP.TOTL","NY.GDP.MKTP.PP.KD","NY.GNP.PCAP.PP.KD","EN.ATM.PM25.MC.ZS","NY.GNP.MKTP.PP.KD","EN.ATM.CO2E.PC","AG.LND.AGRI.ZS","NY.GDP.PCAP.PP.KD","HDI"]
df_euro_other_req_vals = df_euro_other_req_cols.filter(df_euro_other_req_cols.Indicator_Code.isin(attribute_names))
df_euro_other_req_vals.createOrReplaceTempView("table_euro_other_req_vals")
attribute_names = ["SP.DYN.LE00.IN","SP.POP.TOTL","NY.GDP.MKTP.PP.KD","NY.GNP.PCAP.PP.KD","EN.ATM.PM25.MC.ZS","NY.GNP.MKTP.PP.KD","EN.ATM.CO2E.PC","AG.LND.AGRI.ZS","NY.GDP.PCAP.PP.KD","HDI"]
col_names = ["Country_name","Year","Life_expectancy","Population","GDP","GNI_per_capita","air_pollution","GNI","CO2_emissions","Agricultural_land","GPD_per_capita","HDI"]

sql_case=""
case_str_1 = " case true when Indicator_Code == '"
case_str_2 = "' then Value else 0 end"

i=2
for attribute in attribute_names:
    sql_case+=case_str_1+attribute+case_str_2+" as "+col_names[i]+" ,"
    i+=1
sql_case = sql_case[:-1]
sql_all = "select Country_name,Year,"+sql_case+" from table_euro_other_req_vals"
print(sql_all)
df_euro_other_req_vals = spark.sql(sql_all)

df_euro_other_req_vals.createOrReplaceTempView("table_euro_other_split")
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select Country_name,Year,"+sql2_case+" from table_euro_other_split group by Country_name,Year"

df_euro_other_per_year = spark.sql(sql2)
df_euro_other_per_year.createOrReplaceTempView("table_euro_other_per_year")
df_euro_other_name_change = df_euro_other_per_year.withColumn("Country_name",when(df_euro_other_per_year.Country_name == "United Kingdom of Great Britain and Northern Ireland","United Kingdom").otherwise(df_euro_other_per_year.Country_name))
df_euro_other_name_change.createOrReplaceTempView("table_euro_other_name_change")

df_euro_water_chem_name_change = df_euro_water_chem_year.withColumn("country", when(df_euro_water_chem_year.country == "Kosova / Kosovo","Kosovo").when(df_euro_water_chem_year.country == "Србија","Serbia").when(df_euro_water_chem_year.country == "Ελλάς","Greece").when(df_euro_water_chem_year.country == "España","Spain").when(df_euro_water_chem_year.country == "Κύπρος - Kıbrıs","Cyprus").when(df_euro_water_chem_year.country == "Schweiz/Suisse/Svizzera/Svizra","Switzerland").when(df_euro_water_chem_year.country == "Bosna i Hercegovina / Босна и Херцеговина","Bosnia and Herzegovina").when(df_euro_water_chem_year.country == "Shqipëria","Albania").when(df_euro_water_chem_year.country == "Éire / Ireland","Ireland").when(df_euro_water_chem_year.country == "Türkiye","Turkey").when(df_euro_water_chem_year.country == "Hrvatska","Croatia").when(df_euro_water_chem_year.country == "België / Belgique / Belgien","Belgium").when(df_euro_water_chem_year.country == "Бългaрия","Bulgaria").when(df_euro_water_chem_year.country == "Crna Gora / Црна Гора","Montenegro").when(df_euro_water_chem_year.country == "România","Romania").when(df_euro_water_chem_year.country == "Eesti","Estonia").when(df_euro_water_chem_year.country == "Slovensko","Slovakia").when(df_euro_water_chem_year.country == "Polska","Poland").when(df_euro_water_chem_year.country == "Северна Македонија","North Macedonia").when(df_euro_water_chem_year.country == "Slovenija","Slovenia").when(df_euro_water_chem_year.country == "Latvija","Latvia").when(df_euro_water_chem_year.country == "Deutschland","Germany").when(df_euro_water_chem_year.country == "Österreich","Austria").when(df_euro_water_chem_year.country == "Magyarország","Hungary").when(df_euro_water_chem_year.country == "Česko","Czechia").when(df_euro_water_chem_year.country == "Suomi / Finland","Finland").when(df_euro_water_chem_input.country == "Беларусь","Belarus").when(df_euro_water_chem_year.country == "Lëtzebuerg","Luxembourg").when(df_euro_water_chem_year.country == "Россия","Russia").when(df_euro_water_chem_year.country == "Italia","Italy").otherwise(df_euro_water_chem_year.country))

df_euro_water_chem_name_change.createOrReplaceTempView("table_euro_water_chem_name_change")
df_euro_water_chem_name_change = spark.sql("select country,pH,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Oxygen_saturation,Water_temperature,Total_suspended_solids,Conductivity,Phosphate,WQI,cast(from_unixtime(unix_timestamp(phenomenonTimeSamplingDate,'yyyy'),'yyyy') as int) as phenomenonTimeSamplingDate from table_euro_water_chem_name_change")
df_euro_water_chem_name_change.createOrReplaceTempView("table_euro_water_chem_name_change")
"""
Joining table_africa_india_other_per_year and Table_africa_india_chem_year_avg tables
"""
joined_table = spark.sql("select country,Year,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Oxygen_saturation,Water_temperature,Total_suspended_solids,Conductivity,Phosphate,pH,WQI,Life_expectancy,Population,GDP,GNI_per_capita,air_pollution,GNI,CO2_emissions,Agricultural_land,GPD_per_capita,HDI from table_euro_water_chem_name_change join table_euro_other_name_change  on table_euro_other_name_change.Country_name = table_euro_water_chem_name_change.country and table_euro_water_chem_name_change.phenomenonTimeSamplingDate=table_euro_other_name_change.Year order by country,Year")
joined_table.createOrReplaceTempView("table_joined")
joined_table.coalesce(1).write.mode('overwrite').option('header','true').option("compression", "gzip").csv('dbfs:/FileStore/euro_similarity_data_WQI')
