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
Code Description : Hypothesis Testing between cosine similarity of WQI and all other chemicals
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
import random
import numpy as np
import math
import hashlib
import csv
import sys
import json
import re
from datetime import datetime
from scipy import stats
from pyspark import SparkContext, SparkConf
africa_rdd_input_path = "/FileStore/tables/Africa_hypothesis.csv"
india_rdd_input_path = "/FileStore/tables/India_hypothesis.csv"
euro_rdd_input_path = "/FileStore/tables/euro_hypothesis.csv"

df_india_wqi_input = spark.read.load(india_rdd_input_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_india_wqi_input.createOrReplaceTempView("table_india_wqi_input")
df_india_wqi_input.limit(10).toPandas()
col_names = ["Country","date_reported","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Water_temperature","Total_suspended_solids","Conductivity","Phosphate","pH","Non_ionised_ammonia","WQI"]
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+")/count("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select Country,date_reported,"+sql2_case+" from table_india_wqi_input group by Country,date_reported"
df_india_monthly = spark.sql(sql2)
df_india_monthly.limit(10).toPandas()
df_euro_wqi_input = spark.read.load(euro_rdd_input_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_euro_wqi_input.createOrReplaceTempView("table_euro_wqi_input")
df_euro_wqi_input.limit(10).toPandas()
col_names = ["Country","date_reported","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Water_temperature","Total_suspended_solids","Conductivity","Phosphate","pH","Non_ionised_ammonia","WQI"]
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+")/count("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select country as Country,phenomenonTimeSamplingDate as date_reported,"+sql2_case+" from table_euro_wqi_input group by Country,date_reported"
df_euro_monthly = spark.sql(sql2)
df_euro_monthly.limit(10).toPandas()
df_africa_wqi_input = spark.read.load(africa_rdd_input_path,
                     format="csv", sep=",", inferSchema="true", header="true")
df_africa_wqi_input.createOrReplaceTempView("table_africa_wqi_input")
df_africa_wqi_input.limit(10).toPandas()
col_names = ["Country","date_reported","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Water_temperature","Total_suspended_solids","Conductivity","Phosphate","pH","Non_ionised_ammonia","WQI"]
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+")/count("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select Country,date_reported,"+sql2_case+" from table_africa_wqi_input group by Country,date_reported"
df_africa_monthly = spark.sql(sql2)
df_africa_monthly.limit(10).toPandas()
df_euro_monthly = df_euro_monthly.withColumn("Country", when(df_euro_monthly.Country == "Kosova / Kosovo","Kosovo").when(df_euro_monthly.Country == "Србија","Serbia").when(df_euro_monthly.Country == "Ελλάς","Greece").when(df_euro_monthly.Country == "España","Spain").when(df_euro_monthly.Country == "Κύπρος - Kıbrıs","Cyprus").when(df_euro_monthly.Country == "Schweiz/Suisse/Svizzera/Svizra","Switzerland").when(df_euro_monthly.Country == "Bosna i Hercegovina / Босна и Херцеговина","Bosnia and Herzegovina").when(df_euro_monthly.Country == "Shqipëria","Albania").when(df_euro_monthly.Country == "Éire / Ireland","Ireland").when(df_euro_monthly.Country == "Türkiye","Turkey").when(df_euro_monthly.Country == "Hrvatska","Croatia").when(df_euro_monthly.Country == "België / Belgique / Belgien","Belgium").when(df_euro_monthly.Country == "Бългaрия","Bulgaria").when(df_euro_monthly.Country == "Crna Gora / Црна Гора","Montenegro").when(df_euro_monthly.Country == "România","Romania").when(df_euro_monthly.Country == "Eesti","Estonia").when(df_euro_monthly.Country == "Slovensko","Slovakia").when(df_euro_monthly.Country == "Polska","Poland").when(df_euro_monthly.Country == "Северна Македонија","North Macedonia").when(df_euro_monthly.Country == "Slovenija","Slovenia").when(df_euro_monthly.Country == "Latvija","Latvia").when(df_euro_monthly.Country == "Deutschland","Germany").when(df_euro_monthly.Country == "Österreich","Austria").when(df_euro_monthly.Country == "Magyarország","Hungary").when(df_euro_monthly.Country == "Česko","Czechia").when(df_euro_monthly.Country == "Suomi / Finland","Finland").when(df_euro_monthly.Country == "Беларусь","Belarus").when(df_euro_monthly.Country == "Lëtzebuerg","Luxembourg").when(df_euro_monthly.Country == "Россия","Russia").when(df_euro_monthly.Country == "Italia","Italy").otherwise(df_euro_monthly.Country))
df_africa_india_monthly = df_africa_monthly.union(df_india_monthly)
df_euro_africa_india_water = df_euro_monthly.union(df_africa_india_monthly)
header = df_euro_africa_india_water.columns
rdd_euro_africa_india_water = df_euro_africa_india_water.collect()
rdd_africa_monthly = sc.parallelize(rdd_euro_africa_india_water)
rdd_africa_monthly.take(1)


"""
Hypothesis Testing
"""

def keyvaluepairs(row):
    return (row[0],[[row[1],row[2:]]])

def appending_cols(val1,val2):
    return val1+val2
header_2 = header[2:]
def final_rdd(row):
    res={}
    for i in range(0,len(header_2)):
        res[header_2[i]] = []
        for r in row[1]:
            res[header_2[i]].append(r[1][i])
    return (row[0],res)
rdd_hypothesis_counrty = rdd_africa_monthly.map(list).map(keyvaluepairs).reduceByKey(appending_cols).filter(lambda x: len(x[1])>=9).map(lambda x : (x[0],sorted(x[1],key=lambda y: y[0]))).map(final_rdd)
# rdd_hypothesis_counrty.take(1)

def mean_centric(x):
    val1=0
    n1=len(x[1])
    for j in x[1]:
        val1+=j
    m1=val1/n1
    for j in range(len(x[1])):
        x[1][j] = [x[1][j],x[1][j]-m1]
    val2=0
    n2=len(x[2])
    for j in x[2]:
        val2+=j
    m2=val2/n2
    for j in range(len(x[2])):
        x[2][j] = [x[2][j],x[2][j]-m2]
    return x

def cosine_sim(x,y):
    n=0
    x1=0
    x2=0
    for i in range(len(x)):
        n+=x[i][1]*y[i][1]
        x1+=x[i][1]*x[i][1]
        x2+=y[i][1]*y[i][1]
    return n/(math.sqrt(x1)*math.sqrt(x2)) if x1!=0 and x2!=0 else 0

def get_p_val(v1,v2):
    v1=[i[0] for i in v1]
    v2=[i[0] for i in v2]
    return stats.ttest_ind(v1, v2)[1]
"""
Similairty, p value  and Bonferroni corrected P-Value fo WQI and each other column
"""
for f in header_2:
    if f != "WQI":
        feature = f
        print(feature)
        rdd_2_features = rdd_hypothesis_counrty.map(lambda x: (x[0],x[1].get("WQI"),x[1].get(feature)))
        result = rdd_2_features.map(mean_centric).map(lambda x: (x[0],x[1],x[2],cosine_sim(x[1],x[2]))).map(lambda x: (x[0],x[3],get_p_val(x[1],x[2]))).collect()
        cosine = 0
        pvalue = 0
        l=0
        for r in result:
            cosine+= r[1]
            if r[1]!=0:
                l+=1
            pvalue+= r[2]
        cosine/=l
        pvalue/=len(result)
        print(str(cosine)+" - "+str(pvalue)+" - "+str(58*pvalue))        