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
Code Description : Extracting country name from Latitude and longitude fro stations in europe region.
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
from geopy.geocoders import Nominatim
import multiprocessing as mp
pd.set_option("display.max_columns",None)
df = spark.read.load("hdfs:///data/Waterbase_v2018_1_WISE4_MonitoringSite_DerivedData.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
df.na.drop()
df1 = df.select(col('lon'),col('lat'))
df2 = df1.na.drop("any")
df3 = df2.distinct()
df3.toPandas().to_csv (r'/locations_unique.csv', index = False, header=True)
geolocator = Nominatim(timeout=10,user_agent="geoapiExercises")
df_pd = pd.read_csv('/location_country.csv')
def fun(row):
  return geolocator.reverse(str(row['lat'])+","+str(row['lon'])).raw['address']
def fun2(row):
  return row['address'].get('country', '')
df_pd["address"] = df_pd.apply(fun, axis=1)
df_pd["country"] = df_pd.apply(fun2,axis=1)
df_pd.to_csv (r'/location_country_all.csv', index = False, header=False)