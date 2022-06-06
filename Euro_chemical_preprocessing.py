"""
Project : CSE 545: Big Data Analytics Project Report
Aim : SDG 6 - Clean Water and Sanitation
Team Memabers:
1. Murthy Kaja
2. Yashwanth M
3. Kowshik S
4. Nithin Reddy
Cluster Used: GCP Dataproc, Ubuntu 18.04, 1 Master, 4 Worker Nodes, (n1-highmem-4, 48GB Disk)
Code Description : Using Spark Data frames and Rdd for data preprocessing. This includes columnar data to row format and replacing missingvalues with median. For europe data
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
df = spark.read.load("hdfs:///data/Waterbase_v2018_1_T_WISE4_DisaggregatedData.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
list_chemicals = ["EEA_3152-01-0","CAS_14265-44-2","EEA_3112-01-4","CAS_7439-89-6","CAS_14797-55-8","CAS_16887-00-6","CAS_7440-23-5","EEA_3132-01-2","EEA_3131-01-9","EEA_3121-01-5","EEA_31-02-7","CAS_7664-41-7","EEA_31613-01-1","EEA_3142-01-6"]
df = df.filter(df.observedPropertyDeterminandCode.isin(list_chemicals))
df.createOrReplaceTempView("just_after_FilteredTable")
df_required_columns = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory,observedPropertyDeterminandCode,resultUom,phenomenonTimeSamplingDate,resultObservedValue from just_after_FilteredTable")
df_required_columns.createOrReplaceTempView("Filtered_columns")
print(spark.sql("select observedPropertyDeterminandCode,resultUom,count(1) from just_after_FilteredTable group by observedPropertyDeterminandCode,resultUom order by observedPropertyDeterminandCode").toPandas())
df_p_value = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory,observedPropertyDeterminandCode,case resultUom when 'mg{PO4}/L' then 'mg{P}/L' else resultUom end as resultUom,phenomenonTimeSamplingDate,case resultUom when 'mg{PO4}/L' then resultObservedValue*0.3262 else resultObservedValue end as resultObservedValue from Filtered_columns")
df_p_value.createOrReplaceTempView("Filtered_P")
df_mg_ug = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory,observedPropertyDeterminandCode,case resultUom=='mg/L' and observedPropertyDeterminandCode=='CAS_7439-89-6' when true then 'ug/L' else resultUom end as resultUom,phenomenonTimeSamplingDate,case resultUom=='mg/L' and observedPropertyDeterminandCode=='CAS_7439-89-6' when true then resultObservedValue*1000 else resultObservedValue end as resultObservedValue from Filtered_P")
df_mg_ug.createOrReplaceTempView("Filtered_mg_ug")
df_Ammonia = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory, case observedPropertyDeterminandCode == 'CAS_7664-41-7' when true then 'EEA_31613-01-1' else observedPropertyDeterminandCode end as observedPropertyDeterminandCode,resultUom,phenomenonTimeSamplingDate,case observedPropertyDeterminandCode when 'CAS_7664-41-7' then resultObservedValue/0.106 else resultObservedValue end as resultObservedValue from Filtered_mg_ug")
df_Ammonia.createOrReplaceTempView("Filtered_Ammonia")
df_mmol_mg = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory, observedPropertyDeterminandCode, case resultUom=='mmol/L' when true then 'mg/L' else resultUom end as resultUom,phenomenonTimeSamplingDate,case resultUom when 'mmol/L' then resultObservedValue*18 else resultObservedValue end as resultObservedValue from Filtered_Ammonia")
df_mmol_mg.createOrReplaceTempView("Filtered_mmol_mg")
df_O2 = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory, observedPropertyDeterminandCode, case observedPropertyDeterminandCode=='EEA_3132-01-2' when true then 'mg{O2}/L' else resultUom end as resultUom,phenomenonTimeSamplingDate,resultObservedValue from Filtered_mmol_mg")
df_O2.createOrReplaceTempView("Filtered_O2")
df_normalized = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory, observedPropertyDeterminandCode,case resultUom=='S/m' when true then 'uS/cm' else resultUom end as resultUom,phenomenonTimeSamplingDate,case resultUom=='S/m' when true then resultObservedValue*10000 else resultObservedValue end as resultObservedValue from Filtered_mmol_mg")
df_normalized.createOrReplaceTempView("Filtered_normalized")
df_normalized = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory,observedPropertyDeterminandCode,phenomenonTimeSamplingDate,resultObservedValue from Filtered_normalized")
df_normalized.limit(10).toPandas()
print(spark.sql("select observedPropertyDeterminandCode,resultUom,count(1) from Filtered_normalized group by observedPropertyDeterminandCode,resultUom order by observedPropertyDeterminandCode").toPandas())
col_names=["monitoringSiteIdentifier","phenomenonTimeSamplingDate","pH","Phosphate","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Oxygen_saturation","Water_temperature","Total_suspended_solids","Non_ionised_ammonia","Conductivity"]
df_normalized = spark.sql("select monitoringSiteIdentifier,observedPropertyDeterminandCode,phenomenonTimeSamplingDate,resultObservedValue from Filtered_normalized")
df_normalized.createOrReplaceTempView("Table_Filtered_normalized")
df_per_day_avg = spark.sql("select monitoringSiteIdentifier,phenomenonTimeSamplingDate,observedPropertyDeterminandCode,sum(resultObservedValue)/count(1) as value from Table_Filtered_normalized group by monitoringSiteIdentifier,phenomenonTimeSamplingDate,observedPropertyDeterminandCode")
df_per_day_avg.createOrReplaceTempView("Table_per_day_avg")
# df_per_day_avg.limit(10).toPandas()


sql_case=""
case_str_1 = " case true when observedPropertyDeterminandCode == '"
case_str_2 = "' then value else 0 end "
# col_names = ["Station","date_reported","BOD","Chloride","Conductivity","Iron","Non_ionised_ammonia","Nitrate","Sodium","Dissolved_oxygen","Oxygen_saturation","Water_temperature","Phosphate","Total_suspended_solids","pH"]
chem_names = ["EEA_3152-01-0","CAS_14265-44-2","CAS_7439-89-6","CAS_14797-55-8","CAS_16887-00-6","CAS_7440-23-5","EEA_3132-01-2","EEA_3131-01-9","EEA_3121-01-5","EEA_31-02-7","EEA_31613-01-1","EEA_3142-01-6"]
i=2
for chem in chem_names:
    sql_case+=case_str_1+chem+case_str_2+" as "+col_names[i]+" ,"
    i+=1
sql_case = sql_case[:-1]
sql_all = "select monitoringSiteIdentifier,phenomenonTimeSamplingDate,"+sql_case+" from Table_per_day_avg"
# print(sql_all)
df_cols_rows = spark.sql(sql_all)
df_cols_rows.createOrReplaceTempView("Table_cols_rows")
sql2 = ""
sql2_case = ""
for cols in col_names[2:]:
    sql2_case+= " sum("+cols+") as "+cols+" ,"
sql2_case=sql2_case[:-1]
sql2 = "select monitoringSiteIdentifier,phenomenonTimeSamplingDate,"+sql2_case+" from Table_cols_rows group by monitoringSiteIdentifier,phenomenonTimeSamplingDate"
# print(sql2)
df_per_day = spark.sql(sql2)
df_per_day.withColumn('phenomenonTimeSamplingDate', to_timestamp('phenomenonTimeSamplingDate').cast('string')).withColumn('phenomenonTimeSamplingDate', substring('phenomenonTimeSamplingDate', 1,7)).createOrReplaceTempView("Table_row_month")
sql3=""
sql3_case = ""
for cols in col_names[2:]:
    sql3_case += " sum("+cols+") as "+cols+" ,"
sql3_case = sql3_case[:-1]
sql3 = "select monitoringSiteIdentifier,phenomenonTimeSamplingDate,"+sql3_case+" from Table_row_month group by monitoringSiteIdentifier,phenomenonTimeSamplingDate"
df_per_month = spark.sql(sql3)
df_per_month.createOrReplaceTempView("Table_per_month")
print(df_per_month.limit(10).toPandas())
df_final = spark.sql("select monitoringSiteIdentifier,parameterWaterBodyCategory,observedPropertyDeterminandCode,phenomenonTimeSamplingDate,resultObservedValue from Filtered_normalized")
input_rdd = df_final.rdd
"""
0 - pH - EEA_3152-01-0
1 - Phosphate - CAS_14265-44-2
2 -Iron - CAS_7439-89-6
3 -Nitrate - CAS_14797-55-8
4 -Chloride- CAS_16887-00-6
5 -Sodium - CAS_7440-23-5
6 -Dissolved_oxygen - EEA_3132-01-2
7 -Oxygen_saturation - EEA_3131-01-9
8 -Water_temperature - EEA_3121-01-5
9 -Total_suspended_solids - EEA_31-02-7
10 -Non_ionised_ammonia - EEA_31613-01-1
11 -Conductivity - EEA_3142-01-6
"""
value_index={"EEA_3152-01-0":0,
             "CAS_14265-44-2":1,
             "CAS_7439-89-6":2,
             "CAS_14797-55-8":3,
             "CAS_16887-00-6":4,
             "CAS_7440-23-5":5,
             "EEA_3132-01-2":6,
             "EEA_3131-01-9":7,
             "EEA_3121-01-5":8,
             "EEA_31-02-7":9,
             "EEA_31613-01-1":10,
             "EEA_3142-01-6":11}
def newcolumns(row):
    key=(row.monitoringSiteIdentifier,row.parameterWaterBodyCategory,row.phenomenonTimeSamplingDate)
    value=[0]*12
    if row.resultObservedValue is not None:
        value[value_index.get(row.observedPropertyDeterminandCode)]=float(row.resultObservedValue)
    else:
        value[value_index.get(row.observedPropertyDeterminandCode)]=0
    return (key,[value,0])
    
def removetime(row):
    key=(row[0][0],row[0][1],row[0][2][:10])
    return (key,row[1])
def removedate(row):
    key=(row[0][0],row[0][1],row[0][2][:7])
    return (key,row[1])
    
def keep_month_year_only(row):
    return [row.monitoringSiteIdentifier,row.parameterWaterBodyCategory,row.observedPropertyDeterminandCode,row.phenomenonTimeSamplingDate[:7],row.resultObservedValue]

def newcolumns_list(row):
    key=(row[0],row[1],row[2],row[3])
    value=[[0,0]]*11
    if row.resultObservedValue is not None:
        value[value_index.get(row.observedPropertyDeterminandCode)][0]=float(row.resultObservedValue)
        value[value_index.get(row.observedPropertyDeterminandCode)][1]=1
    else:
        value[value_index.get(row.observedPropertyDeterminandCode)]=-1
    return (key,value)

def mean_values_month(row):
    key=row[0]
    value=row[1][0]
    if row[1][1]!=0:
        for i in range(len(value)):
            value[i]/=row[1][1]
    return (list(key)+value)


col_names=["monitoringSiteIdentifier","parameterWaterBodyCategory","phenomenonTimeSamplingDate","pH","Phosphate","Iron","Nitrate","Chloride","Sodium","Dissolved_oxygen","Oxygen_saturation","Water_temperature","Total_suspended_solids","Non_ionised_ammonia","Conductivity"]
df_month_year_data = input_rdd.map(newcolumns).reduceByKey(lambda a,b: [[x + y for x, y in zip(a[0], b[0])],2]).map(removetime).map(removedate).map(mean_values_month).toDF(col_names)
print(df_month_year_data.limit(10).toPandas())
df_month_year_data.createOrReplaceTempView("table_chemical_composotion")
df_loc = spark.read.load("hdfs:///data/location_country_all.csv",
                     format="csv", sep=",", inferSchema="true", header="false")
# df_loc.columns = ['lon', 'lat', 'country']
df_loc = df_loc.withColumnRenamed("_c0","lon").withColumnRenamed('_c1', 'lat').withColumnRenamed('_c2', 'country')
df_loc.createOrReplaceTempView("loc_country")
print(df_loc.limit(10).toPandas())
df_mon_ll = spark.read.load("hdfs:///data/Waterbase_v2018_1_WISE4_MonitoringSite_DerivedData.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
df_mon_ll = df_mon_ll.na.drop("any")
df_mon_ll = df_mon_ll.select("monitoringSiteIdentifier","lon","lat")
df_mon_ll.createOrReplaceTempView("mon_lon_lat")
print(df_mon_ll.limit(10).toPandas())
df_mon_country = spark.sql("select a.monitoringSiteIdentifier,b.country from mon_lon_lat a join loc_country b on  a.lon=b.lon and a.lat=b.lat")
df_mon_country.createOrReplaceTempView("mon_country")
print(df_mon_country.limit(10).toPandas())
df_chem_location = spark.sql("select country,phenomenonTimeSamplingDate,pH,Phosphate,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Oxygen_saturation,Water_temperature,Total_suspended_solids,Non_ionised_ammonia,Conductivity from table_chemical_composotion a join mon_country b on a.monitoringSiteIdentifier = b.monitoringSiteIdentifier")
df_chem_location.createOrReplaceTempView("table_chem_loc")
print(df_chem_location.limit(10).toPandas())
col_repalce_mean = ['pH','Phosphate','Iron','Nitrate','Chloride','Sodium','Dissolved_oxygen','Oxygen_saturation','Water_temperature','Total_suspended_solids','Non_ionised_ammonia','Conductivity']
df_chem_location = df_chem_location.select([when(col(c)==0,None).otherwise(col(c)).alias(c) for c in df_chem_location.columns])
for column_mean in col_repalce_mean:
    df_mean = df_chem_location.fillna(value=0, subset=[column_mean]).select(mean(col(column_mean)).alias('avg')).collect()
    avg = df_mean[0]['avg']
    df_chem_location = df_chem_location.withColumn(column_mean, F.udf(lambda x: avg if x is None else x)(F.col(column_mean)))
    df_chem_location = df_chem_location.withColumn(column_mean, F.round(df_chem_location[column_mean], 2))
# df.show()
df_chem_location.createOrReplaceTempView("table_chem_loc_nonull")
print(df_chem_location.limit(10).toPandas())
df_temp_wqi = spark.sql("select *,case true when Water_temperature<20 then 1 when Water_temperature>40 then 0 else 1-(Water_temperature/20-1) end as Ttemp,Oxygen_saturation/100 as TO2, case true when Total_suspended_solids>250 then 0 else 1-(Total_suspended_solids/250) end as Ttss,case true when Dissolved_oxygen>25 then 1 else Dissolved_oxygen/25 end as TDO,case true when Conductivity<200 then 1 when Conductivity>4000 then 0 else 1-((Conductivity-200)/3800) end as Tcond from table_chem_loc_nonull ")
df_temp_wqi.createOrReplaceTempView("table_temp_wqi")
# df_wqi_euro = spark.sql("select country,phenomenonTimeSamplingDate,pH,Phosphate,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Oxygen_saturation,Water_temperature,Total_suspended_solids,Non_ionised_ammonia,Conductivity,Ttemp*(30*TO2+25*Ttss+25*TDO+20*Tcond) as WQI from table_temp_wqi")
df_wqi_euro = spark.sql("select country,phenomenonTimeSamplingDate,pH,Phosphate,Iron,Nitrate,Chloride,Sodium,Dissolved_oxygen,Oxygen_saturation,Water_temperature,Total_suspended_solids,Non_ionised_ammonia,Conductivity,(30*Ttemp+25*Ttss+25*TDO+20*Tcond) as WQI from table_temp_wqi")
df_wqi_euro.limit(10).toPandas()
df_wqi_euro.coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:///data/euro_wqi.csv')




