# Project : CSE 545: Big Data Analytics Project Report
# Aim : SDG 6 - Clean Water and Sanitation
# Team Memabers:
# 1. Murthy Kaja
# 2. Yashwanth Madaka
# 3. Kowshik Sola
# 4. Nithin Reddy
# Code Description : This code is for Similarity Search among different countries. 
# Pipeline - Shingling -> Min Hashing -> LSH -> Euclidean Distance
# This code taken from CSE 545 Big Data Assignment 2 by Yashwanth Madaka - 114353641
# Cluster Used: GCP Dataproc, Ubuntu 18.04, 1 Master, 4 Worker Nodes, (n1-highmem-4, 48GB Disk)

# Model Imports
from pprint import pprint
import random
import numpy as np #for numeric algebra and arrays
import math
import hashlib
import csv
import re
import xdrlib
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf



sc = SparkContext.getOrCreate(SparkConf())

# Load CSV into Income_RDD
countries_csv=sc.textFile("mergedc1.csv",32)
# Store headers into broadcast variables
broadcast_headers = sc.broadcast(countries_csv.first().split(','))

# print("broadcast headers", broadcast_headers.value)


# Function to split a row into (k,v) => (country, set(shingles))
def rowToSet(p):
    country_name = 0
    year = 0
    x = set()
    for row in p:
        columns = []
        columns.append(row)
        array = list(csv.reader(columns, delimiter=','))[0]
        # array = row.split(",")
        if(array[0] != 'country'):
            x = set()
            country_name = array[0]
            year = array[1]
            # print("country_name",array[0])
            for i in range(2,len(broadcast_headers.value)):
                if array[i] != '0' and array[i] != '':
                    b = broadcast_headers.value[i]
                    c = array[i]
                    element = b + ":" + c
                    x.add(element)
            yield ((country_name,year), x)

# Sparse representation of Characteristic Matrix
countries_rdd = countries_csv.mapPartitions(rowToSet)
# print("check", countries_rdd.take(1))



# Implement Hash Functions:
# Hash Function implementation referred from : https://stackoverflow.com/questions/2255604/hash-functions-family-generator-in-python

_memorymask = {}
def hash_function(n):
  mask = _memorymask.get(n)
  if mask is None:
    random.seed(n)
    mask = _memorymask[n] = random.getrandbits(32)
  def myhash(x):
    return (hash(x) ^ mask) % 2**10
  return myhash

def applyHashFunction(x):
    tempList = []
    for i in range(100):
        tempList.append((x,(i,hash_function(i)(x))))
    return tempList

# Prepare UniqueShingles RDD as {(Shin_1, HashFunc), Value} where Value = HashFunc(Shin_1)
def getShingles(p):
    x = set()
    for row in p[1]:
        x.add(row)
    yield (x)

unique_shingles_rdd = countries_rdd.flatMap(getShingles)

# Function to split key into (shingle, shingle)
def split(x):
    tempList=[]
    for element in x:
        tempList.append((element, element))
    return tempList

unique_shingles_rdd = unique_shingles_rdd.flatMap(lambda x : split(x)).reduceByKey(lambda x, y: x).map(lambda x: x[0])
unique_shingles_rdd = unique_shingles_rdd.flatMap(lambda x: applyHashFunction(x))

# print("unique_shingles_rdd_sample",unique_shingles_rdd.take(1))
# print("unique shingles rdd count: ", unique_shingles_rdd.count())



# Prepare RDD as (H_Id_1, Shin_1), (H_Id_1, Shin_2)
def disperse(x):
    id = x[0]
    for element in x[1]:
        yield (element,id)

countries_rdd_flatMapped = countries_rdd.flatMap(disperse)
# print("countries_rdd_flatMapped",countries_rdd_flatMapped.take(10))


# To follow the algorithm, Iam calculating uniqueShinglesRDD and performing join on my main RDD. 
# At the end my RDD will be of the form (k,v) => ((hashFunctionNum, (countryname, year)), HashValue)
result_rdd = countries_rdd_flatMapped.join(unique_shingles_rdd).map(lambda x : ((x[1][0], x[1][1][0]), x[1][1][1])).reduceByKey(lambda x, y: min(x,y))

# print("result_rdd: ",(result_rdd.take(1)))

result_rdd_reshuffled = result_rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).sortByKey().map(lambda x : (x[1][0],[x[1][1]])).reduceByKey(lambda x, y: x+y)

print("result_rdd_reshuffled: ",(result_rdd_reshuffled.take(1)))



# Tweak this to modify your bandsize
BANDSIZE = 10

# Hash Function implementation referred from : https://stackoverflow.com/questions/2255604/hash-functions-family-generator-in-python

_memorymask_2 = {}

def hash_function_bucket(n):
  mask = _memorymask_2.get(n)
  if mask is None:
    random.seed(n)
    mask = _memorymask_2[n] = random.getrandbits(32)
  def myhash2(x):
    return (hash(x) ^ mask) % 256
  return myhash2

def applyHashBucket(string, band_num):
    return hash_function_bucket(band_num)(string)

# Function to Hash the bands and return (k,v) => ((HashValue, BandNum), Hospital_Id)
def redistribute(x):
    hospital_id = x[0]
    signature_vector = x[1]
    band_num = 0
    prev = 0
    z = set()
    for i in range(0, 100, BANDSIZE):
        value = applyHashBucket(sum(signature_vector[prev: i]), band_num)
        prev = i
        z.add(((value, band_num), hospital_id))
        band_num += 1
    yield z



# Reframe the RDD such that (k,v) => ((HashValue, BandNum), Hospital_Id)
lsh_rdd = result_rdd_reshuffled.flatMap(redistribute).flatMap(lambda x: x)


# Get the list of (Key, Values) where Values = [list of all similar hospitals] and Key = (hashValue, BandNum)
# Then Iam performing ReduceByKey on (HashValue, BandNum) which will help me to get the list of all similar hospitals which matched in that particular band. 
lsh_rdd = lsh_rdd.groupByKey().mapValues(list)

print("lsh_rdd", lsh_rdd.take(10))
# exit()


# print("check", result_rdd_reshuffled.filter(lambda x : 'Albania' in x[0][0]).map(lambda x: x[1]).reduce(lambda x, y : x+y))


# Hospital PK's to print
# countries_to_print = ["Albania", "Austria", "Belgium", "Bulgaria", "Switzerland"]
# Hospital PK's to print
countries_to_print = ["South Africa"]

def search_country_year(row,country,year):
    for r in row:
        if r[0]==country and r[1]==year:
            return True
    return False


# Function to calculate JaccardSimilarity of 2 Countries. Formula = Set Intersection / 100
def calculateJaccardSimilarity(country1, country2):
    jaccard_similarity = 0 
    setA = set(result_rdd_reshuffled.filter(lambda x : country1 in x[0][0]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    setB = set(result_rdd_reshuffled.filter(lambda x : country2 in x[0][0]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    jaccard_similarity = len(setA & setB) / len(setA.union(setB))
    return jaccard_similarity

def calculateEuclideanDistance(country1, year1, country2, year2):
    jaccard_similarity = 0 
    listA = list(result_rdd_reshuffled.filter(lambda x : country1 in x[0][0] and year1 in x[0][1]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    listB = list(result_rdd_reshuffled.filter(lambda x : country2 in x[0][0] and year2 in x[0][1]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    point1 = np.array(listA)
    point2 = np.array(listB)
    dist = np.linalg.norm(point1 - point2)
    return dist
    jaccard_similarity = len(setA & setB) / len(setA.union(setB))
    return jaccard_similarity

def calculateEuclideanDistance2(country1, year1, country2, year2):
    jaccard_similarity = 0 
    listA = list(countries_rdd.filter(lambda x : country1 in x[0][0] and year1 in x[0][1]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    listB = list(countries_rdd.filter(lambda x : country2 in x[0][0] and year2 in x[0][1]).map(lambda x: x[1]).reduce(lambda x, y : x + y))
    point1 = np.array(listA)
    point2 = np.array(listB)
    dist = np.linalg.norm(point1 - point2)
    return dist
    jaccard_similarity = len(setA & setB) / len(setA.union(setB))
    return jaccard_similarity

for country1 in countries_to_print:
    print("Calculating Similarities for Country Name: ", country1, " are ")
    year1 = '2020'
    similar_hospitals = list(set(lsh_rdd.filter(lambda x : search_country_year(x[1],country1,year1)).map(lambda x : x[1]).reduce(lambda x,y:x+y)))
    similar_hospitals = sorted(similar_hospitals, key=lambda x: int(x[1]))
    print("similar_hospitals: ",similar_hospitals)
    list_jc = []
    list_ec = []
    # exit()
    for i in range(0,len(similar_hospitals)):
        country2 = similar_hospitals[i][0]
        year2 = similar_hospitals[i][1]
        if(country1 != country2):
            # jc = calculateJaccardSimilarity(country1, country2)
            # list_jc.append((country2, year2, jc))
            # ec = calculateEuclideanDistance2(country1, year1, country2, year2)
            ec = calculateEuclideanDistance(country1, year1, country2, year2)
            list_ec.append((country2, year2, ec))
            # print("calculating")
            # print("(a)",country1, "in year", year1, " is similar to country name ", country2, " in year ", year2)
            # print("(b) Jaccard Similarity for ", str(country1), " & ", str(country2), ":", calculateJaccardSimilarity(country1, country2))
            # print("(c) First 10 values of the signature matrix of ", str(country2), " country name is : ", (result_rdd_reshuffled.filter(lambda x : country2 in x[0][0]).map(lambda x : x[1]).reduce(lambda x, y: x + y))[:10])
    # list_jc = sorted(list_jc, key=lambda x : x[2])
    # print("Similar countries", list_jc)
    list_ec = sorted(list_ec, key=lambda x : x[2])
    pprint("Similar countries")
    pprint(list_ec)