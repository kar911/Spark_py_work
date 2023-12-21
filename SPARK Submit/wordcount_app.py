#!/usr/bin/python3

#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext

print('--------------------------')
xx=sc.textFile('file:///home/talentum/selfishgiant.txt').flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x:x[1], ascending=False)
print('--------------------------')
print(xx.take(5))
print('--------------------------')
print(sc.master)
print('--------------------------')
