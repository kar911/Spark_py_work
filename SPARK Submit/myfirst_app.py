#!/usr/bin/python3

#Entrypoint 2.x
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

# On yarn:
# spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().master("yarn").getOrCreate()
# specify .master("yarn")

sc = spark.sparkContext

print('--------------------------')
print(sc.parallelize([('a',['apple','banana','lemon']),('b',["grapes"])]).mapValues(lambda x : len(x)).collect())
print('--------------------------')
print(sc.master)
print('--------------------------')
