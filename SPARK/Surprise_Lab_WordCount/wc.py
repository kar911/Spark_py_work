#!/usr/bin/python

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

import pyspark.sql.functions as F

df1 = spark.read.text("hdfs:///user/talentum/constitution.txt")
print(df1.printSchema())
type(df1)

#df1.show(5, truncate = False)
df1.show(10)
df2 = df1.withColumn("split", F.split(F.col('value'), "\s+"))
df2.show(10)
df2.drop(F.col('value')).select(F.explode(F.col('split')).alias("Word")).show(10)
# .filter(~F.col('Word').isin(stop_words)).groupBy(F.col('Word')).count() \
# .sort(F.col('count').desc()).withColumnRenamed('count', 'Freq')
# df2.show(20, truncate = False)

#df1.show(5, truncate = False)
# df1.show(10)
df2 = df1.withColumn("split", F.split(F.col('value'), "\s+")) \
.drop(F.col('value')).select(F.explode(F.col('split')).alias("Word")) \
.filter(~F.col('Word').isin(stop_words)).groupBy(F.col('Word')).count() \
.sort(F.col('count').desc()).withColumnRenamed('count', 'Freq')
df2.show(20, truncate = False)

#df1.show(5, truncate = False)
# df1.show(10)
df2 = df1.withColumn("split", F.split(F.col('value'), "\s+")) \
.drop(F.col('value')).select(F.explode(F.col('split')).alias("Word")) \
.groupBy(F.col('Word')).count() \
.sort(F.col('count').desc()).withColumnRenamed('count', 'Freq')
df2.show(20, truncate = False)
