
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

if __name__ == '__main__':
	spark = SparkSession.builder.appName("My First Spark App").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")

	schema = StructType([StructField("RecordNumber", IntegerType(), True),
	StructField("Zipcode", StringType(), True),
	StructField("ZipCodeType", StringType(), True),
	StructField("City", StringType(), True),
	StructField("State", StringType(), True),
	StructField("LocationType", StringType(), True),
	StructField("Lat", StringType(), True),
	StructField("Long", StringType(), True),
	StructField("Xaxis", StringType(), True),
	StructField("Yaxis", StringType(), True),
	StructField("Zaxis", StringType(), True),
	StructField("WorldRegion", StringType(), True),
	StructField("Country", StringType(), True),
	StructField("LocationText", StringType(), True),
	StructField("Location", StringType(), True),
	StructField("Decommisioned", StringType(), True)
	])

	df = spark.readStream.schema(schema).json("file:///home/talentum/test-jupyter/Spark-Structured-Streaming/source")
	print(df.printSchema())

	groupDF = df.select("Zipcode").groupBy("Zipcode").count()
	groupDF.printSchema()

	#groupDF.writeStream.format("console").outputMode("complete").start().awaitTermination()
	df.writeStream \
  	.outputMode("append") \
  	.format("parquet") \
  	.option("path", "hdfs://localhost:9000/user/talentum/tmp/datalake/FaresRaw") \
  	.option("checkpointLocation", "hdfs://localhost:9000/user/talentum/tmp/checkpoints/FaresRaw") \
	.start().awaitTermination()

