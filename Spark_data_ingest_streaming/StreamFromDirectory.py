
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

if __name__ == '__main__':
	spark = SparkSession.builder.appName("Covid_data").enableHiveSupport().getOrCreate()
	spark.sparkContext.setLogLevel("ERROR")
	#|-- DeadCounter: integer (nullable = true)
	#|-- CaseCounter: integer (nullable = true)
	#|-- RecoveredCounter: integer (nullable = true)
	#|-- CriticalCounter: integer (nullable = true)
	#|-- Date: string (nullable = true)	

	schema = StructType([StructField("DeadCounter", IntegerType(), True),
StructField("CaseCounter", IntegerType(), True),
StructField("RecoveredCounter", IntegerType(), True),
StructField("CriticalCounter", IntegerType(), True),
	StructField("Date", StringType(), True)
	])

	df = spark.readStream.schema(schema).csv("file:///home/talentum/Spark_data_ingeest/source",header=True)
	print(df.printSchema())
	df.writeStream \
  	.outputMode("append") \
  	.format("parquet") \
  	.option("path", "hdfs://localhost:9000/user/talentum/tmp/datalake/Covid_data") \
  	.option("checkpointLocation", "hdfs://localhost:9000/user/talentum/tmp/checkpoints/Covid_data") \
	.start().awaitTermination()

