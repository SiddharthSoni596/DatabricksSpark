from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row

spark = SparkSession.builder.appName("dataframe handling").master("local[3]").getOrCreate()

data = [{"key" : 1,"value":"Hello"} , {"key":2,"value":"Byee"} , {"key":3,"value":"Tata"}]
data = [Row(**each) for each in data]
df = spark.createDataFrame(data)
print(df.printSchema())
print(df.show())