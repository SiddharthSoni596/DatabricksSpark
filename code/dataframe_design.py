from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr , col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark = SparkSession.builder.appName("dataframe handling").master("local[3]").getOrCreate()

# schema = '''DEST_COUNTRY_NAME String , ORIGIN_COUNTRY_NAME String ,count Int'''
schema = StructType([
    StructField("DEST_COUNTRY_NAME",StringType(),True),
    StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
    StructField("count",IntegerType(),True),
])
df = spark.read.csv("/Users/siddharth/PycharmProjects/DatabricksPySpark/data/flight_data.csv",header=True,schema=schema)

# print(df.printSchema())
# print(df.count())
# print(df.schema)
# print(df.show(5))
# vr = df.take(10)[0]
# print(vr,vr["ORIGIN_COUNTRY_NAME"])

# print(df.select("ORIGIN_COUNTRY_NAME","count",lit(1)).show())
# select ORIGIN_COUNTRY_NAME,count,1 from table
# select ORIGIN_COUNTRY_NAME as origin,count,1 from table
# print(df.explain())
# print("#"*10)
# print(df.select(expr("ORIGIN_COUNTRY_NAME as origin"),"count",lit(1)).show())
# print(df.select(col("ORIGIN_COUNTRY_NAME").alias("origin"),"count",lit(1)).show())
# print(df.select(expr("*")).show())
df_lower_case_origin = df.withColumn("Origin Lower Case Font" , expr("lower(ORIGIN_COUNTRY_NAME)"))
# df_lower_case_origin = df.withColumn("Origin Lower Case Font" , col("ORIGIN_COUNTRY_NAME").)
df_renamed = df.withColumnRenamed("DEST_COUNTRY_NAME","Dest").\
                withColumnRenamed("ORIGIN_COUNTRY_NAME","Origin").\
                where(col("count") < 200).\
                distinct()
print(df_renamed.show())
