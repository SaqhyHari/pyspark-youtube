from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("Youtube_Data_Analysis").getOrCreate()
file_path = "/home/hari/youtube/youtubevideostat.txt"
schema1 = StructType([
    StructField("f1", StringType(), True),
    StructField("f2", StringType(), True),
    StructField("f3", StringType(), True),
    StructField("f4", IntegerType(), True),
    StructField("f5", IntegerType(), True),
    StructField("f6", IntegerType(), True),
    StructField("f7", IntegerType(), True),
    StructField("f8", IntegerType(), True),
    StructField("f9", IntegerType(), True),
    StructField("f10", StringType(), True),
    StructField("f11", StringType(), True),
    StructField("f12", StringType(), True),
    StructField("f13", StringType(), True),
    StructField("f14", StringType(), True),
    StructField("f15", StringType(), True),
    StructField("f16", StringType(), True),
    StructField("f17", StringType(), True),
    StructField("f18", StringType(), True),
    StructField("f19", StringType(), True),
    StructField("f20", StringType(), True),
    StructField("f21", StringType(), True),
    StructField("f22", StringType(), True),
    StructField("f23", StringType(), True)])
df_read = spark.read \
 .option("delimiter", "\t") \
 .schema(schema1) \
 .option("inferSchema", "True") \
 .csv(file_path)
df_read.createOrReplaceTempView("youtube_data") # create temporary table
df_read.printSchema()
df1 = spark.sql('''select f1 as vdo_id, f2 as vdo_uploaded, f3 as vdo_category, f4 as vdo_interval, f5 vdo_dummy, f6 as vdo_comm, f7 as vdo_likes, f8 as vdo_views , f9 as vdo_dislikes, f10  as vdo_rel_id from youtube_data''')
df1.createOrReplaceTempView("col_4")
df1.show()
start=input("Starting like count:")
end=input("Ending like count:")
df3 = spark.sql('''select distinct vdo_id, vdo_likes from col_4 where vdo_likes between '%s' and '%s' '''%(start,end))
df3.coalesce(1).write.option("delimiter",",").option("header", "true").mode("overwrite").csv('/home/hari/youtube/vdoRating')
df3.show()
