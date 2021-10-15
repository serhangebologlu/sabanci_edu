from pyspark.sql import SparkSession
from pyspark.sql.functions import *

input_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.assets"
output_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.assets"

myspark = SparkSession\
    .builder.appName("deneme")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2")\
    .getOrCreate()
    
df = myspark.read.format("com.mongodb.spark.sql.DefaultSource").load()

if df.count() > 0:
    df24Hours = df.where(datediff(current_date(), col("updated_at")) < 1)
    
    # Son 24 saat içinde kayıp olan varlıklar filtreleniyor
    df24Hours = df24Hours.filter("change_24h < 0")
    # Son 24 saat içinde kayıp olan varlıklar filtreleniyor
    
    # En çok kayıp olan varlık sıralama yapılarak bulunuyor
    df24Hours.orderBy(asc("change_24h")).select("asset_id", "updated_at", "change_24h")\
        .show(1)
    # En çok kayıp olan varlık sıralama yapılarak bulunuyor


myspark.stop()


                
    