from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
    df = df.where(datediff(current_date(), col("updated_at")) < 1)
    amount = 50000
    
    # Varlıklara göre en güncel kayıtlar toplanıyor
    w = Window.partitionBy("asset_id").orderBy(desc('updated_at'))
    df = df.withColumn('Rank',dense_rank().over(w))
    df = df.filter(df.Rank == 1).drop(df.Rank)
    # Varlıklara göre en güncel kayıtlar toplanıyor
    
    # Belirlenen fiyatın üstünde olan varlıklar filtreleniyor
    dfHour = df.filter(df.price > amount)
    dfHour.select("asset_id", "change_1h", "change_24h", "price", "updated_at").show()
    # Belirlenen fiyatın üstünde olan varlıklar filtreleniyor
    
    
myspark.stop()



                
    