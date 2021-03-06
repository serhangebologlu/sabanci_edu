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

if df.count() > 0 :
    df24Hours = df.where(datediff(current_date(), col("updated_at")) < 1)
    
    # AssetId'ye göre gruplama yaparak en yüksek ve en düşük fiyatlar bulunuyor
    df24Hours = df24Hours.groupBy('asset_id').agg(min('price').alias('min_price'), max('price').alias('max_price'))
    # AssetId'ye göre gruplama yaparak en yüksek ve en düşük fiyatlar bulunuyor
    
    # En yüksek ve en düşük fiyat arasındaki fark ayrı bir kolona yazılıyor
    df24Hours = df24Hours.withColumn("diff_price", df24Hours.max_price - df24Hours.min_price)
    # En yüksek ve en düşük fiyat arasındaki fark ayrı bir kolona yazılıyor
    
    # Yüksekten düşüğe sıralama yapılıp en büyük fark olan ilk kayıt alınıyor
    df24Hours.orderBy(desc("diff_price")).select("asset_id", "diff_price").show(1)
    # Yüksekten düşüğe sıralama yapılıp en büyük fark olan ilk kayıt alınıyor


myspark.stop()







                
    