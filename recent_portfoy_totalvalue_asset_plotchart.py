from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


input_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
output_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
input_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"
output_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"

# Müşteri varlıkları çekiliyor
spark = SparkSession\
.builder.appName("customer_asset")\
.config("spark.mongodb.input.uri", input_customer_assets_uri)\
.config("spark.mongodb.output.uri", output_customer_assets_uri)\
.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2")\
.getOrCreate()
    
dfCustomerAssets = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
customer_asset_list = dfCustomerAssets.collect()
spark.stop()
# Müşteri varlıkları çekiliyor

# Market verileri çekiliyor
spark = SparkSession\
.builder.appName("markets")\
.config("spark.mongodb.input.uri", input_markets_uri)\
.config("spark.mongodb.output.uri", output_markets_uri)\
.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2")\
.getOrCreate()

dfMarkets = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# Market verileri çekiliyor


if dfMarkets.count() > 0:
    dfMarkets = dfMarkets.where(datediff(current_date(), col("updated_at")) < 1)
    
    # Verileri toplayacağımız boş DataFrame yaratılıyor
    emptyRDD = spark.sparkContext.emptyRDD()
            
    bufferSchema = StructType([
          StructField('id', StructType([
                 StructField('oid', StringType(), True)
                 ])),
          StructField('base_asset', StringType(), True),
          StructField('change_24h', DoubleType(), True),
          StructField('created_at', TimestampType(), True),
          StructField('exchange_id', StringType(), True),
          StructField('hour_minute', StringType(), True),
          StructField('price', DoubleType(), True),
          StructField('price_unconverted', DoubleType(), True),
          StructField('quote_asset', StringType(), True),
          StructField('spread', DoubleType(), True),
          StructField('status', DoubleType(), True),
          StructField('symbol', DoubleType(), True),
          StructField('updated_at', TimestampType(), True),
          StructField('volume_24h', DoubleType(), True)
      ])
    
    dfMarketFinal = spark.createDataFrame(data = emptyRDD, schema = bufferSchema)
    # Verileri toplayacağımız boş DataFrame yaratılıyor
    
    #Müşteri varlıkları boş DataFrame'de toplanıyor
    for row in customer_asset_list:
        dfMarketBuffer = dfMarkets.filter(col("exchange_id") == str(row["exchange_id"])).filter(col("base_asset") == str(row["asset_id"]))
        dfMarketFinal = dfMarketFinal.union(dfMarketBuffer)
    #Müşteri varlıkları boş DataFrame'de toplanıyor
    
    #Toplanan müşteri varlıkları saat dakikaya göre gruplanıp toplamı alınıyor
    dfMarketFinal = dfMarketFinal.groupBy("hour_minute").agg(sum('price').alias('sum_price')).orderBy("hour_minute")
    dfMarketFinal.show()
    #Toplanan müşteri varlıkları saat dakikaya göre gruplanıp toplamı alınıyor
    
    # Çizim yapılıyor
    if dfMarketFinal.count() > 0:
        df = pd.DataFrame(dfMarketFinal.toPandas())
        ax = df.plot(x='hour_minute', y='sum_price')
        ax.set(xlabel='Tarih', ylabel='Fiyat')
        plt.legend(["Fiyat"])
        plt.show()
    # Çizim yapılıyor
    
spark.stop()
