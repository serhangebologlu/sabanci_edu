from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


input_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
output_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
input_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"
output_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"
smallestPrice = 0
smallestAssetId = ""
smallestExchangeId = ""

def shortenDate(updatedDate):
    return updatedDate.strftime("%Y-%m-%d %H:%M")

shortenDateUDF = udf(lambda z:shortenDate(z),StringType()) 

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
    
    # Market verileri içinden müşteri varlıklarından son 24 saatte fiyatı en çok azalan bulunuyor
    for row in customer_asset_list:
        dfMarketBuffer = dfMarkets.filter(col("base_asset") == str(row["asset_id"])).filter(col("exchange_id") == str(row["exchange_id"]))
        foundRow = dfMarketBuffer.orderBy(desc("updated_at"))\
            .select("base_asset", "exchange_id", "updated_at", "price", "change_24h").first()
        # print(foundRow)
        if foundRow is not None:
            if smallestPrice > foundRow.change_24h:
                smallestPrice = foundRow.change_24h
                smallestAssetId = foundRow.base_asset
                smallestExchangeId = foundRow.exchange_id
    # Market verileri içinden müşteri varlıklarından son 24 saatte fiyatı en çok azalan bulunuyor
    
    # Bulunan varlığın son 24 saatteki fiyatları alınıyor    
    dfMarkets = dfMarkets.filter(col("exchange_id") == smallestExchangeId).filter(col("base_asset") == smallestAssetId)
    dfMarkets = dfMarkets.withColumn("updated_at_modified", shortenDateUDF(col("updated_at")).cast(TimestampType()))
    dfMarkets = dfMarkets.select("exchange_id","base_asset","updated_at_modified", "price", "hour_minute")
    # Bulunan varlığın son 24 saatteki fiyatları alınıyor
    
    # Çizim yapılıyor
    if dfMarkets.count() > 0:
        df = pd.DataFrame(dfMarkets.toPandas())
        # df["updated_at_modified"] = pd.to_datetime(df['updated_at_modified']).dt.time
        print(df)
        ax = df.plot(x='hour_minute', y='price')
        ax.set(xlabel='Tarih', ylabel='Fiyat')
        plt.legend(["Fiyat"])
        plt.show()
    # Çizim yapılıyor

spark.stop()
