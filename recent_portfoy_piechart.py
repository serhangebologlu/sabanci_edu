from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import numpy as np

input_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
output_customer_assets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.customer_assets"
input_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"
output_markets_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.markets"
pieChartList = []
pieChartLabelList = []

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



if dfMarkets.count() > 0 :
    dfMarkets = dfMarkets.where(datediff(current_date(), col("updated_at")) < 1)
    
    # Market verileri içinden müşteri varlıklarının fiyatları listeye ekleniyor
    for row in customer_asset_list:
        dfMarketBuffer = dfMarkets.filter(col("exchange_id") == str(row["exchange_id"])).filter(col("base_asset") == str(row["asset_id"]))
        foundRow = dfMarketBuffer.orderBy(desc("updated_at"))\
            .select("base_asset", "exchange_id", "updated_at", "price").first()
        if foundRow is not None:    
            pieChartList.append(foundRow.price)
            pieChartLabelList.append(foundRow.exchange_id + " " + foundRow.base_asset)
    # Market verileri içinden müşteri varlıklarının fiyatları listeye ekleniyor
    
 
    
#PieChart çiziliyor
y = np.array(pieChartList)
plt.pie(y, labels = pieChartLabelList)
plt.show() 
#PieChart çiziliyor

spark.stop()
