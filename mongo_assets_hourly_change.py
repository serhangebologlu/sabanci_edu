from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

input_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.assets"
output_uri = "mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu.assets"

myspark = SparkSession\
    .builder.appName("deneme")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.2")\
    .getOrCreate()
    
df = myspark.read.format("com.mongodb.spark.sql.DefaultSource").load()

followingAssets = ["NEAR", "MBOX", "ARPA"]

# followingAssets = "MBOX"

def shortenDate(updatedDate):
    return updatedDate.strftime("%Y-%m-%d %H:%M:%S")

shortenDateUDF = udf(lambda z:shortenDate(z),StringType()) 

df24Hours = df.where(datediff(current_date(), col("updated_at")) < 1).filter("change_24h < 0")

foundRow = df24Hours.orderBy(asc("change_24h")).select("asset_id", "updated_at").first()

# print(foundRow)

foundRowControlTime = foundRow.updated_at.strftime("%Y-%m-%d %H:%M:%S")

df24Hours = df24Hours.withColumn("control_date", lit(foundRowControlTime).cast(TimestampType()))

df24Hours = df24Hours.withColumn("updated_at_modified", shortenDateUDF(col("updated_at")).cast(TimestampType()))

# df24Hours.printSchema()

# df24Hours.show(10)

df24Hours = df24Hours.filter("control_date == updated_at_modified").filter(df24Hours.asset_id.isin(followingAssets))

# df24Hours = df24Hours.filter("control_date == updated_at_modified").filter(df24Hours.asset_id.isin(followingAssets))

df24Hours.select("asset_id", "change_1h", "updated_at_modified", "control_date").show()








                
    