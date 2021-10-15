import requests
import json
import pymongo
import pprint
import datetime

from pymongo import MongoClient
client = MongoClient("mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu")
size=50
listMarkets = []
now = datetime.datetime.now()

def addZeroToHourMinute(param):
    if len(param) == 1:
        return "0" + param
    return param

hour = addZeroToHourMinute(str(now.hour))
minute = addZeroToHourMinute(str(now.minute))


def connectToMongoDB(client):
    client.server_info()
    db = client["sabanci_edu"]
    marketsTable = db["markets"]
    return marketsTable

marketsTable = connectToMongoDB(client)

def getRequest(url):
    marketResponse = requests.get(url)
    market_obj = json.loads(marketResponse.text)
    market_arr = market_obj["markets"]
    paging = market_obj["next"]
    return (market_arr, paging)

def convertMarketToMongoDBMarket(market):
    marketInsert = {
      "exchange_id":  market["exchange_id"],
      "symbol": market["symbol"],
      "base_asset": market["base_asset"],
      "quote_asset": market["quote_asset"],
      "price_unconverted": market["price_unconverted"],
      "price": market["price"],
      "change_24h": market["change_24h"],
      "spread": market["spread"],
      "volume_24h": market["volume_24h"],
      "status": market["status"],
      "created_at": datetime.datetime.strptime(str(market["created_at"])[:19], '%Y-%m-%dT%H:%M:%S'),
      "updated_at": datetime.datetime.strptime(str(market["updated_at"]), '%Y-%m-%dT%H:%M:%S.%f'),
      "hour_minute": hour + ":" + minute
      }
    return marketInsert
    
def insertManyToMongoDB(markets):
    result = marketsTable.insert_many(markets)
    print(f"Many tutorial: {result.inserted_ids}")

getResponse = ("0", "0")
    
while getResponse[1].isdigit():
    getResponse = getRequest("https://www.cryptingup.com/api/assets/USD/markets?start=" + getResponse[1] + "&size=" + str(size))
    for market in getResponse[0]:
        listMarkets.append(convertMarketToMongoDBMarket(market))
        
insertManyToMongoDB(listMarkets)    

client.close()

