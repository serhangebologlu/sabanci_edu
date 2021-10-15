import requests
import json
import pymongo
import pprint
import datetime

from pymongo import MongoClient
client = MongoClient("mongodb+srv://serhan:MFeYFBP26zU7A8SQ@cluster0.vwshb.mongodb.net/sabanci_edu")
size=50
listAssets = []

def connectToMongoDB(client):
    client.server_info()
    db = client["sabanci_edu"]
    assetsTable = db["assets"]
    return assetsTable

assetsTable = connectToMongoDB(client)

def getRequest(url):
    assetResponse = requests.get(url)
    asset_obj = json.loads(assetResponse.text)
    asset_arr = asset_obj["assets"]
    paging = asset_obj["next"]
    return (asset_arr, paging)

def insertToMongoDB(asset):
    assetInsert = {
      "asset_id":  asset["asset_id"],
      "name": asset["name"],
      "price": asset["price"],
      "volume_24h": asset["volume_24h"],
      "change_1h": asset["change_1h"],
      "change_24h": asset["change_24h"],
      "change_7d": asset["change_7d"],
      "status": asset["status"],
      "created_at": datetime.datetime.strptime(asset["created_at"], '%Y-%m-%dT%H:%M:%S.%f'),
      "updated_at": datetime.datetime.strptime(asset["updated_at"], '%Y-%m-%dT%H:%M:%S.%f')
      }

    result = assetsTable.insert_one(assetInsert)

    print(f"One tutorial: {result.inserted_id}")
    
def convertAssetToMongoDBAsset(asset):
    assetInsert = {
      "asset_id":  asset["asset_id"],
      "name": asset["name"],
      "price": asset["price"],
      "volume_24h": asset["volume_24h"],
      "change_1h": asset["change_1h"],
      "change_24h": asset["change_24h"],
      "change_7d": asset["change_7d"],
      "status": asset["status"],
      "created_at": datetime.datetime.strptime(str(asset["created_at"])[:19], '%Y-%m-%dT%H:%M:%S'),
      "updated_at": datetime.datetime.strptime(str(asset["updated_at"]), '%Y-%m-%dT%H:%M:%S.%f')
      }
    return assetInsert
    
def insertManyToMongoDB(assets):
    result = assetsTable.insert_many(assets)
    print(f"Many tutorial: {result.inserted_ids}")


getResponse = ("0", "0")
    
while getResponse[1].isdigit():
    getResponse = getRequest("https://www.cryptingup.com/api/assets?start=" + getResponse[1] + "&size=" + str(size))
    for asset in getResponse[0]:
        listAssets.append(convertAssetToMongoDBAsset(asset))
        
    
insertManyToMongoDB(listAssets)    

client.close()

