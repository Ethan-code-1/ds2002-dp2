from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

db = client.ebo4dq
collection = db.project2


#Travesing directory contents
path = "./data"

for (root, dirs, files) in os.walk(path):
    for file_name in files:
        with open("./data/" + file_name) as file:

            #First check if file contains valid json
            try:
                file_data = json.load(file)
            except:
                print("JSON format is invalid for file", file_name)
                continue           

            if isinstance(file_data, list):
                collection.insert_many(file_data)  
            else:
                collection.insert_one(file_data)

        print("Completed json file: ", file_name)

       
