from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["big_data_project_2024"]

for collection in db.list_collection_names():
    db[collection].delete_many({})