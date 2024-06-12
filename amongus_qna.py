from pymongo import MongoClient
from datetime import datetime, timezone

client = MongoClient('mongodb://localhost:27017/')

db = client['big_data_project_2024']
collection = db['processed']


#Query parameters
#===============================================================================================================

lower_str = "2024-06-12T14:57:00"
lower_time = datetime.fromisoformat(lower_str)
lower_utc = lower_time.astimezone(timezone.utc)

upper_str = "2024-06-12T14:57:30"
upper_time = datetime.fromisoformat(upper_str)
upper_utc = upper_time.astimezone(timezone.utc)


#===============================================================================================================

query = [
    {
        "$match":
        {
            "time": {"$gte": lower_utc, "$lte": upper_utc}
        }
    },
    {
        "$group":
        {
            "_id": "$link",
            "cars": {"$sum": "$vcount"},
        }
    },
    {
        "$sort": {"cars": 1}
    },
    {
        "$limit": 1
    }
]

cursor = collection.aggregate(query)

for doc in cursor:
    print(doc)