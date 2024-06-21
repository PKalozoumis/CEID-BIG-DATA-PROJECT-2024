from pymongo import MongoClient
from datetime import datetime, timezone
import pytz

import re

client = MongoClient('mongodb://localhost:27017/')

db = client['big_data_project_2024']
collection = db['processed']


opt = None

err = False

while (opt == None):

    print("\nWhat would you like to do:\n================================================================\n1. Find link with most cars within a time period\n2. Find link with greatest average speed within a time period\n3. Find longest route within a time period\n")

    opt = input(f"{'Your option' if not err else 'Invalid option, try again'}: ")

    #print("\nGreat!")

    if not re.match(r"1|2|3", opt):
        err=True
        opt = None
        print("\033[8A", end='')
        print("\033[J", end='')

print("\nMake sure to insert UTC time in the format YYYY-MM-DD HH:MM:SS\n")

#Query parameters
#================================================================================
lower_str = input("Lower date: ")
#lower_time = datetime.strptime(lower_str, '%Y-%m-%d %H:%M:%S')
lower_time = datetime.strptime("2024-06-21 17:01:29", '%Y-%m-%d %H:%M:%S')

upper_str = input("Upper time: ")
#upper_time = datetime.strptime(upper_str, '%Y-%m-%d %H:%M:%S')
upper_time = datetime.strptime("2024-06-21 17:01:44", '%Y-%m-%d %H:%M:%S')

print()

#=======================================================================================================================================

if (opt == "1"):

    '''
    query = [
        {
            "$match":
            {
                "time": {"$gte": lower_time, "$lte": upper_time}
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
    '''

    query = {
        "time": {"$gte": lower_time, "$lte": upper_time}
    }

    #================================================================================

    cursor = collection.find(query).sort({"vcount": 1}).limit(1)

    for doc in cursor:
        print(f"Link {doc['link']} with {doc['vcount']} cars at {doc['time']}")

#=======================================================================================================================================


elif (opt == 2):
    pass

elif (opt == 3):
    pass