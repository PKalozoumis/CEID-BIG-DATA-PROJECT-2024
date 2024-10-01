from pymongo import MongoClient
from datetime import datetime, timezone

import re

client = MongoClient('mongodb://localhost:27017/')

db = client['big_data_project_2024']

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

if lower_str == "": #Default time, for quick demonstration. DO NOT USE
    lower_str = "2024-06-21 21:09:09"
    print("\033[1A", end='')
    print("\033[15G", end='')
    print("\b\b2024-06-21 21:09:09")

lower_time = datetime.strptime(lower_str, '%Y-%m-%d %H:%M:%S')

upper_str = input("Upper time: ")

if upper_str == "": #Default time, for quick demonstration. DO NOT USE
    upper_str = "2024-06-21 21:10:29"
    print("\033[1A", end='')
    print("\033[15G", end='')
    print("\b\b2024-06-21 21:10:29")

upper_time = datetime.strptime(upper_str, '%Y-%m-%d %H:%M:%S')

print()

#=======================================================================================================================================

if (opt == "1"):

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
                "_id": "$vcount",
                "data": {"$push": "$$ROOT"}
            }
        },
        {
            "$sort": {"_id": 1}
        },
        {
            "$limit": 1
        }
    ]

    #================================================================================

    cursor = db["processed"].aggregate(query)

    for doc in cursor:
        num_vehicles = doc["_id"]
        data = doc["data"]

        for d in data:
            print(f"Link {d['link']} with {num_vehicles} cars at {d['time']}")

#=======================================================================================================================================


elif (opt == "2"):

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
                "_id": "$vspeed",
                "data": {"$push": "$$ROOT"}
            }
        },
        {
            "$sort": {"_id": -1}
        },
        {
            "$limit": 1
        }
    ]

    #================================================================================

    cursor = db["processed"].aggregate(query)

    for doc in cursor:
        avg_speed = doc["_id"]
        data = doc["data"]

        for d in data:
            print(f"Link {d['link']} with average speed {avg_speed} km/h at {d['time']}")

#=======================================================================================================================================

elif (opt == "3"):
    query = [
        #Group by the vehicle
        #Goal is find the distance each vehicle has driven within the time window, along with the actual path it traversed
        #At the end, return the vehicle(s) and the paths with the greatest distance
        #==========================================================================================================
        {
            "$group":
            {
                "_id": "$name",
                "docs": {"$push": "$$ROOT"}
            }
        },
        {
            "$sort": {"time": 1}
        },

        #We have a SORTED array of all the entries, for each car
        #Change array elements, so that they only contain:
        #
        #  name: Name of the vehicle
        #  time: The timestamp
        #  link: The current link
        #  position: Position within the current link
        #  next: Information about the next document in the array (5 seconds from now)
        #      link: The next link
        #      position: Position within the next link
        #      dista_until_next_timestamp: The distance I'll drive between now and the next moment. THIS IS WHAT GETS SUMMED UP
        #==========================================================================================================
        {
            "$project":
            {
                "mappedArray":
                {
                    "$map":
                    {
                        "input": {"$range": [0, {"$size": "$docs"}]},
                        "as": "i",
                        "in":
                        {
                            #Current entry information
                            "name": {"$arrayElemAt": ["$docs.name", "$$i"]},
                            "time": {"$arrayElemAt": ["$docs.time", "$$i"]},
                            "link": {"$arrayElemAt": ["$docs.link", "$$i"]},
                            "position": {"$arrayElemAt": ["$docs.position", "$$i"]},

                            #Next entry information
                            "next":
                            {
                                "$cond":
                                {
                                    #The last document
                                    #The next entry will be about the same link (since I'm on my destination link)(Am I????)
                                    #The position will be the end of the link (500)
                                    "if": {"$eq": ["$$i", {"$subtract": [{"$size": "$docs"}, 1]}]},
                                    "then":
                                    {
                                        "position": 500,
                                        "link": {"$arrayElemAt": ["$docs.link", "$$i"]},
                                        "dista_until_next_timestamp": {"$subtract": [500, {"$arrayElemAt": ["$docs.position", "$$i"]}]}
                                    },
                                    "else":
                                    {
                                        "$cond":
                                        {
                                            "if": #I stay within the same link between timestamps
                                            {
                                                "$eq":
                                                [
                                                    {"$arrayElemAt": ["$docs.link", "$$i"] },
                                                    {"$arrayElemAt": ["$docs.link", {"$add": ["$$i", 1]} ] }
                                                ]
                                            },
                                            "then": #Just subtract the two positions
                                            {
                                                "position": {"$arrayElemAt": ["$docs.position", {"$add": ["$$i", 1]} ] },
                                                "link": {"$arrayElemAt": ["$docs.link", {"$add": ["$$i", 1]} ] },
                                                "dista_until_next_timestamp":
                                                {
                                                    "$subtract":
                                                    [
                                                        {"$arrayElemAt": ["$docs.position", {"$add": ["$$i", 1]} ] },
                                                        {"$arrayElemAt": ["$docs.position", "$$i"]}
                                                    ]
                                                }
                                            },
                                            "else": #Add distance from current position to the end of the current link, plus the remaining distance
                                            {
                                                "position": {"$arrayElemAt": ["$docs.position", {"$add": ["$$i", 1]} ] },
                                                "link": {"$arrayElemAt": ["$docs.link", {"$add": ["$$i", 1]} ] },
                                                "dista_until_next_timestamp":
                                                {
                                                    "$add":
                                                    [
                                                        {"$subtract": [500, {"$arrayElemAt": ["$docs.position", "$$i"]}]},
                                                        {"$arrayElemAt": ["$docs.position", {"$add": ["$$i", 1]}]}
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },

        #Limit time
        #Why am I using "<" instead of "<="?
        #Because from each array element, we will use the "distance until THE NEXT timestamp" for the sum
        #During this next timestamp, we will be in our final position in the specified time window
        #So we don't want the entry for that specific timestamp. We don't care about its next
        #==========================================================================================================
        {
            "$project":
            {
                "filteredArray":
                {   
                    "$filter":
                    {
                        "input": "$mappedArray",
                        "as": "elem",
                        "cond":
                        {
                            "$and":
                            [
                                {"$gte": ["$$elem.time", lower_time]},
                                {"$lt": ["$$elem.time", upper_time]}
                            ]
                        }
                    }
                }
            }
        },
        {
            "$unwind": "$filteredArray"
        },
        {
            "$replaceRoot": {"newRoot": "$filteredArray"}
        },

        #Calculate final dista for each vehicle
        #==========================================================================================================
        {
            "$group":
            {
                "_id": "$name",
                "docs": {"$push": "$$ROOT"},
                "dista_travelled": {"$sum": "$next.dista_until_next_timestamp"}
            }
        },

        #EVERYTHING AFTER THIS POINT IS ABOUT FORMATTING THE PATH

        #For each vehicle, find its last link within the time window
        #For each array element, mention its position using the constants "first", "last", "middle", "firstlast" (first & last)
        #==========================================================================================================
        {
            "$project":
            {
                "dista_travelled": 1,
                "last_link": {"$arrayElemAt": ["$docs.next.link", -1]},
                "temp":
                {
                    "$map":
                    {
                        "input": {"$range": [0, {"$size": "$docs"}]},
                        "as": "i",
                        "in":
                        {
                            "$mergeObjects": [{"$arrayElemAt": ["$docs", "$$i"]},
                            {
                                "$cond":
                                {
                                    "if": {"$eq": ["$$i", 0]}, #Element is first
                                    "then":
                                    {
                                        "$cond":
                                        {
                                            "if": {"$eq": ["$$i", {"$subtract": [{"$size": "$docs"}, 1]}]}, #Element is both first and last
                                            "then": {"index": "firstlast"},
                                            "else": {"index": "first"}
                                        }
                                    },
                                    "else": #Element is not first
                                    {
                                        "$cond":
                                        {
                                            "if": {"$eq": ["$$i", {"$subtract": [{"$size": "$docs"}, 1]}]}, #Element is last
                                            "then": {"index": "last"},
                                            "else":{"index": "middle"}
                                        }
                                    }
                                }
                            }]
                        }
                    }
                }
            }
        },

        #==========================================================================================================
        {
            "$project":
            {
                "dista_travelled": 1,
                "last_link": 1,
                "path":
                {
                    "$filter":
                    {
                        "input": "$temp",
                        "as": "elem",
                        "cond":
                        {
                            "$or":
                            [
                                {"$eq": ["$$elem.index", "first"]},
                                {"$eq": ["$$elem.index", "last"]},
                                {"$eq": ["$$elem.index", "firstlast"]},
                                {
                                    "$and":
                                    [
                                        {"$ne": ["$$elem.link", "$$elem.next.link"]},
                                        {"$ne": ["$$elem.index", "last"]}
                                    ]
                                    
                                }
                            ]
                        }
                    }
                }
            }
        },

        #==========================================================================================================
        {
            "$project":
            {
                "dista_travelled": 1,
                "last_link": 1,
                "path":
                {
                    "$map":
                    {
                        "input": "$path",
                        "as": "elem",
                        "in":
                        {
                            "$cond":
                            {
                                "if": {"$eq": ["$$elem.index", "first"]},
                                "then":
                                {
                                    "$cond":
                                    {
                                        "if": {"$eq": ["$$elem.link", "$$elem.next.link"]},
                                        "then":
                                        {
                                            "$concat": ["$$elem.link", "(", {"$toString": "$$elem.position"}, ")"]
                                        },
                                        "else":
                                        {
                                            "$concat": ["$$elem.link", "(", {"$toString": "$$elem.position"}, ")", " - ", "$$elem.next.link"]
                                        }
                                    }
                                    
                                },
                                "else":
                                {
                                    "$cond":
                                    {
                                        "if": {"$eq": ["$$elem.index", "firstlast"]},
                                        "then":
                                        {
                                            "$concat": ["$$elem.link", "(", {"$toString": "$$elem.position"}, ")", " - ", "$$elem.next.link", "(", {"$toString": "$$elem.next.position"}, ")"]
                                        },
                                        "else":
                                        {
                                            "$cond":
                                            {
                                                "if": {"$eq": ["$$elem.index", "last"]},
                                                "then":
                                                {
                                                    "$concat": ["$$elem.next.link", "(", {"$toString": "$$elem.next.position"}, ")"]
                                                },
                                                "else": "$$elem.next.link"
                                            }
                                        }
                                    }
                                }                       
                            }
                        }
                    }
                }
            }
        },

        #==========================================================================================================
        {
            "$project":
            {
                "dista_travelled": 1,
                "path":
                {
                    "$filter":
                    {
                        "input": "$path",
                        "as": "elem",
                        "cond":
                        {
                            "$ne": ["$$elem", "$last_link"]
                        }
                    }
                }
            }
        },

        #==========================================================================================================
        {
            "$project":
            {
                "dista_travelled": 1,
                "path":
                {
                    "$reduce":
                    {
                        "input": "$path",
                        "initialValue": "",
                        "in":
                        {
                            "$concat":
                            [
                                {
                                    "$cond":
                                    {
                                        "if": {"$ne": ["$$value", ""]},
                                        "then": {"$concat": ["$$value", " - "]},
                                        "else": ""
                                    }
                                },
                                "$$this"
                            ]
                        }
                    }
                }
            }
        },
        {
            "$group":
            {
                "_id": "$dista_travelled",
                "data": {"$push": "$$ROOT"}
            }
        },
        {
            "$sort":
            {
                "_id": -1
            }
        },
        {
            "$limit": 1
        }
    ]

    cursor = db["raw"].aggregate(query)

    for doc in cursor:
        dista = doc["_id"]
        vehicles = doc["data"]

        for vehicle in vehicles:
            print(f"Vehicle {vehicle['_id']} drove for {dista} meters on path \"{vehicle['path']}\"")

print()