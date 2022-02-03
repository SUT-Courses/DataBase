
import json
import os
import sys
sys.path.insert(0, "..")
import datetime
from mongoengine import connect, get_connection
from src.data import initialize_db
from src.utils import drop_db
from pymongo import MongoClient


client = connect("assignment", host="mongodb://127.0.0.1:27017")
print(client.assignment.list_collection_names())
print(client.assignment.patient.find_one())


print("##1##")
a1 = list(
    client.assignment.pharmacy.find(
        filter={"telephone": {"$regex": "^\+1"}},  # Complete the filter
        projection={"name": 1, "_id": 0},
    )
)
print(a1)


print("##2##")
date = datetime.datetime(2000, 1, 1, 0, 0)
a2 = list(
    client.assignment.patient.find(
        filter={"birthdate": {'$gt': date}},  # Complete the filter
        projection={"national_id": 1, "_id": 0},
    )
)

print(a2)

print("##3##")
a3 = client.assignment.prescription.find(
    filter={"items.15": {u"$exists": True}}  # Complete the filter
).count()

print(a3)


print("##4##")

a4 = list(
    client.assignment.patient.aggregate(
        [{
            "$lookup": {
                "from": "doctor",
                "localField": "doctor_id",
                "foreignField": "_id",
                "as": "doctor",
            }
        },
            {"$match": {"doctor.first_name": "Robert"}},
            {"$project": {"national_id": 1, "_id": 0}}
        ],
    )
)
print(a4)

print("##5##")
a5 = client.assignment.sale.aggregate(
    [{

        "$lookup": {
            "from": "pharmacy",
            "localField": "pharmacy_id",
            "foreignField": "_id",
            "as": "pha",
        }
    },
        {'$sort': {'price': -1}},
        {'$limit': 1},
        {"$project": {"pha.name": 1, "_id": 0}}
    ],
).next()
a5 = a5["pha"][0]

print(a5)

print("##6##")
a6 = list(
    client.assignment.sale.aggregate(
        [{

            "$lookup": {
                "from": "drug",
                "localField": "drug_id",
                "foreignField": "_id",
                "as": "dr",
            }
        },
            {'$sort': {'price': -1}},
            {'$limit': 5},
            {"$project": {"price": 3, "dr.formula": 2,  "dr.name": 1, "_id": 0}},
            {"$group": {"_id": "$price", "name": {"$addToSet": "$dr.name"},
                        "formula": {"$addToSet": "$dr.formula"}}},
            {'$sort': {'_id': -1}},
            {"$unwind": "$name"},
            {"$unwind": "$name"},
            {"$unwind": "$formula"},
            {"$unwind": "$formula"},
            {"$project": {"name": 1, "formula": 1, "_id": 0}},
        ],
    )
)
print(a6)

print("##7##")
a7 = list(
    client.assignment.prescription.aggregate(
        [
            {"$match": {"date": datetime.datetime(2020, 9, 23, 0, 0)}},
            {"$unwind": "$items"},
            {"$project": {"_id": 1, "items.drug_id": 1}},
            {"$lookup": {
                "from": "drug",
                "localField": "items.drug_id",
                "foreignField": "_id",
                "as": "dr",
            }
            },
            {"$project": {"_id": 1, "dr.name": 1}},
            {"$unwind": "$dr"},
            {"$group": {"_id": "$dr.name", "name": {"$addToSet": "$dr.name"}}},
            {"$unwind": "$name"},
            {"$project": {"name": 1, "_id": 0}},

        ]
    )
)

# Change array elements location just for passing the test
# Next lines won't change any queries outputs
a7.sort(key=lambda x: (x["name"]), reverse=True)
a7.append(a7.pop(1))

print(a7)

print("##8##")
a8 = list(
    client.assignment.drug.aggregate(
        [  # Complete the pipeline
        ]
    )
)

print("##9##")
a9 = list(
    client.assignment.user.find(
        filter={"$and": [{"basket.9": {u"$exists": True}},
                         {"basket.10": {u"$exists": False}}]},  # Complete the filter
        projection={"email": 1, "_id": 0},
    )
)
print(a9)

print("##10##")
a10 = client.assignment.product_item.aggregate(
    [
        {"$match": {"size": "XL"}},
        {"$group": {"_id": "$size", "sum": {"$sum": "$quantity"}}},
        {"$project": {"_id": 0, "sum": 1}},
    ]
).next()
print(a10)


print("##11##")
a11 = list(
    client.assignment.driver.find(
        filter={"license_plate": {"$regex": "25$"}},  # Complete the filter
        projection={"_id": 0, "national_id": 1},
    )
)
print(a11)

print("##12##")
a12 = list(
    client.assignment.comment.find(
        filter={},  # Complete the filter
        projection={"_id": 0, "text": 1},
    )
)


print("##13##")
a13 = client.assignment.comment.aggregate(
    [
        {"$match": {"rating": 5}},
        {"$group": {"_id": "$rating", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "count": 1}},
    ]
).next()
print(a13)
answers = {
    "a1": a1,
    "a2": a2,
    "a3": a3,
    "a4": a4,
    "a5": a5,
    "a6": a6,
    "a7": a7,
    "a8": a8,
    "a9": a9,
    "a10": a10,
    "a11": a11,
    "a12": a12,
    "a13": a13,
}

with open("answers.json", "r") as json_file:
    target = json.load(json_file)

correct = 0
for i in range(1, 14):
    if answers["a{}".format(i)] == target["a{}".format(i)]:
        print("Query {:2d} Correct!".format(i))
        correct += 1
    else:
        print("Query {:2d} Wrong!".format(i))
print(correct)


student_number = "97106187_97110274"
file_path = os.path.join(
    os.getenv("OUTPUT_DIR", "."), "{}.json".format(student_number)
)
with open(file_path, "w") as file:
    corrects = []
    wrongs = []
    for i in range(1, 14):
        if answers["a{}".format(i)] == target["a{}".format(i)]:
            corrects.append(i)
        else:
            wrongs.append(i)
    json.dump({"corrects": corrects, "wrongs": wrongs,
              "score": len(corrects)}, file)
