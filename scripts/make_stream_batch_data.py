from pymongo import MongoClient, ReplaceOne
import random
import string
import sys
import time
import bson

mc = MongoClient("mongodb://root:root@localhost")

def random_person():
    return {
        "name": ''.join(random.sample(string.ascii_letters + string.digits, random.randint(3, 15))),
        "age": random.randint(1, 100),
        "gender": random.choice("FM"),

    }


def make_collection(db, col, n):
    batch = []
    for i in range(n):
        batch.append(ReplaceOne({"_id": bson.ObjectId()}, random_person(), upsert=True))
        if len(batch) > 1024:
            mc[db][col].bulk_write(batch)
            batch.clear()

    if len(batch) > 0:
        mc[db][col].insert_many(batch)

if __name__ == '__main__':
   make_collection(sys.argv[1], sys.argv[2], int(sys.argv[3]))