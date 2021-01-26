from pymongo import MongoClient, InsertOne
import random
import string
import sys

mc = MongoClient("mongodb://root:root@localhost")

def random_person():
    return {
        "name": ''.join(random.sample(string.ascii_letters + string.digits, random.randint(3, 15))),
        "age": random.randint(1, 100)
    }


def make_collection(db, col, n):
    docs = [InsertOne(random_person()) for i in range(n)]
    mc[db][col].bulk_write(docs)


if __name__ == '__main__':
   make_collection(sys.argv[1], sys.argv[2], int(sys.argv[3]))