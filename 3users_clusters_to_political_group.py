import os
import sys

# Path for spark source folder
from pymongo import MongoClient

from DAOCollectionsFriends import TwitterFriendsCollections
from DAOTwitterUsers import TwitterUsers
from DAOGraph_users_extractor import GraphUsersExtractor

os.environ['SPARK_HOME']="/Users/Marc/Library/spark-2.1.0"

# Append pyspark  to Python Path
sys.path.append("/Users/Marc/Library/spark-2.1.0/python/")

import pyspark
import os
import math
import random
import sys
import shutil
import toyplot

# make sure pyspark tells workers to use python2 not 3 if both are installed\\n\",\n",
os.environ["PYSPARK_PYTHON"] = "python2"
os.environ['PYTHONPATH'] = ':'.join(sys.path)

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vectors

#%matplotlib inline
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

sc = pyspark.SparkContext('local[*]')
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.caseSensitive", "true");

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

client = MongoClient("127.0.0.1", 27017, connect=True)
db = client["SilverEye"]
twitter_users = TwitterUsers(client, "SilverEye")
collections_friends_dao = TwitterFriendsCollections(client, "SilverEye")

users_related_array = twitter_users.get_users_and_friends_array()

collections_friends = collections_friends_dao.get_all_collections()


def get_political_orientation_by_followers(user):

    collections_count = {}
    for collection in collections_friends:
        collections_count[collection] = 0

    for collection in collections_friends:
        for friend in user[1]:
            if friend in collections_friends[collection]:
                collections_count[collection] += 1

    max_collection = ""
    max_collection_value = 0
    last_max_collection = ""

    print(collections_count)

    for collection in collections_count:
        if collections_count[collection] > max_collection_value:
            max_collection_value = collections_count[collection]
            last_max_collection = max_collection
            max_collection = collection

    if len(last_max_collection)<1:
        last_max_collection = max_collection

    id = long(user[0])

    return (id,max_collection,last_max_collection)

result = []

for user in users_related_array:
    result.append(get_political_orientation_by_followers(user))

for partial_result in result:
    print partial_result

result_df = sc.parallelize(result).toDF(["identifier", "label_last", "label_max"])
result_df.show()

result_df.write.parquet("result.df.f_political")