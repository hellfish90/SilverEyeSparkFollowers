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
from sets import Set

from pyspark.sql.functions import col

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

result_df_4_5_6_7 = sqlContext.read.parquet("result.df.min")

result_df_4_5_6_7.show()
#print result_df_4_5_6_7.count()


result_df_political = sqlContext.read.parquet("result.df.f_political")

#result_df_political.show()

clusters_result = result_df_political.join(result_df_4_5_6_7, result_df_political["identifier"] == result_df_4_5_6_7["id"])
clusters_result = clusters_result.drop("id")
clusters_result = clusters_result.drop("namespace")

clusters_result.show()
#clusters_filtered = clusters_result.filter("label_max == 'psoe'")
#clusters_filtered.show()

#clusters_filtered = clusters_result.filter("label7 == 531442416")
#clusters_filtered.show()
#print clusters_filtered.count()

clusters_result.groupBy("label").count().sort(col("count").desc()).show()


def get_groups(row):
    return row[0]

groups_by_7 = clusters_result.groupBy("label").count().sort(col("label")).rdd.map(get_groups).collect()

users_by_groups_7 = {}
for group in groups_by_7:
    users_by_groups_7[group] = clusters_result.filter("label == "+str(group)).rdd.map(get_groups).collect()
    print users_by_groups_7[group]


print "_________"
print "Group 7 \nNum groups:" + str(len(users_by_groups_7.keys()))
print "_________"

friends_users_group_7 = {}
friends_users_group = Set()

print "Get Friends of group\n"

for group in users_by_groups_7:
    for user in users_by_groups_7[group]:
        for friend in twitter_users.get_friends_by_user_id(user):
            friends_users_group.add(friend)
        friends_users_group_7[group] = friends_users_group
        print user
    print "___________"
    friends_users_group = Set()
    print "Complete "+"("+str(len(friends_users_group_7[group]))+"):" + str(group)


print "Groups with friends\n"

collections_friends = collections_friends_dao.get_all_collections()
#print collections_friends

collections_friends_group_match_results = {}

groups_7_results = {}
i=0
for group in friends_users_group_7:
    i+=1
    for collection in collections_friends:
        collections_friends_group_match_results[collection]=0

    for friend in friends_users_group_7[group]:
        for collection in collections_friends:
            if friend in collections_friends[collection]:
                collections_friends_group_match_results[collection] += 1

    max_collection_match_group_result = ""
    last_collection_match_group_result = ""
    max_collection_match_group_result_value = 0
    for collection_match_group in collections_friends_group_match_results:
        if collections_friends_group_match_results[collection_match_group] > max_collection_match_group_result_value:
            max_collection_match_group_result_value = collections_friends_group_match_results[collection_match_group]
            last_collection_match_group_result = max_collection_match_group_result
            max_collection_match_group_result = collection_match_group
    groups_7_results[group] = (max_collection_match_group_result, last_collection_match_group_result)
    print "Group"+"("+str(i)+"/"+str(len(friends_users_group_7))+") -> "+str(group)+"\nMax Collection:" + groups_7_results[group][0] +"\nLast Max Collection:"+ groups_7_results[group][1]
    print "__________________\n"


def update_identifier_to_collection(row):
    new_row = groups_7_results[row[3]][0]
    return row[0], new_row

friends_users_group_7_mapped = clusters_result.rdd.map(update_identifier_to_collection).collect()

friends_users_group_7_mapped = sc.parallelize(friends_users_group_7_mapped).toDF(["identifier7", "label7P"])

clusters_result = clusters_result.join(friends_users_group_7_mapped, clusters_result["identifier"] == friends_users_group_7_mapped["identifier7"])
clusters_result = clusters_result.drop("identifier7")

clusters_result.show()

clusters_result.write.parquet("result.df.min.political")

'''
for collection in collections_friends:
    collections_friends_group_match_results[collection] = 0
    for group in friends_users_group_7:
        for friend in friends_users_group_7[group]:
            #print collections_friends[collection]
            if friend in collections_friends[collection]:
                collections_friends_group_match_results[collection] += 1


        max_collection_match_group_result = ""
        last_collection_match_group_result = ""
        max_collection_match_group_result_value = 0
        for collection_match_group in collections_friends_group_match_results:
            if collections_friends_group_match_results[collection_match_group] > max_collection_match_group_result_value:
                max_collection_match_group_result_value = collections_friends_group_match_results[collection_match_group]
                last_collection_match_group_result = max_collection_match_group_result
                max_collection_match_group_result = collection_match_group
        groups_7_results[group] = (max_collection_match_group_result, last_collection_match_group_result)
        print "Group "+str(group)+"\nMax Collection:" + groups_7_results[group][0] +"\nLast Max Collection:"+ groups_7_results[group][1]
        print "\n__________________"
'''