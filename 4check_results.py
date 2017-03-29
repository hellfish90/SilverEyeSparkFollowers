import os
import sys

# Path for spark source folder
from pymongo import MongoClient

from DAOCollectionsFriends import TwitterFriendsCollections
from DAOTwitterUsers import TwitterUsers
from DAOGraph_users_extractor import GraphUsersExtractor

os.environ['SPARK_HOME'] = "/Users/Marc/Library/spark-2.1.0"

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

sc = pyspark.SparkContext('local[*]')
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.caseSensitive", "true");

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

users_analysis = sqlContext.read.csv(header=True, path="user_analysis.csv")

users_analysis.show()

result_df_4_5_6_7 = sqlContext.read.parquet("result.df.min.political")
result_df_4_5_6_7.show()

clusters_result = users_analysis.join(result_df_4_5_6_7, users_analysis["ID"] == result_df_4_5_6_7["identifier"])
clusters_result = clusters_result.drop("ID")
clusters_result = clusters_result.drop("Comentary")
clusters_result = clusters_result.drop("MongoID")

clusters_result.show()


def check_prediction_assertion(row):
    possible_political_party = []
    negative_political_party = []
    positive_political_party = []

    success = 0
    error = 0
    partial_assert = 0
    partial_assert_error = 0
    empty_result = 0

    political_parties = [u"pp", u"psoe", u"ciudadanos", u"podemos", u"PDeCAT", u"Cup", u"erc", u"IZ"]

    i = 1

    for political_party in political_parties:
        if row[i] is None:
            possible_political_party.append(political_party)
        elif int(row[i]) == 1:
            positive_political_party.append(political_party)
        elif int(row[i]) == -1:
            negative_political_party.append(political_party)

        i += 1

    for party in positive_political_party:
        if row[13] == party:
            success = 1

    for party in negative_political_party:
        if row[13] == party:
            error = 1

    for party in possible_political_party:
        if row[13] == party:
            if len(positive_political_party) < 1:
                partial_assert = 1
            else:
                partial_assert_error = 1

    if success == 0 and error == 0 and partial_assert == 0 and partial_assert_error == 0:
        empty_result = 1

    return row[9], success, error, partial_assert, empty_result, partial_assert_error, str(
        positive_political_party), str(
            negative_political_party), str(possible_political_party), row[13]


success = 0
error = 0
partial_success = 0
partial_error_success = 0
empty_result = 0
count = 0

for element in clusters_result.rdd.map(check_prediction_assertion).collect():
    print element
    count += 1
    success += element[1]
    error += element[2]
    partial_success += element[3]
    empty_result += element[4]
    partial_error_success += element[5]

print "____________________________________"
print "Success -> " + str(success) + "/" + str(count)
print "Partial Success -> " + str(partial_success) + "/" + str(count)
print "Partial Error Success -> " + str(partial_error_success) + "/" + str(count)
print "Error->" + str(error) + "/" + str(count)
print "Empty Result ->" + str(empty_result) + "/" + str(count)
print "____________________________________"
print "Total Assert ->" + str(success + partial_success + partial_error_success) + "/" + str(count)
print "____________________________________"
