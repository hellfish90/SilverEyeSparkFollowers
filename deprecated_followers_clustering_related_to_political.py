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

users_related_array =twitter_users.get_users_and_friends_array()

users_related = sc.parallelize(users_related_array)

users_related_rdd = users_related.map(lambda row: {"id": row[0], "friends": row[1]})

users_related_rdd.cache()

#print users_related_rdd.collect()

result_df_7 = sqlContext.read.parquet("result.df.7")
result_df_7 = result_df_7.selectExpr("id as id", "label as label7")

result_df_6 = sqlContext.read.parquet("result.df.6")
result_df_6 = result_df_6.selectExpr("id as id6", "label as label6")
result_df_6 = result_df_6.drop("namespace")

merged_7_6 = result_df_7.join(result_df_6, result_df_7["id"] == result_df_6["id6"])
merged_7_6 = merged_7_6.drop("id6")

result_df_5 = sqlContext.read.parquet("result.df.5")
result_df_5 = result_df_5.selectExpr("id as id5", "label as label5")
result_df_5 = result_df_5.drop("namespace")

merged_7_6_5 = merged_7_6.join(result_df_5, merged_7_6["id"] == result_df_5["id5"])
merged_7_6_5 = merged_7_6_5.drop("id5")

result_df_4 = sqlContext.read.parquet("result.df.4")
result_df_4 = result_df_4.selectExpr("id as id4", "label as label4")
result_df_4 = result_df_4.drop("namespace")

merged_7_6_5_4 = merged_7_6_5.join(result_df_4, merged_7_6_5["id"] == result_df_4["id4"])
merged_7_6_5_4 = merged_7_6_5_4.drop("id4")

merged_7_6_5_4.show()

merged_7_6_5_4.write.parquet("result.df.4.5.6.7")
