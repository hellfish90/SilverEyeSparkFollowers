# -*- coding: utf-8 -*-
import os
import sys

# Path for spark source folder
from pymongo import MongoClient

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

# make sure pyspark tells workers to use python2 not 3 if both are installed\\n\",\n",
os.environ["PYSPARK_PYTHON"] = "python2"
os.environ['PYTHONPATH'] = ':'.join(sys.path)

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vectors

# %matplotlib inline
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

spark_home = os.environ.get('SPARK_HOME', None)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 pyspark-shell"
)

sc = pyspark.SparkContext('local[*]')
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.caseSensitive", "true");

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

print spark_home, sc
from graphframes import GraphFrame
from pyspark.sql.functions import col

client = MongoClient("127.0.0.1", 27017, connect=True)
db = client["SilverEye"]
twitter_users = TwitterUsers(client, "SilverEye")
graph_users_extractor = GraphUsersExtractor(twitter_users)

edges = graph_users_extractor.get_edges()
vertex = graph_users_extractor.get_vertex()

v = sqlContext.createDataFrame(vertex, ["id", "namespace"])
# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame(edges, ["src", "dst", "relationship"])
# Create a GraphFrame

g = GraphFrame(v, e)
g.vertices.show()
g.edges.show()

result_df_political = sqlContext.read.parquet("result.df.f_political")


# result_df_political.show()

def get_groups(row):
    return row[0]


min_length = 9999999
iteration_min_length = 100
yeah_good_length = False
for i in range(7, 15):
    result_df = g.labelPropagation(maxIter=i)

    clusters_result = result_df_political.join(result_df, result_df_political["identifier"] == result_df["id"])
    clusters_result = clusters_result.drop("id")

    clusters_result_groups = clusters_result.groupBy("label").count().sort(col("label")).rdd.map(get_groups).collect()
    clusters_result.groupBy("label").count().sort(col("label")).show()
    users_by_groups_7 = {}
    groups_length = 0
    for group in clusters_result_groups:
        groups_length += 1
    print "Length:\n" + str(groups_length)
    print "Iteration:" + str(i)
    result_df.show()

    if min_length > groups_length:
        min_length = groups_length
        iteration_min_length = i

    if groups_length < 21:
        yeah_good_length = True

    if yeah_good_length:
        break

result_df = g.labelPropagation(maxIter=iteration_min_length)

clusters_result = result_df_political.join(result_df, result_df_political["identifier"] == result_df["id"])
clusters_result = clusters_result.drop("id")

clusters_result_groups = clusters_result.groupBy("label").count().sort(col("label")).rdd.map(get_groups).collect()
clusters_result.groupBy("label").count().sort(col("label")).show()
users_by_groups_7 = {}
groups_length = 0
for group in clusters_result_groups:
    groups_length += 1
print "Length:\n" + str(groups_length)
print "Iteration:" + str(iteration_min_length)
result_df.show()
result_df.write.parquet("result.df.min")

print "Last length:\n"
print str(groups_length)

# result_df_5.groupBy("label").count().sort(col("count").desc()).show()
# print "label 37888712|"
# result_df_5.filter(result_df_5["label"] == 37888712).show()
# print "label 1444520108"
# result_df_5.filter(result_df_5["label"] == 1444520108).show()
# print "label 143"
# result_df_5.filter(result_df_5["label"] == 531442416).show()


# edge_array = []
# for edge in edges:
#    edge_array.append([edge[0],edge[1]])

# edges = np.array(edge_array)
# y = toyplot.graph(edges, width=300);
# canvas = toyplot.Canvas(width=300)
# axes = canvas.cartesian()
# axes.plot(y);


# Query: Get in-degree of each vertex.
# g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
# g.edges.filter("relationship = 'friend'").count()

# Run PageRank algorithm, and show results.
# results = g.pageRank(resetProbability=0.01, maxIter=20)
# results.vertices.select("id", "pagerank").show()
"""
# Run LPA algorithm, and show results
#result = g.labelPropagation(maxIter=5)
result.select("id", "label").show()

resultComponent = db.ResultComponent

def save_result(row):

    result = {"_id":str(row["id"]),
              "name":str(row["name"]),
              "followersCount":str(row["followersCount"]),
              "component":str(row["component"])}

    resultComponent.insert_into(result)

result = g.stronglyConnectedComponents(maxIter=15)
result.show()
result.groupBy("component").count().show()



result = g.labelPropagation(maxIter=5)
result.show()
result.groupBy("label").count().sort(col("count").desc()).show()
print "label 37888712|"
result.filter(result["label"] == 37888712).show()
print "label 1444520108"
result.filter(result["label"] == 1444520108).show()
print "label 143"
result.filter(result["label"] == 143).show()

for row in result:
    result_row = {"_id":row.id.value,
              "name":row.name.value,
              "followersCount":row.followersCount.value,
              "component":row.component.value}
    print result_row
    resultComponent.insert_one(result_row)

print type(result)

#for index,row in result.iterrows():
#    print (row.id,row.name,row.followersCount,row.component)


#http://toyplot.readthedocs.io/en/stable/graph-visualization.html
"""
