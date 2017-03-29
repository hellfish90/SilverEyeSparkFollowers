from pymongo import MongoClient

from DAOTwitterUsers import TwitterUsers
import tweepy
import json
import os
from sets import Set
import time


class GraphUsersExtractor:

    def __init__(self, twitterUsers):
        self.twitter_users = twitterUsers



    def get_user_by_ids(self, ids):
        f = open(os.path.dirname(os.path.dirname(__file__)) +'/SparkFollowers/config.json', 'r')
        config = json.load(f)
        access_token = config['access_token']
        access_token_secret = config['access_token_secret']
        consumer_key = config['consumer_key']
        consumer_secret = config['consumer_secret']
        f.close()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

        print ids
        users = []

        while(len(ids)>0):
            try:
                users = api.lookup_users(user_ids=ids)
                break
            except Exception as e:
                print "Sleep :O", e
                time.sleep(15*60)

            return users

    def get_vertex(self):
        vertex = Set()
        unique_vertex = []

        for user in self.twitter_users.get_all_users():
            vertex.add(long(user["_id"]))
            for friend in user["friends"]:
                vertex.add(long(friend["id"]))

        for v in vertex:
            unique_vertex.append((v,v))

        return unique_vertex

    def get_edges(self):
        edges = []
        for user in self.twitter_users.get_all_users():
            for friend in user["friends"]:
                edges.append((str(user["_id"]),str(friend["id"]),"friend"))
        return edges



if __name__ == "__main__":

    client = MongoClient("127.0.0.1", 27017, connect=True)
    twitter_users = TwitterUsers(client, "SilverEye")
    graph_users_extractor = GraphUsersExtractor(twitter_users)

    print "Vertex: " + str(len(graph_users_extractor.get_vertex()))
    print "Edges: " + str(len(graph_users_extractor.get_edges()))