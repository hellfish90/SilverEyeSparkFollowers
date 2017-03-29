# -*- coding: utf-8 -*-
import os
import sys

# Path for spark source folder
from pymongo import MongoClient

from DAOCollectionsFriends import TwitterFriendsCollections
from DAOTwitterUsers import TwitterUsers

client = MongoClient("127.0.0.1", 27017, connect=True)
db = client["SilverEye"]
twitter_users = TwitterUsers(client, "SilverEye")
twitter_friends_collections = TwitterFriendsCollections(client, "SilverEye")

match = 0

for user in twitter_users.get_all_users():
    for collection in twitter_friends_collections.get_all_collections():
        for friends in collection["fiends"]:
            if friends == user["friends"]:
                match += 1
        twitter_users.add_collection_result(user["_id"], collection["_id"], match)
