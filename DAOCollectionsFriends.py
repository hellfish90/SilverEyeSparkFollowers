from pymongo import MongoClient


class TwitterFriendsCollections:

    def __init__(self, mongo_client, database):
        self.mongo_client = mongo_client
        self.twitter_collection_friends = self.mongo_client[database]['CollectionFollowers']

    def get_collection_friend_by_id(self, id):
        return self.twitter_collection_friends.find_one({"_id": {"$eq": id}})

    def get_size(self):
        return self.twitter_collection_friends.count()

    def get_all_collections(self):
        collections = {}
        for collection in self.twitter_collection_friends.find():
            collections[collection["_id"]]=collection["fiends"]
        return collections

if __name__ == "__main__":

    client = MongoClient("127.0.0.1", 27017, connect=True)
    twitter_users = TwitterFriendsCollections(client, "SilverEye")


    print twitter_users.get_all_collections().keys()

    #for collection_friends in twitter_users.get_all_users():
    #    print collection_friends