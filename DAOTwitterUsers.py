from pymongo import MongoClient


class TwitterUsers:

    def __init__(self, mongo_client, database):
        self.mongo_client = mongo_client
        self.twitter_users_collection = self.mongo_client[database]['User']

    def save_new_user(self, id, user):
        self.twitter_users_collection.update({"_id": id}, {"user": user}, upsert=True)

    def get_user_by_id(self, id):
        return self.twitter_users_collection.find_one({"_id": {"$eq": id}})

    def add_collection_result(self,id,collection,result):
        self.twitter_users_collection.update({'_id':id},{ "$set":{"result_relation":{collection: result}}}, upsert=True)

    def get_size(self):
        return self.twitter_users_collection.count()

    def get_all_users(self):
        return self.twitter_users_collection.find()

    def get_users_and_friends_array(self):

        users_friends = []

        for user in self.get_all_users():
            friends = []
            for friend in user["friends"]:
                friends.append(friend["id"])

            users_friends.append((user["_id"],friends))
        return users_friends

    def get_friends_by_user_id(self, id):
        user_friends = []
        user = self.get_user_by_id(id)
        for friend in user["friends"]:
            user_friends.append(friend["id"])
        return user_friends



if __name__ == "__main__":

    client = MongoClient("127.0.0.1", 27017, connect=True)
    twitter_users = TwitterUsers(client, "SilverEye")

    user1 = twitter_users.get_friends_by_user_id(1499289019)
    user2 = twitter_users.get_friends_by_user_id(3373741366)

    print "user1:"
    print str(len(user1))

    print "user2:"
    print str(len(user2))

    print "Users: " + str(twitter_users.get_all_users().count())