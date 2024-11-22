import pymongo
import logging


class MongoDBPipeline:
    def __init__(self, mongo_uri, mongo_database, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_database = mongo_database
        self.mongo_collection = mongo_collection
        self.client = None
        self.db = None
        self.collection = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        mongo_uri = crawler.settings.get("MONGO_URI")
        mongo_database = crawler.settings.get("MONGO_DATABASE")
        mongo_collection = crawler.settings.get("MONGO_COLLECTION")
        return cls(mongo_uri, mongo_database, mongo_collection)

    def open_spider(self, spider):
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_database]
            self.collection = self.db[self.mongo_collection]
            spider.log(f"MongoDB connection established to {self.mongo_uri}.")
        except Exception as e:
            spider.log(f"Error connecting to MongoDB: {str(e)}", level=logging.ERROR)

    def close_spider(self, spider):
        try:
            if self.client:
                self.client.close()
                spider.log("MongoDB connection closed.")
        except Exception as e:
            spider.log(
                f"Error closing MongoDB connection: {str(e)}", level=logging.ERROR
            )

    def process_item(self, item, spider):
        try:
            # Log before inserting
            spider.log(
                f"Attempting to insert item into MongoDB: {dict(item)}",
                level=logging.DEBUG,
            )

            # Insert the item into MongoDB
            self.collection.insert_one(dict(item))

            # Log successful insertion
            spider.log(
                f"Successfully inserted item into MongoDB: {dict(item)}",
                level=logging.INFO,
            )
        except Exception as e:
            spider.log(
                f"Error inserting item into MongoDB: {str(e)}", level=logging.ERROR
            )

        return item
