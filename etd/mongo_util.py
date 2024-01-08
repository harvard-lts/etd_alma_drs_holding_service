import pymongo
import os
import logging


FIELD_SUBMISSION_STATUS = "alma_submission_status"
FIELD_PQ_ID = "proquest_id"
FIELD_SCHOOL_ALMA_DROPBOX = "school_alma_dropbox"
FIELD_DIRECTORY_ID = "directory_id"
FIELD_IN_DASH = "indash"
DRS_HOLDING_DROPBOX_STATUS = "DRS_HOLDING_DROPBOX"
DRS_HOLDING_API_STATUS = "DRS_HOLDING_API"
ALMA_STATUS = "ALMA"


class MongoUtil():   # pragma: no cover, not used by unit tests

    logger = logging.getLogger('etd_alma_drs_holding')

    def __init__(self):
        self.client = pymongo.MongoClient(os.getenv("MONGO_URL"))
        self.db = self.client[os.getenv("MONGO_DB")]
        self.collection = self.db[os.getenv("MONGO_COLLECTION")]

    def insert_record(self, record):  # pragma: no cover, not currently used
        self.collection.insert_one(record)

    def insert_records(self, records):
        self.collection.insert_many(records)

    def update_status(self, query, status):
        statusupdate = {"$set": {FIELD_SUBMISSION_STATUS: status}}
        self.logger.debug("Updating status for {} to {}".
                          format(query, status))
        self.collection.update_one(query, statusupdate)

    def query_records(self, query={}, fields=None):
        if fields is not None:
            return list(self.collection.find(query, fields))
        else:  # pragma: no cover, not currently used by app
            return list(self.collection.find(query))

    def delete_records(self):
        self.collection.delete_many({})

    def close_connection(self):
        self.client.close()

    def __del__(self):
        self.close_connection()

    # we use this if we want to set to the test collection for testing
    def set_collection(self, collection):
        self.collection = collection
