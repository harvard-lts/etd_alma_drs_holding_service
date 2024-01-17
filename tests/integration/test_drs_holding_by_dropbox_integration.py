from etd.drs_holding_by_dropbox import DRSHoldingByDropbox
from etd.mongo_util import MongoUtil
from celery import Celery
import tasks.tasks as tasks
import os
import os.path
import datetime
import shutil


class TestDRSHoldingByDropbox():

    single_record = [
        {
            "proquest_id": "12345678",
            "school_alma_dropbox": "gsd",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat(),
            "directory_id": "proquest1234-5678-gsd",
            "indash": True
        }
    ]

    multiple_records = [
        {
            "proquest_id": "12345678",
            "school_alma_dropbox": "gsd",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat(),
            "directory_id": "proquest1234-5678-gsd",
            "indash": True
        },
        {
            "proquest_id": "2345678",
            "school_alma_dropbox": "dce",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat(),
            "directory_id": "proquest1234-5555-dce",
            "indash": True
        },
        {
            "proquest_id": "3456789",
            "school_alma_dropbox": "gsas",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat(),
            "directory_id": "proquest1234-8765-gsas",
            "indash": True
        },
        {
            "proquest_id": "98989898",
            "school_alma_dropbox": "gsas",
            "alma_submission_status": "ALMA",
            "insertion_date": datetime.datetime.now().isoformat(),
            "last_modified_date": datetime.datetime.now().isoformat(),
            "alma_dropbox_submission_date":
            datetime.datetime.now().isoformat(),
            "directory_id": "proquest9876-1234-gsas",
            "indash": True
        }
    ]

    def test_send_to_alma_worker_single(self):
        """"
        Test case for send_to_alma_worker method of DRSHoldingByDropbox class.
        """
        self.__setup_single_test_collection()

        pqid = self.single_record[0]["proquest_id"]
        object_urn = "URN-3:HUL.DRS.OBJECT:"+pqid
        drs_holding = DRSHoldingByDropbox(pqid, object_urn,
                                          os.getenv("MONGO_TEST_COLLECTION"))
        holding_sent = drs_holding.send_to_alma_worker(integration_test=True)
        assert holding_sent

        self.__teardown_single_test_collection()
    
    def test_send_to_alma_worker_multiple(self):
        """"
        Test case for send_to_alma_worker method of DRSHoldingByDropbox class.
        """
        self.__setup_multiple_test_collection()

        for record in self.multiple_records:
            pqid = record["proquest_id"]
            object_urn = "URN-3:HUL.DRS.OBJECT:"+pqid
            drs_holding = DRSHoldingByDropbox(pqid, object_urn,
                                              os.getenv(
                                                  "MONGO_TEST_COLLECTION"))
            holding_sent = drs_holding.send_to_alma_worker(
                integration_test=True)
            assert holding_sent

        self.__teardown_multiple_test_collection()
    
    def test_task(self):
        """"
        Test case for send_to_alma_worker method of DRSHoldingByDropbox class.
        """
        self.__setup_multiple_test_collection()

        for record in self.multiple_records:
            pqid = record["proquest_id"]
            object_urn = "URN-3:HUL.DRS.OBJECT:"+pqid
            msg = {"pqid": pqid,
                   "object_urn": object_urn,
                   "integration_test": True}
            tasks.create_drs_holding_record_in_alma(msg)
        self.__teardown_multiple_test_collection()
    
    def test_multiple_task_messages(self):
        """"
        Test case for send_to_alma_worker method of DRSHoldingByDropbox class.
        """
        self.__setup_multiple_test_collection()

        app1 = Celery('tasks')
        app1.config_from_object('celeryconfig')
        for record in self.multiple_records:
            pqid = record["proquest_id"]
            object_urn = "URN-3:HUL.DRS.OBJECT:"+pqid
            msg = {"pqid": pqid,
                   "object_urn": object_urn,
                   "integration_test": True}
            app1.send_task('etd-alma-drs-holding-service.tasks.add_holdings',
                           args=[msg], kwargs={},
                           queue=os.getenv("CONSUME_QUEUE_NAME"))
        assert False
        # self.__teardown_multiple_test_collection()
    
    def __setup_single_test_collection(self):
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv(
            "MONGO_TEST_COLLECTION")])
        mongo_util.insert_records(self.single_record)
        mongo_util.close_connection()

        mets_source = "tests/data/in/proquest2023071720-993578-gsd/mets.xml"
        dest_in = os.path.join("/home/etdadm/local/data/in",
                               self.single_record[0]["directory_id"])
        out_dir = os.path.join("/home/etdadm/local/data/out",
                               self.single_record[0]["directory_id"])
        os.makedirs(dest_in, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        shutil.copy(mets_source, dest_in)
        
    def __teardown_single_test_collection(self):
        dest_in = os.path.join("/home/etdadm/local/data/in",
                               self.single_record[0]["directory_id"])
        out_dir = os.path.join("/home/etdadm/local/data/out",
                               self.single_record[0]["directory_id"])
        shutil.rmtree(dest_in)
        shutil.rmtree(out_dir)
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv(
            "MONGO_TEST_COLLECTION")])
        mongo_util.delete_records({"proquest_id":
                                   self.single_record[0]["proquest_id"]})
        mongo_util.close_connection()

    def __setup_multiple_test_collection(self):
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv(
            "MONGO_TEST_COLLECTION")])
        mongo_util.insert_records(self.multiple_records)
        mongo_util.close_connection()

        mets_source = "tests/data/in/proquest2023071720-993578-gsd/mets.xml"
        for record in self.multiple_records:
            dest_in = os.path.join("/home/etdadm/local/data/in",
                                   record["directory_id"])
            out_dir = os.path.join("/home/etdadm/local/data/out",
                                   record["directory_id"])
            os.makedirs(dest_in, exist_ok=True)
            os.makedirs(out_dir, exist_ok=True)
            shutil.copy(mets_source, dest_in)
        
    def __teardown_multiple_test_collection(self):
        
        mongo_util = MongoUtil()
        mongo_util.set_collection(mongo_util.db[os.getenv(
            "MONGO_TEST_COLLECTION")])
        for record in self.multiple_records:
            mongo_util.delete_records({"proquest_id": record["proquest_id"]})
            dest_in = os.path.join("/home/etdadm/local/data/in",
                                   record["directory_id"])
            out_dir = os.path.join("/home/etdadm/local/data/out",
                                   record["directory_id"])
            shutil.rmtree(dest_in)
            shutil.rmtree(out_dir)
        mongo_util.close_connection()
        
