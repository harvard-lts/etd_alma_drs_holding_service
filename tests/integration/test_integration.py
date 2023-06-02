from etd.worker import Worker


class TestWorkerIntegrationClass():

    def test_api(self):
        expected_msg = "REST api is running."
        worker = Worker()
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self):
        expected_msg = "REST api is NOT running."
        worker = Worker()
        msg = worker.call_api()
        assert msg != expected_msg
