import tasks.tasks as tasks

FEATURE_FLAGS = "feature_flags"


class TestTasksClass():

    def test_add_holdings(self):
        message = {"unit_test": "true", FEATURE_FLAGS: {
                'dash_feature_flag': "off",
                'alma_feature_flag': "off",
                'send_to_drs_feature_flag': "off",
                'drs_holding_record_feature_flag': "off"},
                "identifier": "30522803"}
        retval = tasks.add_holdings(message)
        assert "hello" in retval
        assert "feature_flags" in retval
        assert "identifier" in retval
        assert retval["identifier"] == "30522803"

    def test_add_holdings_no_feature_flags(self):
        message = {"unit_test": "true", "identifier": "30522803"}
        retval = tasks.add_holdings(message)
        assert "hello" in retval
        assert "identifier" in retval
        assert retval["identifier"] == "30522803"
