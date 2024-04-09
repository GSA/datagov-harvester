class TestCFTasking:
    def test_add_task(self, cf_handler, dhl_cf_task_data):

        assert cf_handler.start_task(**dhl_cf_task_data) is not None

    def test_get_task(self, cf_handler, dhl_cf_task_data):

        task = cf_handler.get_task(
            dhl_cf_task_data["app_guuid"], dhl_cf_task_data["task_id"]
        )
        assert task is not None

    def test_get_all_app_tasks(self, cf_handler, dhl_cf_task_data):
        tasks = cf_handler.get_all_app_tasks(dhl_cf_task_data["app_guuid"])
        assert tasks is not None

    def test_cancel_task(self, cf_handler, dhl_cf_task_data):

        task = cf_handler.stop_task(dhl_cf_task_data["task_id"])
        assert task is not None

    def test_read_recent_task_logs(self, cf_handler, dhl_cf_task_data):

        logs = cf_handler.read_recent_app_logs(
            dhl_cf_task_data["app_guuid"], dhl_cf_task_data["task_id"]
        )

        assert logs is not None
