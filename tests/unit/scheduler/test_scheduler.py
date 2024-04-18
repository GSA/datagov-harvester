import requests


class TestScheduler:
    def test_if_scheduler_on(self):
        res = requests.get("http://localhost:8080/scheduler").json()
        assert res["running"] is True
