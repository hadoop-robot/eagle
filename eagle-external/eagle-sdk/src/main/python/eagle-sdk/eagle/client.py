class EagleClientConn:
    def __init__(self, endpoint="https://localhost:9090/", auth_key=""):
        pass

    def is_https(self):
        return True

    def get_port(self):
        return 9090

    def get_host(self):
        return "localhost"

    def close(self):
        pass


class EagleClientBase:
    def __init__(self, connection):
        pass

    def close(self):
        pass


class EagleEntityClient(EagleClientBase):
    pass


class EagleLogClient(EagleClientBase):
    pass


class EagleMetricClient(EagleClientBase):
    pass

class EagleAlertClient(EagleClientBase):
    pass

class EagleStreamClient:
    def __init__(self, url="http://localhost:9090/stream/HADOOP_JMX_STREAM_APOLLO", access_key={}):
        # Async load stream metadata
        pass

    def register_metric(self, metric, dimension_fields=[],value_fields=[]):
        pass

    def unregister_metric(self, metric):
        pass

    def send_metric(self, metric, value={}):
        pass

    def send_log(self, type, log):
        pass

    def send_alert(self, alert):
        pass

    def close(self):
        pass
