from datetime import datetime


class Job():
    def __init__(self):
        self.request = None  # NexusRequestObject
        self.result_future = None  # tornado.gen.Future
        self.time_created = datetime.now()
        self.time_done = None




