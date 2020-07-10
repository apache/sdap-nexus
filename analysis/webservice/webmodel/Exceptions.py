from webservice.webmodel.StandardNexusErrors import StandardNexusErrors


class NexusProcessingException(Exception):
    def __init__(self, error=StandardNexusErrors.UNKNOWN, reason="", code=500):
        self.error = error
        self.reason = reason
        self.code = code
        Exception.__init__(self, reason)


class NoDataException(NexusProcessingException):
    def __init__(self, reason="No data found for the selected timeframe"):
        NexusProcessingException.__init__(self, StandardNexusErrors.NO_DATA, reason, 400)


class DatasetNotFoundException(NexusProcessingException):
    def __init__(self, reason="Dataset not found"):
        NexusProcessingException.__init__(self, StandardNexusErrors.DATASET_MISSING, reason, code=404)