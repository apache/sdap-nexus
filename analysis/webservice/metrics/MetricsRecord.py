from collections import OrderedDict
import logging

metrics_logger = logging.getLogger(__name__)


class MetricsRecord(object):
    def __init__(self, fields):
        self._fields = OrderedDict()
        for field in fields:
            self._fields[field.key] = field

    def record_metrics(self, **kwargs):
        for field_key, addend in kwargs.items():
            if field_key in self._fields:
                self._fields[field_key].add(addend)

    def print_metrics(self, logger=None, include_zero_values=False):
        if not logger:
            logger = metrics_logger

        logging_lines = []
        for field in self._fields.values():
            value = field.value()
            if value > 0 or include_zero_values:
                line = "{description}: {value}".format(description=field.description, value=field.value())
                logging_lines.append(line)

        logger.info('\n'.join(logging_lines))

    def write_metrics(self):
        raise NotImplementedError
