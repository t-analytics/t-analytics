import logging
from connectors.connectors._BigQuery import BigQuery
from datetime import datetime


class LogDBHandler(logging.Handler):

    def __init__(self, path_to_json, db_name, db_table):
        logging.Handler.__init__(self)
        self.bq = BigQuery(path_to_json)
        self.db_name = db_name
        self.db_table = db_table

    def emit(self, record):
        log_msg = record.msg
        log_msg = log_msg.strip()
        log_msg = log_msg.replace('\'', '\'\'')

        exc_params_list = list(record.exc_info[:2])
        for number, exc_list_element in enumerate(exc_params_list):
            if exc_list_element is not None:
                exc_params_list[number] = str(exc_list_element)

        exc_class, exc_param = exc_params_list

        list_of_json = {"level_no": record.levelno,
                        "line_no": record.lineno,
                        "level_name": record.levelname,
                        "module": record.module,
                        "msg": log_msg,
                        "file_name": record.filename,
                        "func_name": record.funcName,
                        "exc_class": exc_class,
                        "exc_param": exc_param,
                        "name": record.name,
                        "pathname": record.pathname,
                        "process": record.process,
                        "process_name": record.processName,
                        "thread": record.thread,
                        "thread_name": record.threadName,
                        "status_code": record.status_code,
                        "status_msg": record.status_msg,
                        "created": datetime.strftime(datetime.fromtimestamp(record.created), "%Y-%m-%d %H:%M:%S")}
        self.bq.insert_json(self.db_name, self.db_table, [list_of_json])


def _logger(path_to_json, db_name, db_table, log_error_level, client_name, placement):
    log_db = LogDBHandler(path_to_json, db_name, db_table)
    logging.getLogger(f"{client_name}_{placement}").addHandler(log_db)
    log = logging.getLogger(f"{client_name}_{placement}")
    log.setLevel(log_error_level)
    return log


my_log1 = _logger("t-isobar-lenta-bq.json", "Logging", "Logging", "DEBUG", "LENTA", "VKontakte")


my_log1.debug("Status code", extra={"status_code": 200, "status_msg": "status_msg my_log1"}, exc_info=True)


