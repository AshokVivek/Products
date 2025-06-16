import os

import logging
import logging.config
from datetime import datetime, timezone

import watchtower
from pythonjsonlogger import jsonlogger

CLOUDWATCH_LOG_GROUP = os.environ.get("CLOUDWATCH_LOG_GROUP")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['PIPELINE'] = os.environ.get("PIPELINE")
        
        if not log_record.get('emission_timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['emission_timestamp'] = now

        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

        log_record['message'] = record.message


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": CustomJsonFormatter,
            "format": "%(emission_timestamp)s %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": LOG_LEVEL,
            "formatter": "json",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False
        }
    }
}

if CLOUDWATCH_LOG_GROUP:
    # Integrate CloudWatch to Logging config
    LOGGING_CONFIG['handlers']['cloudwatch'] = {
        'class': 'watchtower.CloudWatchLogHandler',
        'formatter': 'json',
        'level': LOG_LEVEL,
        'log_group_name': CLOUDWATCH_LOG_GROUP,
        "send_interval": 10,  # seconds
        "create_log_group": False
    }
    LOGGING_CONFIG['loggers']['ram_log'] = {
        'handlers': ['cloudwatch'],
        'level': LOG_LEVEL,
        'propagate': False
    }


def fetch_logger():
    formatter = CustomJsonFormatter('%(emission_timestamp)s %(message)s')

    logging.config.dictConfig(LOGGING_CONFIG)

    _logger = logging.getLogger(__name__)
    _logger.setLevel(LOG_LEVEL)

    # Console Stream logger
    console_handler = logging.StreamHandler()
    _logger.addHandler(console_handler)

    # CloudWatch Logger
    if CLOUDWATCH_LOG_GROUP:
        cw_handler = watchtower.CloudWatchLogHandler(log_group=CLOUDWATCH_LOG_GROUP)
        cw_handler.setFormatter(formatter)
        _logger.addHandler(cw_handler)
    return _logger


LOGGER = fetch_logger()
