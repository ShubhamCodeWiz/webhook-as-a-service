import logging
import json
from datetime import datetime, timezone

class JSONFormatter(logging.Formatter):
    def format(self, record):
    # Base fields
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger_name": record.name,
        }
        
        # Merge all extra fields dynamically
        # Standard LogRecord attributes to exclude
        skip_fields = {
            "args", "asctime", "created", "exc_info", "exc_text",
            "filename", "funcName", "levelname", "levelno", "lineno",
            "message", "module", "msecs", "msg", "name", "pathname",
            "process", "processName", "relativeCreated", "stack_info",
            "thread", "threadName"
        }
        
        for key, value in record.__dict__.items():
            if key not in skip_fields:
                log_record[key] = value
        
        return json.dumps(log_record)
    
    
    
def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())

        logger.addHandler(handler)

    logger.setLevel(level)

    logger.propagate = False

    return logger