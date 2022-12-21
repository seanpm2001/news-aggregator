from enum import Enum
from typing import Any, Callable, Optional

import structlog


class LogLevel(Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"  # used by uvicorn
    NOTSET = "NOTSET"

    def to_str(self) -> str:
        return self.value.lower()

    def to_method(self, logger: structlog.BoundLogger) -> Optional[Callable[..., Any]]:
        c: Optional[Callable[..., Any]] = None
        if self is LogLevel.DEBUG:
            c = logger.debug
        elif self is LogLevel.INFO:
            c = logger.info
        elif self is LogLevel.WARNING:
            c = logger.warning
        elif self is LogLevel.ERROR:
            c = logger.error
        elif self is LogLevel.CRITICAL:
            c = logger.error
        else:
            raise ValueError(f"Unhandled LogLevel: {self}")
        return c

    def to_int(self) -> int:
        if self is LogLevel.CRITICAL:
            return 50
        if self is LogLevel.ERROR:
            return 40
        if self is LogLevel.WARNING:
            return 30
        if self is LogLevel.INFO:
            return 20
        if self is LogLevel.DEBUG:
            return 10
        if self is LogLevel.TRACE:
            return 5  # must match uvicorn's logging.py TRACE_LOG_LEVEL value
        if self is LogLevel.NOTSET:
            return 0
        raise ValueError(f"Unhandled LogLevel: {self}")
