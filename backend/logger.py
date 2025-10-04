from __future__ import annotations

import datetime
import json
import logging
import threading
import traceback

import loguru
from loguru import logger

from server.settings import logging_settings


_LOGGER_INITIALIZED = False


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except AttributeError:
            level = record.levelno  # type: ignore[assignment]

        frame, depth = logging.currentframe(), 2

        while frame.f_code.co_filename in (logging.__file__,):
            if frame.f_back is None:
                break
            frame = frame.f_back
            depth += 1

        logger.opt(
            depth=depth,
            exception=record.exc_info,
        ).log(
            level,
            record.getMessage(),
        )


def sink_serializer(
    message: loguru.Message,
) -> None:
    record = message.record
    extra = record["extra"]

    simplified = {
        "timestamp": (
            record["time"]
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec="milliseconds")
        ),
        "level": record["level"].name,
        "name": record["name"],
        "path": record["file"].path,
        "function": record["function"],
        "thread_id": threading.current_thread().ident,
        "line": record["line"],
        "message": record["message"],
        "extra": extra,
        "exception": None,
    }

    if (pod_name := logging_settings.pod_name) is not None:
        simplified["inst"] = pod_name

    if (exception := record["exception"]) is not None:
        simplified["exception"] = {
            "type": repr(exception.type),
            "value": exception.value,
            "traceback": "".join(traceback.format_tb(exception.traceback)),
        }

    print(json.dumps(simplified, default=str, ensure_ascii=False))


def init_logger(
    level: str = logging_settings.level,
    fmt: str = logging_settings.format,
    keep_loggers: list[str] | None = None,
    suppress_loggers: list[str] | None = None,
) -> None:
    """
    Initialize logging within loguru
    Propagate messages from logging to loguru

    :param level:
    :param fmt:
    :param keep_loggers:
    :param suppress_loggers:
    :return: None
    """

    global _LOGGER_INITIALIZED  # pylint: disable=W0603
    if _LOGGER_INITIALIZED:
        return

    if keep_loggers is None:
        keep_loggers = [
            "alembic",
            "uvicorn",
            "uvicorn.asgi",
            "uvicorn.error",
            "fastapi",
            "redisbeat",
            "redis_lock",
        ]

    if suppress_loggers is None:
        suppress_loggers = ["uvicorn.access"]

    # no loggers will write to stdout through standard logging
    logging.basicConfig(level=level, handlers=[logging.NullHandler()], force=True)

    logger.remove()
    logger.add(
        sink_serializer,
        backtrace=True,
        colorize=False,
        format=fmt,
        enqueue=False,
        level=level.upper(),
    )

    keep_loggers = keep_loggers or []
    for _log in keep_loggers:
        logging.getLogger(_log).handlers = [InterceptHandler()]

    suppress_loggers = suppress_loggers or []
    for _log in suppress_loggers:
        _logger = logging.getLogger(_log)
        _logger.propagate = False
        _logger.handlers = [logging.NullHandler()]

    _LOGGER_INITIALIZED = True
