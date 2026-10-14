"""Logging configuration and timing helper shared by all pipeline steps."""

import functools
import logging
import time
from collections.abc import Callable

from luxtrade.config import LOG_DIR

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def setup_logging(level: int | str = logging.DEBUG, log_file: str = "pipeline.log") -> None:
    """Configure root logging to console and ``logs/<log_file>``."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format=_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_DIR / log_file, encoding="utf-8"),
        ],
        force=True,
    )


def timed[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """Log start, end and duration of a pipeline step."""
    logger = logging.getLogger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        logger.info("start %s", func.__name__)
        t0 = time.perf_counter()
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.exception("failed %s after %.2fs", func.__name__, time.perf_counter() - t0)
            raise
        finally:
            logger.info("end %s (%.2fs)", func.__name__, time.perf_counter() - t0)

    return wrapper
