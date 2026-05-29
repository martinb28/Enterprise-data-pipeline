"""
Structured logger for the Enterprise Data Pipeline.
Provides consistent, machine-parseable log output across all pipeline stages.
"""

import logging
import sys
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger instance.

    Args:
        name: Logger name, typically __name__ of the calling module.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


@contextmanager
def log_stage(logger: logging.Logger, stage_name: str):
    """
    Context manager that logs start/end of a pipeline stage with elapsed time.

    Usage:
        with log_stage(logger, "Bronze Ingestion"):
            ... do work ...
    """
    logger.info(f"{'='*60}")
    logger.info(f"▶  STARTING: {stage_name}")
    logger.info(f"{'='*60}")
    start = time.perf_counter()
    try:
        yield
        elapsed = time.perf_counter() - start
        logger.info(f"{'='*60}")
        logger.info(f"✅ COMPLETED: {stage_name} | Elapsed: {elapsed:.2f}s")
        logger.info(f"{'='*60}")
    except Exception as e:
        elapsed = time.perf_counter() - start
        logger.error(f"{'='*60}")
        logger.error(f"❌ FAILED: {stage_name} | Elapsed: {elapsed:.2f}s | Error: {e}")
        logger.error(f"{'='*60}")
        raise


def log_dataframe_stats(logger: logging.Logger, df, name: str) -> None:
    """
    Logs key statistics about a Spark DataFrame.

    Args:
        logger: Logger instance.
        df: Spark DataFrame.
        name: Human-readable name for the DataFrame.
    """
    try:
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(
            f"📊 DataFrame [{name}] | Rows: {row_count:,} | Columns: {col_count} | "
            f"Schema: {[f.name for f in df.schema.fields]}"
        )
    except Exception as e:
        logger.warning(f"Could not compute stats for [{name}]: {e}")


def pipeline_step(step_name: str) -> Callable:
    """
    Decorator that wraps a function with stage logging.

    Usage:
        @pipeline_step("Load Bronze Orders")
        def load_orders():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger(func.__module__)
            with log_stage(logger, step_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator
