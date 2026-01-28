from loguru import logger
from pathlib import Path
from config import settings

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level}</level> | "
    "<cyan>{name}</cyan>:<cyan>{file}</cyan>:"
    "<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

log_path = Path(settings.log_directory) / settings.log_file


def setup_logging():
    # Remove ALL default handlers (including stdout)
    logger.remove()

    # SINGLE file sink with rotation
    logger.add(
        log_path,
        format=LOG_FORMAT,
        level=settings.log_level,
        rotation=settings.log_rotation,
        retention=settings.log_retention,
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )

    return logger


log = setup_logging()


def setup_logger(name: str):
    return log.bind(component=name)
