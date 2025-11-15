from loguru import logger


def setup_logging():
    logger.remove()
    logger.add(
        "logs/runtime.log",
        rotation="10 MB",
        retention="10 days",
        enqueue=True,
        backtrace=True,
        diagnose=False,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
    )
    logger.add(lambda msg: print(msg, end=""))
    return logger