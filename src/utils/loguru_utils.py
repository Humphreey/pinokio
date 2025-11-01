import sys
import loguru


def setup_loguru_formatting(logger: loguru._Logger, include_pid_tid: bool = False, log_path:str|None = None, levels: list = ["DEBUG", "INFO"]) -> None:
    """
    Настройка форматирования для loguru логгера.

    Настраивает формат логов с цветовой подсветкой, временными метками,
    уровнями логирования и опциональной информацией о процессах/потоках.

    Args:
        logger: Экземпляр loguru логгера
        include_pid_tid: Включать ли информацию о процессе/потоке
        log_path: Путь для записи логов в файл (например, "logs/app_{level}.log")
        levels: Список уровней логирования для записи в файлы
    """
    # Add PID, etc. to logging
    if include_pid_tid:
        pid_tid_clause = "<level>{process}/{thread}</level> | "
    else:
        pid_tid_clause = ""
    logger_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | <level>{message}</level> |"
        + pid_tid_clause
        + "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> |"
    )
    # logger.configure(extra={"ip": "", "user": ""})  # Default values
    logger.remove()
    logger.add(sys.stderr, format=logger_format)
    if log_path:
        # logger.add(log_path, encoding="utf-8") #TBD добавить ротацию (неделю (круглая неделя)), сжатие (2 лог файла (инфо и дебаг))
        # log_path = "path_{level}.log"
        for level in levels:
            logger.add(log_path.format(level=level.lower()), level=level, encoding="utf-8")
