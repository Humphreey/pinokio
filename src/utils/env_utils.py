import os

from dotenv import dotenv_values, find_dotenv, load_dotenv


def load_dotenv_with_details(env_file_path: str | None = None, logger: object | None = None) -> None:
    """
    Поиск и загрузка .env файла с детальным логированием.

    Автоматически находит .env файл в проекте, загружает переменные окружения
    и выводит информацию о количестве загруженных переменных.

    Args:
        env_file_path: Путь к .env файлу (если None - автоматический поиск)
        logger: Логгер для вывода информации (если None - вывод в консоль)

    Raises:
        FileNotFoundError: Если .env файл не найден
    """

    # Find the .env file if not specified
    if env_file_path is None:
        env_file_path = find_dotenv()

    # Check if it exists
    if not os.path.isfile(env_file_path):
        raise FileNotFoundError(f"Cannot autodetect .env file. Optional path: {env_file_path}; {os.getcwd()=}")

    # Count vars in the file
    env_vars_cnt = len(dotenv_values(env_file_path))

    # Load environment variables
    load_dotenv(env_file_path, override=True)
    msg = f"Loaded {env_vars_cnt} vars from .env file: {env_file_path}; {os.getcwd()=}"
    logger.info(msg) if logger else print(msg)  # noqa: T201 print found.
