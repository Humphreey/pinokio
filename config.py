import os
import yaml

from loguru import logger
from pydantic_settings import BaseSettings
from pydantic import Field, ValidationError

from src.utils.loguru_utils import setup_loguru_formatting
from src.utils.env_utils import load_dotenv_with_details

# Set cool format for loguru
setup_loguru_formatting(logger)

# Load .env file
load_dotenv_with_details(logger=logger)

class Settings(BaseSettings):
    """
    Настройки приложения PinokIO.
    
    Содержит все необходимые конфигурационные параметры для работы микросервиса,
    включая настройки авторизации, LLM, Redis, Kafka и мониторинга.
    """

    BEARER_TOKEN: str = Field(..., env="BEARER_TOKEN", description="Токен авторизации для API")
    
    DEFAULT_USER_ID_BOT: str = Field(..., env="DEFAULT_USER_ID_BOT", description="ID бота по умолчанию")
    
    KAFKA_SENDER_URL: str = Field(..., env="KAFKA_SENDER_URL", description="URL сервиса отправки в Kafka")

    LLM_URL: str = Field(..., env="LLM_URL", description="URL эндпоинта LLM API")
    LLM_API_KEY: str = Field(..., env="LLM_API_KEY", description="API ключ для LLM")
    LLM_MODEL: str = Field(..., env="LLM_MODEL", description="Модель LLM для использования")

    CHECK_INTERVAL: int = Field(..., env="CHECK_INTERVAL", description="Интервал проверки напоминаний (секунды)")
    

try:
    logger.info(f"Initializing config ..")
    settings = Settings()
    logger.info(f".. success!")
except ValidationError as e:
    logger.error(f"Settings not valid: {e}")
    raise