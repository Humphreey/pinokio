import contextlib
from loguru import logger

from fastapi import FastAPI, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from src.producer import ProducerPinokIO
from src.utils.loguru_utils import setup_loguru_formatting
from src.utils.schemas_kafka import IncomingFromMsRequest
from config import settings


setup_loguru_formatting(logger=logger, include_pid_tid=True)

# Security scheme for Bearer token
security = HTTPBearer()


################################################################################
producer: ProducerPinokIO | None = None

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом FastAPI приложения.
    
    Инициализирует и запускает ProducerPinokIO при старте приложения,
    корректно останавливает его при завершении работы.
    
    Args:
        app: FastAPI приложение
        
    Yields:
        None: Контроль передается приложению для работы
    """
    global producer

    logger.info("lifespan: enter")
    producer = ProducerPinokIO()
    await producer.start_producer()

    yield # running

    logger.info("lifespan: exit")
    await producer.stop_producer()


app = FastAPI(title="PinokIO Service", lifespan=lifespan)


@app.post("/process_request")
async def process_request(
    payload: IncomingFromMsRequest, 
    token: HTTPAuthorizationCredentials = Security(security)
):
    """
    Обработка входящего сообщения от Message Service.
    
    Основной эндпоинт для получения и обработки сообщений от пользователей.
    Выполняет авторизацию, валидацию и передает сообщение в ProducerPinokIO
    для дальнейшей обработки.
    
    Args:
        payload: Данные входящего сообщения (IncomingFromMsRequest)
        token: Bearer токен для авторизации
        
    Returns:
        dict: Результат обработки сообщения с статусом и причиной
        
    Raises:
        HTTPException: 403 если токен авторизации неверный
    """
    # Authorization check
    if token.credentials != settings.BEARER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid bearer token")
    assert producer is not None

    logger.debug(f"[PINOKIO] Получено сообщение: {payload.model_dump_json(indent=2)}")
    result = await producer.process_incoming_message(payload)
    
    return result