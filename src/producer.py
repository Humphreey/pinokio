import asyncio
import time
import yaml
from datetime import datetime
from typing import Any, Optional
from loguru import logger   

from src.llm import LLM
from src.redis_client import RedisClient
from src.utils.schemas_kafka import IncomingFromMsRequest, OutgoingToMsResponse
from src.utils.httpx_utils import post_httpx_request
from src.utils.time_manager import should_process_message_by_time
from config import settings


class ProducerPinokIO:

    def __init__(self):
        """
        Инициализация микросервиса ProducerPinokIO.
        
        Загружает конфигурацию чатов, инициализирует Redis клиент и LLM,
        настраивает параметры мониторинга и напоминаний.

        """
        with (open("configs/config_chats.yaml", "r")) as f:
            self.CONFIG_CHATS = yaml.safe_load(f)
        self.redis_client = RedisClient()
        self.llm = LLM()
        self.reminder_interval = settings.CHECK_INTERVAL 
        self.reminder_task: Optional[asyncio.Task] = None
        self.last_silence_notification: dict[str, float] = {}
        
        
        logger.info("✅ Telegram микросервис инициализирован")


    async def process_incoming_message(self, payload: IncomingFromMsRequest):
        """
        Обработать входящее сообщение от Message Service.

        Выполняет полный цикл обработки входящего сообщения:
        1. Проверяет, является ли чат выходным
        2. Проверяет время работы чата
        3. Проверяет, не является ли сообщение изменением
        4. Определяет тип пользователя (MERCHANT/PP)
        5. Классифицирует сообщение через ИИ (для MERCHANT)
        6. Добавляет в Redis очередь
        7. Запускает воркер для обработки

        Args:
            payload: Данные входящего сообщения (IncomingFromMsRequest)

        Returns:
            dict: Результат обработки с ключами:
                - status: "processed"|"ignored"|"blocked"
                - reason: Причина обработки/игнорирования/блокировки
        """

        chat_id = payload.messages__chat_id

        if chat_id not in self.CONFIG_CHATS:
            logger.error(f"[PINOKIO] Чат {chat_id} не является входным чатом")
            return {"status": "ignored", "reason": "chat_not_found"}

        #########################################################
        # Проверяем время доставки сообщения
        if should_process_message_by_time(
            str(payload.messages__date), 
            self.CONFIG_CHATS[chat_id]['pinger']
        ) == False:
            return {"status": "blocked", "reason": "time_blocked"}

        #########################################################
        # Проверяем, не является ли сообщение изменением другого сообщения
        if payload.text_histories__change_id is not None:
            return {"status": "ignored", "reason": "change message"}

        #########################################################
        # Запускаем тишину на любое сообщение в чате если включено в конфиге
        if self.CONFIG_CHATS[chat_id]["silencer"]["enabled"] == True:
            self.last_silence_notification.update({chat_id: time.time()})
            logger.info(f"[PINOKIO] last_silence_notification: {self.last_silence_notification}")
        #----------------------------------------------------------
        
        # Проверяем соединение с Redis
        logger.info(f"[REDIS] 🔔 Ping to Redis - {self.redis_client.ping()}")
        self._ensure_worker_for_chat(chat_id)
        
        # Получаем данные из сообщения
        messages_id = payload.messages__id
        user_id = payload.messages__user_id
        username = payload.messages__username or "unknown"
        text = payload.text_histories__text or ""
        whitelist = self.CONFIG_CHATS[chat_id]['pinger']['whitelist']
        bot_enabled = self.CONFIG_CHATS[chat_id]['pinger']['bot_enabled']


        if f"@{username}" in whitelist:
            user_type = "pp"
        elif user_id == settings.DEFAULT_USER_ID_BOT:
            user_type = "pp"
            # TODO: user_type = "bot" добавить обработку сообщений от бота
            if bot_enabled == False:                
                return {"status": "ignored", "reason": "bot_disabled"}
        else:
            user_type = "merchant"


        #########################################################
        # Обработка сообщений
        if user_type == "merchant":
            # Проверяем, не является ли сообщение продолжением активной серии
            status = self.redis_client.get_chat_status(chat_id)
            active_series = status.get("active_series") or {}
            if active_series and active_series.get("user_id") == user_id:
                redis_message_id = self.redis_client.add_message(
                    chat_id, messages_id, user_id, username, "merchant", text
                )
                logger.info(f"[PINOKIO] Продолжаем активную серию пользователя {user_id}: {redis_message_id}")
                return {"status": "in processing", "message_id": redis_message_id}

            appended_id = self._append_to_last_long_for_user(chat_id, user_id, username, text)
            if appended_id is not None:
                logger.info(f"[PINOKIO] Текст добавлен к последнему LONG сообщению пользователя {user_id}: {appended_id}")
                return {"status": "in processing", "message_id": appended_id}

            #########################################################
            need_response = self.llm.classify_text(text)
            if need_response.get("class") == 1:
                logger.info(f"[PINOKIO] Merchant сообщение требует ответа: {need_response['class']}")
                redis_message_id = self.redis_client.add_message(chat_id, messages_id, user_id, username, user_type, text)
                return {"status": "in processing", "message_id": redis_message_id}
            else:
                logger.info(f"[PINOKIO] Merchant сообщение НЕ требует ответа")
                return {"status": "ignored", "reason": "no_response_needed"}
        elif user_type == "pp":
            redis_message_id = self.redis_client.add_message(chat_id, messages_id, user_id, username, user_type, text)
            logger.info(f"[PINOKIO] PP сообщение добавлено в очередь: {redis_message_id}")
            if payload.messages__parent_message_id is not None:
                self.process_message_if_reply_exist(chat_id, redis_message_id, payload.messages__parent_message_id)
                return {"status": "in processing", "message_id": redis_message_id}
            else:
                self.process_message_with_pp_response(chat_id, redis_message_id, username, text)
                return {"status": "in processing", "message_id": redis_message_id}


    def process_message_if_reply_exist(self, chat_id: str, redis_message_id: str, parent_message_id: str) -> None:
        """
        Обработать сообщение, которое является ответом на другое сообщение.
        
        Удаляет родительское сообщение из очереди, так как на него был дан ответ.
        Используется для обработки ответов операторов поддержки на вопросы клиентов.
        
        Args:
            chat_id: ID чата
            redis_message_id: ID сообщения в Redis
            parent_message_id: ID родительского сообщения, на которое отвечают
        """
        # удаляем из [SHORT] очереди
        self.redis_client.redis.xdel(
            self.redis_client.raw_stream_tpl.format(chat_id=chat_id),
            redis_message_id,
        )
        logger.info(f"[PINOKIO] Сообщение {redis_message_id} удалено из [SHORT] очереди")
        logger.info(f"[PINOKIO] Обработка ответа: сообщение {redis_message_id} отвечает на {parent_message_id}")
        final_messages = self.redis_client.get_final_messages(chat_id, 100)
        parent_found = False
        for redis_stream_id, fields in final_messages:
            if fields.get("messages_id") == parent_message_id:
                self.redis_client.redis.xdel(
                    self.redis_client.final_stream_tpl.format(chat_id=chat_id),
                    redis_stream_id
                )
                logger.info(f"[PINOKIO] Родительское сообщение {parent_message_id} удалено из [LONG] очереди")
                parent_found = True
                break
        if not parent_found:
            logger.warning(f"[PINOKIO] ⚠️ Родительское сообщение {parent_message_id} не найдено в [LONG] очереди")


    def process_message_with_pp_response(self, chat_id: str, redis_message_id: str, username: str, text: str) -> None:
        """
        Обработать сообщение с ответом от PP без привязки к конкретному вопросу.
        
        Использует ИИ для сопоставления ответа оператора с вопросом клиента
        и удаляет сопоставленное сообщение из очереди.
        
        Args:
            chat_id: ID чата
            redis_message_id: ID сообщения в Redis
            username: Имя пользователя оператора
            text: Текст ответа оператора
        """
        # удаляем из [SHORT] очереди
        self.redis_client.redis.xdel(
            self.redis_client.raw_stream_tpl.format(chat_id=chat_id),
            redis_message_id,
        )
        current_merchant_messages = self._get_merchant_messages(chat_id)
        if not current_merchant_messages:
            logger.info(f"[PINOKIO] Нет merchant сообщений в [LONG] очереди для чата {chat_id}")
            return
        logger.info(f"[PINOKIO] current_merchant_messages: {current_merchant_messages}")
        logger.info(f"[PINOKIO] 🔍 ИИ: сопоставление ответа с вопросом: username={username}, text={text[:50]}...")
        outcome = self.llm.match_answer_to_question(current_merchant_messages, text)
        matched_local_id = outcome.get("matched_message_id") if isinstance(outcome, dict) else None
        if matched_local_id is None:
            logger.info(f"[PINOKIO] ИИ: не удалось сопоставить ответ с merchant сообщением")
            return
        # Удаляем сопоставленное merchant сообщение из [LONG] очереди
        self.redis_client.redis.xdel(
            self.redis_client.final_stream_tpl.format(chat_id=chat_id),
            matched_local_id,
        )
        logger.info(f"[PINOKIO] ИИ: сопоставлено и удалено merchant сообщение из [LONG] очереди: {matched_local_id}")


    async def send_message(self, chat_id: str, message: dict[str, Any], age_seconds: int) -> None:
        """
        Отправить напоминание о висящем сообщении через Kafka и удалить его из очереди.

        Формирует уведомление о висящем сообщении и отправляет его операторам поддержки
        через Kafka Sender. После отправки удаляет сообщение из долгосрочной очереди.

        Args:
            chat_id: ID чата
            message: Данные сообщения из Redis
            age_seconds: Возраст сообщения в секундах

        Returns:
            None
        """
        logger.info(f"📤 Отправляем напоминание о висящем сообщении: {message!r}")
        
        username = message.get("username", "unknown")
        text = message.get("text", "")
        from_chat = self.CONFIG_CHATS[chat_id]['input_chat_name']

        message_timeout = self.CONFIG_CHATS[chat_id]['pinger'].get('message_timeout', 30)
        output_text = (
            f"[PINOKIO] [{from_chat}] Напоминание для {' @'.join(self.CONFIG_CHATS[chat_id]['pinger']['whitelist'])}: \n"
            f"Сообщение от @{username} висит уже {age_seconds} секунд (таймаут {message_timeout}):\n\n"
            f"Текст сообщения:  \n"
            f"{text}\n"
        )

        try:
            kafka_response = OutgoingToMsResponse(
                chats__id=self.CONFIG_CHATS[chat_id]['pinger']['output_chat_id'],
                thread_id=None,
                text_histories__text=output_text,
                users__id=settings.DEFAULT_USER_ID_BOT,
            )
            url = f"{settings.KAFKA_SENDER_URL}/send_kafka"
            response = await post_httpx_request(
                url=url, 
                data=kafka_response.model_dump(),
                headers={"Authorization": f"Bearer {settings.BEARER_TOKEN}"}
            )
            logger.info(f"📤 Напоминание отправлено через Kafka: {response.json()}")

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке напоминания через Kafka: {e}")

        redis_stream_id = message.get("redis_stream_id")
        if redis_stream_id:
            self.redis_client.redis.xdel(
                self.redis_client.final_stream_tpl.format(chat_id=chat_id), 
                redis_stream_id
            )
            logger.info(f"🗑️ Сообщение удалено из [LONG] очереди после напоминания: {redis_stream_id}")

    async def send_silence_notification(self, chat_id: str, silence_duration: int) -> None:
        """
        Отправить уведомление о тишине в чате.
        
        Отправляет уведомление операторам поддержки о том, что в чате долгое время
        нет сообщений в очереди. Используется для контроля активности чатов.
        
        Args:
            chat_id: ID чата
            silence_duration: Длительность тишины в секундах
        """
        logger.info(f"[PINOKIO] Отправляем уведомление о тишине в чате {chat_id}: {silence_duration} секунд")
        
        from_chat = self.CONFIG_CHATS[chat_id]['input_chat_name']
        output_text = (
            f"[PINOKIO] [{from_chat}] Уведомление о тишине! \n"
            f"Во входящем чате нет сообщений в очереди уже {silence_duration} секунд.\n"
            f"Возможно, стоит проверить активность в чате."
        )

        try:
            kafka_response = OutgoingToMsResponse(
                chats__id=self.CONFIG_CHATS[chat_id]['silencer']['output_chat_id'],
                thread_id=None,
                text_histories__text=output_text,
                users__id=settings.DEFAULT_USER_ID_BOT,
            )
            url = f"{settings.KAFKA_SENDER_URL}/send_kafka"
            response = await post_httpx_request(
                url=url, 
                data=kafka_response.model_dump(),
                headers={"Authorization": f"Bearer {settings.BEARER_TOKEN}"}
            )
            logger.info(f"📤 Уведомление о тишине отправлено через Kafka: {response.json()}")

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке уведомления о тишине через Kafka: {e}")


#############################################################################
#============================ СЛУЖЕБНЫЕ ФУНКЦИИ =============================


    def _get_merchant_messages(self, chat_id: str) -> list[dict[str, Any]]:
        """
        Получить merchant сообщения из долгосрочной очереди для чата.

        Извлекает все сообщения от мерчантов из долгосрочной очереди Redis,
        которые ожидают ответа от операторов поддержки.

        Args:
            chat_id: ID чата

        Returns:
            list[dict]: Список сообщений с полями:
                - redis_stream_id: ID сообщения в Redis
                - username: Имя пользователя
                - text: Текст сообщения
        """
        merchant_messages = []
        final_messages = self.redis_client.get_final_messages(chat_id, 50)

        for redis_stream_id, fields in final_messages:
            if fields.get("user_type") == "merchant":
                merchant_messages.append(
                    {
                        "chat_id": chat_id,
                        "messages_id": fields.get("messages_id"),
                        "user_id": fields.get("user_id"),
                        "username": fields.get("username"),
                        "text": fields.get("text"),
                        "end_ts": fields.get("end_ts"),
                        "start_ts": fields.get("start_ts"),
                        "redis_stream_id": redis_stream_id,
                    }
                )

        return merchant_messages


    def _ensure_worker_for_chat(self, chat_id: str) -> None:
        """
        Убедиться, что воркер запущен для чата.

        Проверяет, запущен ли воркер для обработки сообщений конкретного чата.
        Если воркер не запущен, запускает его.

        Args:
            chat_id: ID чата
        """
        # Применяем окно агрегации из конфига чата, если задано
        try:
            window_cfg = self.CONFIG_CHATS[chat_id]['pinger'].get('redis_buffer_window', 20)
            if isinstance(window_cfg, int):
                self.redis_client.set_window(chat_id, window_cfg)
        except Exception:
            pass

        status = self.redis_client.get_chat_status(chat_id)
        if not status.get("worker_running", False):
            self.redis_client.start_worker(chat_id)


    async def start_producer(self) -> None:
        """
        Запустить микросервис.

        Инициализирует и запускает все необходимые компоненты:
        - Redis планировщик для обработки очередей
        - Мониторинг напоминаний о висящих сообщениях
        - Мониторинг тишины в чатах
        """
        try:
            logger.info("[PINOKIO] start_producer -> starting scheduler")
            self.redis_client.start_scheduler()
            
            
            logger.info("[PINOKIO] start_producer -> starting reminder monitor task")
            self.reminder_task = asyncio.create_task(self._start_reminder_monitor())
            logger.info("✅ PINOKIO микросервис запущен")
            logger.info(f"✅ Мониторинг напоминаний запущен (интервал: {self.reminder_interval}c")
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске микросервиса: {e}")
            raise


    async def stop_producer(self) -> None:
        """
        Остановить микросервис.

        Корректно останавливает все компоненты:
        - Отменяет задачи мониторинга
        - Останавливает Redis планировщик и воркеры
        - Сбрасывает все активные серии
        """
        if self.reminder_task:
            self.reminder_task.cancel()
            try:
                await self.reminder_task
            except asyncio.CancelledError:
                pass
            logger.info("🛑 Мониторинг напоминаний остановлен")
        self.redis_client.stop_all()
        logger.info("🛑 PINOKIO микросервис остановлен")    


    async def _start_reminder_monitor(self) -> None:
        """
        Запустить мониторинг напоминаний.

        Основной цикл мониторинга, который периодически проверяет:
        - Висящие сообщения в долгосрочной очереди
        - Тишину в чатах (отсутствие активности)
        - Отправляет соответствующие уведомления
        """
        while True:
            try:
                await self._check_pending_messages()
                await asyncio.sleep(self.reminder_interval)
            except Exception as e:
                logger.error(f"Ошибка в мониторинге напоминаний: {e}")
                await asyncio.sleep(self.reminder_interval)


    async def _check_pending_messages(self) -> None:
        """
        Проверить висящие сообщения и отправить напоминания.

        Выполняет проверку всех активных чатов:
        1. Находит сообщения, которые висят дольше MESSAGE_TIMEOUT секунд
        2. Отправляет напоминания о таких сообщениях
        3. Проверяет тишину в чатах (отсутствие сообщений в очереди)
        4. Отправляет уведомления о тишине при необходимости
        """
        current_time = time.time()
        all_chats = self.redis_client.get_all_chats_status()

        for chat_id, chat_status in all_chats.items():
            if not chat_status.get("worker_running", False):
                logger.debug(f"[PINOKIO] check_pending -> skip chat={chat_id} worker_running=False")
                continue

            # Получаем merchant сообщения из LONG очереди
            merchant_messages = self._get_merchant_messages(chat_id)
            
            silence_mode = self.CONFIG_CHATS[chat_id]["silencer"]["enabled"]
            current_datetime = datetime.fromtimestamp(current_time)
            should_process = should_process_message_by_time(
                current_datetime.isoformat(), 
                self.CONFIG_CHATS[chat_id]['pinger']
            )
            if silence_mode == True and should_process == True:
                if merchant_messages:
                    self.last_silence_notification.update({chat_id: current_time})
                else:
                    last_notification = self.last_silence_notification.get(chat_id, None)
                    silence_timeout = self.CONFIG_CHATS[chat_id]['silencer'].get('silence_timeout', 90)
                    if last_notification is not None and (current_time - last_notification) > silence_timeout:
                        logger.info(f"[PINOKIO] silence -> chat={chat_id} -> send notification")
                        await self.send_silence_notification(chat_id, silence_timeout)
                        self.last_silence_notification.update({chat_id: current_time})

            # Существующая логика для висящих сообщений
            for msg in merchant_messages:
                end_ts_str = msg.get("end_ts", "0")
                end_ts = float(end_ts_str)
                age_seconds = int(current_time - end_ts)

                message_timeout = self.CONFIG_CHATS[chat_id]['pinger'].get('message_timeout', 30)
                if age_seconds > message_timeout:
                    logger.debug(f"[PINOKIO] reminder -> chat={chat_id} id={msg.get('message_id')} age={age_seconds}s > timeout={message_timeout}s -> send")
                    await self.send_message(chat_id, msg, age_seconds)


    def _append_to_last_long_for_user(self, chat_id: str, user_id: str, username: str, text: str) -> str | None:
        """
        Найти последнее LONG сообщение мерчанта с данным user_id и дописать к нему текст.
        Реализовано как: создать новую объединённую запись и удалить старую (Streams неизменяемы).
        """
        final_messages = self.redis_client.get_final_messages(chat_id, 100)
        for redis_stream_id, fields in final_messages:
            if fields.get("user_type") != "merchant":
                continue
            if fields.get("user_id") != user_id:
                continue

            existing_text = fields.get("text", "")
            new_text = f"{existing_text}\n{text}" if existing_text else text

            start_ts = fields.get("start_ts")
            last_ts = time.time()
            try:
                count_prev = int(fields.get("count", "1"))
            except Exception:
                count_prev = 1
            new_count = str(count_prev + 1)

            new_entry = {
                "user_id": user_id,
                "messages_id": fields.get("messages_id", ""),
                "username": username or fields.get("username", "unknown"),
                "user_type": "merchant",
                "text": new_text,
                "start_ts": str(start_ts),
                "end_ts": str(last_ts),
                "count": new_count,
                "type": "long",
            }

            final_stream = self.redis_client.final_stream_tpl.format(chat_id=chat_id)
            new_id = self.redis_client.redis.xadd(final_stream, new_entry)
            # удалить старую запись
            self.redis_client.redis.xdel(final_stream, redis_stream_id)
            logger.info(f"[PINOKIO] LONG объединено: old_id={redis_stream_id} -> new_id={new_id}")
            return new_id

        return None