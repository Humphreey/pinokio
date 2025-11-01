import os
import threading
import time
from typing import Any

import redis
import yaml

from loguru import logger


class RedisClient:
    """
    Клиент для работы с двумя очередями Redis: краткосрочной и долгосрочной.

    Логика работы:
    1. Все входящие сообщения попадают в краткосрочную очередь (stream:chat:{chat_id})
    2. Воркер читает события и ведёт буфер по пользователям
    3. Если от того же пользователя приходит новое сообщение в течение окна (2 сек) - объединяем
    4. Если окно истекло или пришёл другой пользователь - переносим в долгосрочную очередь
    5. Планировщик периодически сбрасывает просроченные серии
    """

    def __init__(self, config_path: str = "configs/config_redis.yaml"):
        """
        Инициализация клиента Redis из конфигурационного файла.

        Создает клиент для работы с двухуровневой системой очередей Redis:
        - Краткосрочная очередь для буферизации сообщений
        - Долгосрочная очередь для финальных сообщений
        - Планировщик для автоматического сброса просроченных серий
        - Воркеры для обработки сообщений

        Args:
            config_path: Путь к YAML конфигурационному файлу (по умолчанию "configs/config_redis.yaml")
        """
        self.config = self._load_config(config_path)

        # Инициализируем Redis подключение
        redis_config = self.config["redis"]
        self.redis = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            db=redis_config["db"],
            password=redis_config["password"],
            decode_responses=redis_config["decode_responses"],
        )

        keys_config = self.config["keys"]
        self.raw_stream_tpl = keys_config["raw_stream"]
        self.final_stream_tpl = keys_config["final_stream"]
        self.agg_hash_tpl = keys_config["agg_hash"]
        self.sched_zset = keys_config["sched_zset"]
        self.conf_hash_tpl = keys_config["conf_hash"]
        self.metrics_hash_tpl = keys_config["metrics_hash"]

        agg_config = self.config["aggregation"]
        self.window_seconds_default = agg_config["window_seconds_default"]
        self.group_name = agg_config["group_name"]

        self.worker_config = self.config["workers"]

        self.scheduler_config = self.config["scheduler"]

        self._stop_events: dict[str, threading.Event] = {}
        self._threads: list[threading.Thread] = []
        self._flush_locks: dict[str, threading.Lock] = {}

        logger.debug(f"[REDIS_CLIENT] ✅ RedisClient инициализирован: {redis_config['host']}:{redis_config['port']}, db={redis_config['db']}")


    def _load_config(self, config_path: str) -> dict[str, Any]:
        """
        Загрузить конфигурацию из YAML файла.

        Args:
            config_path: Путь к конфигурационному файлу

        Returns:
            dict: Словарь с конфигурацией Redis, ключей, агрегации, воркеров и планировщика
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config


    def ping(self) -> bool:
        """
        Проверка подключения к Redis.

        Returns:
            bool: True если подключение работает, False если нет
        """
        try:
            return self.redis.ping()
        except Exception as e:
            logger.debug(f"[REDIS_CLIENT] ❌ Ошибка подключения к Redis: {e}")
            return False

    def set_window(self, chat_id: str, seconds: int) -> None:
        """
        Установить окно агрегации для конкретного чата.

        Args:
            chat_id: ID чата
            seconds: размер окна в секундах (сколько ждать перед сбросом серии)
        """
        self.redis.hset(self.conf_hash_tpl.format(chat_id=chat_id), mapping={"window_s": str(seconds)})
        logger.debug(f"[REDIS_CLIENT] Окно для чата {chat_id} установлено: {seconds} сек")

    def get_window(self, chat_id: str) -> int:
        """
        Получить размер окна агрегации для чата.

        Args:
            chat_id: ID чата

        Returns:
            Размер окна в секундах (по умолчанию 2)
        """
        value = self.redis.hget(self.conf_hash_tpl.format(chat_id=chat_id), "window_s")
        return int(value) if value else self.window_seconds_default

    def get_config(self, chat_id: str) -> dict[str, str]:
        """
        Получить всю конфигурацию чата.

        Args:
            chat_id: ID чата

        Returns:
            Словарь с настройками чата
        """
        return self.redis.hgetall(self.conf_hash_tpl.format(chat_id=chat_id))

    def _increment_metric(self, chat_id: str, field: str, by: int = 1) -> None:
        """
        Увеличить метрику для чата (внутренний метод). (НЕ ИСПОЛЬЗУЕТСЯ)

        Args:
            chat_id: ID чата
            field: название метрики
            by: на сколько увеличить
        """
        self.redis.hincrby(self.metrics_hash_tpl.format(chat_id=chat_id), field, by)

    def get_metrics(self, chat_id: str) -> dict[str, str]:
        """
        Получить все метрики чата. (НЕ ИСПОЛЬЗУЕТСЯ)

        Args:
            chat_id: ID чата

        Returns:
            Словарь с метриками
        """
        return self.redis.hgetall(self.metrics_hash_tpl.format(chat_id=chat_id))

    def reset_metrics(self, chat_id: str) -> None:
        """
        Сбросить все метрики чата.

        Args:
            chat_id: ID чата
        """
        self.redis.delete(self.metrics_hash_tpl.format(chat_id=chat_id))
        logger.debug(f"[REDIS_CLIENT] 📊 Метрики чата {chat_id} сброшены")


    # ==================== ОСНОВНЫЕ ОПЕРАЦИИ ====================
    def add_message(
        self,
        chat_id: str,
        messages_id: str,
        user_id: str,
        username: str,
        user_type: str,
        text: str,
        timestamp: float | None = None,
    ) -> str:
        """
        Добавить новое сообщение в краткосрочную очередь.

        Это основная функция для отправки сообщений. Сообщение попадает в стрим чата,
        откуда его подхватит воркер и обработает согласно логике агрегации.

        Args:
            chat_id: ID чата
            messages_id: ID сообщения
            user_id: ID пользователя
            username: Никнейм пользователя (без @)
            user_type: Тип пользователя ('merchant' или 'pp')
            text: Текст сообщения
            timestamp: Время сообщения (если не указано - текущее время)

        Returns:
            str: ID сообщения в стриме Redis
        """
        if timestamp is None:
            timestamp = time.time()

        message_data = {
            "user_id": user_id,
            "messages_id": messages_id,
            "username": username,
            "user_type": user_type,
            "text": text,
            "timestamp": str(timestamp),
            "type": "short",  # помечаем как краткосрочное
        }

        stream_key = self.raw_stream_tpl.format(chat_id=chat_id)
        message_id = self.redis.xadd(stream_key, message_data)
        self._increment_metric(chat_id, "messages_received")
        logger.info(f"[REDIS_CLIENT] 📄 Cообщение {messages_id=} добавлено в краткосрочную очередь: {message_data=}")
        return message_id


    def _flush_series(self, chat_id: str, current_time: float | None = None) -> str | None:
        """
        Сбросить текущую серию сообщений в долгосрочную очередь (внутренний метод).

        Эта функция вызывается когда:
        1. Истёк дедлайн ожидания (таймер)
        2. Пришло сообщение от другого пользователя
        3. Принудительный сброс серии

        Args:
            chat_id: ID чата
            current_time: текущее время (если не указано - берётся time.time())

        Returns:
            ID финального сообщения или None если серии не было
        """
        if current_time is None:
            current_time = time.time()

        if chat_id not in self._flush_locks:
            self._flush_locks[chat_id] = threading.Lock()

        with self._flush_locks[chat_id]:
            agg_key = self.agg_hash_tpl.format(chat_id=chat_id)
            series_data = self.redis.hgetall(agg_key)

            if not series_data:
                logger.debug(f"[REDIS_CLIENT] no_active_series {chat_id=}")
                return None

            user_id = series_data.get("user_id", "")
            messages_id = series_data.get("messages_id", "")
            username = series_data.get("username", "")
            user_type = series_data.get("user_type", "")
            text = series_data.get("text", "")
            start_ts = float(series_data.get("start_ts", current_time))
            last_ts = float(series_data.get("last_ts", current_time))
            count = int(series_data.get("count", "1"))

            final_message_data = {
                "user_id": user_id,
                "messages_id": messages_id,
                "username": username,
                "user_type": user_type,
                "text": text,
                "start_ts": str(start_ts),
                "end_ts": str(last_ts),
                "count": str(count),
                "type": "long",
            }


            final_stream = self.final_stream_tpl.format(chat_id=chat_id)
            final_message_id = self.redis.xadd(final_stream, final_message_data)
            logger.debug(f"[REDIS_CLIENT] сообщение {messages_id=} сброшено в долгосрочную очередь: {final_message_data=}")

            self.redis.delete(agg_key)
            self.redis.zrem(self.sched_zset, chat_id)

            self._increment_metric(chat_id, "series_flushed")
            self._increment_metric(chat_id, "messages_aggregated", count)

            return final_message_id

    def _schedule_deadline(self, chat_id: str, deadline_timestamp: float) -> None:
        """
        Запланировать дедлайн для сброса серии (внутренний метод).

        Добавляет чат в планировщик с указанным временем дедлайна.
        Планировщик будет периодически проверять эти дедлайны и сбрасывать
        просроченные серии.

        Args:
            chat_id: ID чата
            deadline_timestamp: время дедлайна (Unix timestamp)
        """
        self.redis.zadd(self.sched_zset, {chat_id: deadline_timestamp})
        logger.info(f"[REDIS_CLIENT] Дедлайн для чата {chat_id=} установлен: {deadline_timestamp:.3f}")


    def process_message(
        self,
        chat_id: str,
        messages_id: str,
        user_id: str,
        username: str,
        user_type: str,
        text: str,
        window_seconds: int | None = None,
    ) -> None:
        """
        Обработать входящее сообщение согласно логике агрегации.

        Это основная логика системы:
        1. Если буфер пуст - создаём новую серию
        2. Если тот же автор в окне - объединяем с существующей серией
        3. Если другой автор или окно истекло - сбрасываем старую серию и создаём новую

        Args:
            chat_id: ID чата
            messages_id: ID сообщения
            user_id: ID пользователя
            username: Никнейм пользователя
            user_type: Тип пользователя ('merchant' или 'pp')
            text: Текст сообщения
            window_seconds: Размер окна агрегации (если не указан - берётся из конфига)
        """
        current_time = time.time()

        if window_seconds is None:
            window_seconds = self.get_window(chat_id)

        agg_key = self.agg_hash_tpl.format(chat_id=chat_id)

        current_series = self.redis.hgetall(agg_key)

        if not current_series:
            series_data = {
                "user_id": user_id,
                "messages_id": messages_id,
                "username": username,
                "user_type": user_type,
                "text": text,
                "start_ts": str(current_time),
                "last_ts": str(current_time),
                "count": "1",
            }

            self.redis.hset(agg_key, mapping=series_data)
            next_deadline = current_time + window_seconds
            self._schedule_deadline(chat_id, next_deadline)
            return

        same_author = current_series.get("user_id") == user_id
        last_message_time = float(current_series.get("last_ts", current_time))
        time_since_last = current_time - last_message_time

        if same_author and time_since_last <= window_seconds:
            existing_text = current_series.get("text", "")
            new_text = f"{existing_text}\n{text}" if existing_text else text

            new_count = str(int(current_series.get("count", "1")) + 1)

            update_data = {"text": new_text, "last_ts": str(current_time), "count": new_count}

            self.redis.hset(agg_key, mapping=update_data)
            next_deadline = current_time + window_seconds
            logger.info(f"[REDIS_CLIENT] Сообщение {messages_id=} расширено новым сообщением")
            self._schedule_deadline(chat_id, next_deadline)

            return

        self._flush_series(chat_id, current_time)

        new_series_data = {
            "user_id": user_id,
            "messages_id": messages_id,
            "username": username,
            "user_type": user_type,
            "text": text,
            "start_ts": str(current_time),
            "last_ts": str(current_time),
            "count": "1",
        }

        self.redis.hset(agg_key, mapping=new_series_data)
        next_deadline = current_time + window_seconds
        logger.debug(f"[REDIS_CLIENT] new_series_after_flush -> chat={chat_id} deadline={next_deadline:.3f}")
        self._schedule_deadline(chat_id, next_deadline)

    # ==================== ПЛАНИРОВЩИК ====================

    def scheduler_tick(self, max_batch: int | None = None) -> int:
        """
        Один тик планировщика - проверяет и сбрасывает просроченные серии.

        Эта функция вызывается периодически (например, каждые 200мс) для проверки
        дедлайнов. Находит все чаты, у которых истёк дедлайн, и сбрасывает их серии
        в долгосрочную очередь.

        Args:
            max_batch: максимальное количество чатов для обработки за один тик (если не указан - из конфига)

        Returns:
            Количество сброшенных серий
        """
        if max_batch is None:
            max_batch = self.worker_config["max_batch"]

        current_time = time.time()

        expired_chats = self.redis.zrangebyscore(self.sched_zset, 0, current_time, start=0, num=max_batch)

        flushed_count = 0

        for chat_id in expired_chats:
            logger.debug(f"[REDIS_CLIENT] scheduler_tick -> expired_chat={chat_id} now={current_time:.3f}")
            if self._flush_series(chat_id, current_time):
                flushed_count += 1

        if flushed_count > 0:
            logger.debug(f"[REDIS_CLIENT] ⏰ Планировщик сбросил {flushed_count} просроченных серий")

        return flushed_count

    def foREDIS_CLIENTe_flush_chat(self, chat_id: str) -> str | None:
        """
        Принудительно сбросить серию для конкретного чата.

        Полезно когда нужно немедленно завершить текущую серию,
        например, при закрытии чата или смене режима.

        Args:
            chat_id: ID чата

        Returns:
            ID финального сообщения или None если серии не было
        """
        result = self._flush_series(chat_id)
        if result:
            logger.debug(f"[REDIS_CLIENT] 🔨 Принудительный сброс серии в чате {chat_id}")
        return result

    def _flush_all(self) -> dict[str, str | None]:
        """
        Принудительно сбросить все активные серии.

        Returns:
            Словарь {chat_id: message_id} для всех сброшенных серий
        """
        all_chats = self.redis.zrange(self.sched_zset, 0, -1)
        results = {}

        for chat_id in all_chats:
            logger.debug(f"[REDIS_CLIENT] self._flush_all() -> chat={chat_id}")
            results[chat_id] = self._flush_series(chat_id)

        logger.debug(f"[REDIS_CLIENT] 🔨 Принудительный сброс всех серий: {len(results)} чатов")
        return results

    def _ensure_consumer_group(self, chat_id: str) -> None:
        """
        Создать consumer group для чата (внутренний метод).

        Consumer group нужен для чтения стрима несколькими воркерами.
        Каждое сообщение будет обработано только одним воркером.

        Args:
            chat_id: ID чата
        """
        stream = self.raw_stream_tpl.format(chat_id=chat_id)
        logger.debug(f"[REDIS_CLIENT] ensure_consumer_group -> enter chat={chat_id} stream={stream} group={self.group_name}")
        try:
            # Используем id="0-0", чтобы группа получила и уже существующие сообщения (до создания группы)
            self.redis.xgroup_create(stream, self.group_name, id="0-0", mkstream=True)
            logger.debug(f"[REDIS_CLIENT] ensure_consumer_group -> created group name={self.group_name} stream={stream} id=0-0")
        except redis.ResponseError as e:
            # Группа уже существует
            logger.debug(f"[REDIS_CLIENT] ensure_consumer_group -> exists group name={self.group_name} stream={stream}: {e}")
        except Exception as e:
            logger.debug(f"[REDIS_CLIENT] ensure_consumer_group -> error chat={chat_id} stream={stream} err={e}")

    def _chat_worker(self, chat_id: str, stop_event: threading.Event, block_ms: int | None = None) -> None:
        """
        Воркер для обработки сообщений конкретного чата (внутренний метод).

        Этот метод работает в отдельном потоке и:
        1. Читает новые сообщения из стрима чата
        2. Обрабатывает каждое сообщение через process_message
        3. Подтверждает обработку (ACK)
        4. Периодически запускает планировщик

        Args:
            chat_id: ID чата
            stop_event: событие для остановки воркера
            block_ms: время блокировки при чтении стрима (мс, если не указан - из конфига)
        """
        if block_ms is None:
            block_ms = self.worker_config["block_ms"]

        self._ensure_consumer_group(chat_id)

        consumer_name = f"worker_{chat_id}_{threading.current_thread().ident}"

        while not stop_event.is_set():
            try:
                response = self.redis.xreadgroup(
                    self.group_name,
                    consumer_name,
                    streams={self.raw_stream_tpl.format(chat_id=chat_id): ">"},
                    count=64,
                    block=block_ms,
                )

                if response:
                    _, entries = response[0]
                    for message_id, fields in entries:
                        logger.debug(f"[REDIS_CLIENT] worker({chat_id}) -> got id={message_id} fields={fields}")
                        user_id = fields.get("user_id", "")
                        messages_id = fields.get("messages_id", "")
                        username = fields.get("username", "")
                        user_type = fields.get("user_type", "")
                        text = fields.get("text", "")

                        self.process_message(chat_id, messages_id, user_id, username, user_type, text)

                        self.redis.xack(self.raw_stream_tpl.format(chat_id=chat_id), self.group_name, message_id)
                        logger.debug(f"[REDIS_CLIENT] worker({chat_id}) -> ack id={message_id}")

            except Exception as e:
                logger.debug(f"[REDIS_CLIENT] ❌ Ошибка в воркере чата {chat_id}: {e}")
                time.sleep(1)

        logger.debug(f"[REDIS_CLIENT] 🛑 Воркер остановлен для чата {chat_id}")

    def _scheduler_loop(self, stop_event: threading.Event, interval_ms: int | None = None) -> None:
        """
        Основной цикл планировщика (внутренний метод).

        Работает в отдельном потоке и периодически проверяет дедлайны.

        Args:
            stop_event: событие для остановки планировщика
            interval_ms: интервал между проверками (мс, если не указан - из конфига)
        """
        if interval_ms is None:
            interval_ms = self.scheduler_config["interval_ms"]

        while not stop_event.is_set():
            try:
                self.scheduler_tick()
                time.sleep(interval_ms / 1000.0)
            except Exception as e:
                logger.debug(f"[REDIS_CLIENT] ❌ Ошибка в планировщике: {e}")
                time.sleep(1)

        logger.debug("[REDIS_CLIENT] 🛑 Планировщик остановлен")

    # ==================== УПРАВЛЕНИЕ ПОТОКАМИ ====================

    def start_worker(self, chat_id: str) -> None:
        """
        Запустить воркер для обработки сообщений чата.

        Создаёт отдельный поток для чтения и обработки сообщений конкретного чата.
        Если воркер уже запущен - ничего не делает.

        Args:
            chat_id: ID чата
        """
        if chat_id in self._stop_events:
            logger.debug(f"[REDIS_CLIENT] ⚠️ Чат уже запущен в воркере --> {chat_id}")
            return

        stop_event = threading.Event()
        self._stop_events[chat_id] = stop_event

        thread = threading.Thread(
            target=self._chat_worker,
            args=(chat_id, stop_event),
            daemon=True,
            name=f"worker_{chat_id}",
        )
        thread.start()
        self._threads.append(thread)

        logger.debug(f"[REDIS_CLIENT] Чат активен --> {chat_id}")

    def stop_worker(self, chat_id: str) -> None:
        """
        Остановить воркер для конкретного чата.

        Args:
            chat_id: ID чата
        """
        if chat_id not in self._stop_events:
            logger.debug(f"[REDIS_CLIENT] ⚠️ Воркер для чата {chat_id} не запущен")
            return

        self._stop_events[chat_id].set()

        del self._stop_events[chat_id]

        if chat_id in self._flush_locks:
            del self._flush_locks[chat_id]

        logger.debug(f"[REDIS_CLIENT] 🛑 Воркер остановлен для чата {chat_id}")

    def start_scheduler(self, interval_ms: int | None = None) -> None:
        """
        Запустить глобальный планировщик.

        Создаёт отдельный поток для периодической проверки дедлайнов.

        Args:
            interval_ms: интервал между проверками (мс, если не указан - из конфига)
        """
        if "scheduler" in self._stop_events:
            logger.debug("[REDIS_CLIENT] ⚠️ Планировщик уже запущен")
            return

        stop_event = threading.Event()
        self._stop_events["scheduler"] = stop_event

        thread = threading.Thread(
            target=self._scheduler_loop,
            args=(stop_event, interval_ms),
            daemon=True,
            name="scheduler",
        )
        thread.start()
        self._threads.append(thread)

        logger.debug("[REDIS_CLIENT] ✅ Планировщик запущен")

    def stop_scheduler(self) -> None:
        """
        Остановить глобальный планировщик.
        """
        if "scheduler" not in self._stop_events:
            logger.debug("[REDIS_CLIENT] ⚠️ Планировщик не запущен")
            return

        self._stop_events["scheduler"].set()
        del self._stop_events["scheduler"]

        logger.debug("[REDIS_CLIENT] 🛑 Планировщик остановлен")

    def stop_all(self) -> None:
        """
        Остановить все воркеры и планировщик.

        Принудительно сбрасывает все активные серии перед остановкой.
        """
        logger.debug("[REDIS_CLIENT] 🛑 Остановка всех воркеров и планировщика...")

        self._flush_all()

        for stop_event in self._stop_events.values():
            stop_event.set()

        self._stop_events.clear()
        self._threads.clear()
        self._flush_locks.clear()

        logger.debug("[REDIS_CLIENT] ✅ Все воркеры и планировщик остановлены")

    # ==================== ЧТЕНИЕ ДАННЫХ ====================

    def get_final_messages(self, chat_id: str, count: int = 20) -> list[tuple]:
        """
        Получить финальные (агрегированные) сообщения чата.

        Возвращает список кортежей (message_id, fields) с финальными сообщениями,
        отсортированными по времени (новые первыми).

        Args:
            chat_id: ID чата
            count: Количество сообщений для получения

        Returns:
            list[tuple]: Список кортежей (message_id, fields)
        """
        return self.redis.xrevrange(self.final_stream_tpl.format(chat_id=chat_id), count=count)

    def get_raw_messages(self, chat_id: str, count: int = 20) -> list[tuple]:
        """
        Получить сырые (необработанные) сообщения чата.

        Args:
            chat_id: ID чата
            count: количество сообщений для получения

        Returns:
            Список кортежей (message_id, fields)
        """
        return self.redis.xrevrange(self.raw_stream_tpl.format(chat_id=chat_id), count=count)

    def get_chat_status(self, chat_id: str) -> dict[str, Any]:
        """
        Получить статус чата: активные серии, метрики, конфигурацию.

        Args:
            chat_id: ID чата

        Returns:
            dict: Словарь со статусом чата, включающий:
                - has_active_series: Есть ли активная серия
                - active_series: Данные активной серии
                - deadline_timestamp: Время дедлайна
                - metrics: Метрики чата
                - config: Конфигурация чата
                - worker_running: Запущен ли воркер
        """
        agg_key = self.agg_hash_tpl.format(chat_id=chat_id)
        active_series = self.redis.hgetall(agg_key)

        deadline_score = self.redis.zscore(self.sched_zset, chat_id)

        metrics = self.get_metrics(chat_id)

        config = self.get_config(chat_id)

        return {
            "chat_id": chat_id,
            "has_active_series": bool(active_series),
            "active_series": active_series,
            "deadline_timestamp": deadline_score,
            "deadline_seconds_left": max(0, deadline_score - time.time()) if deadline_score else None,
            "metrics": metrics,
            "config": config,
            "worker_running": chat_id in self._stop_events,
        }

    def get_all_chats_status(self) -> dict[str, dict[str, Any]]:
        """
        Получить статус всех чатов.

        Returns:
            Словарь {chat_id: status} для всех чатов
        """
        all_chats = self.redis.zrange(self.sched_zset, 0, -1)

        for chat_id in self._stop_events.keys():
            if chat_id != "scheduler" and chat_id not in all_chats:
                all_chats.append(chat_id)

        result = {}
        for chat_id in all_chats:
            result[chat_id] = self.get_chat_status(chat_id)

        return result

    def cleanup_chat(self, chat_id: str) -> None:
        """
        Очистить все данные чата (стримы, буферы, метрики).

        ⚠️ ВНИМАНИЕ: Это удалит ВСЕ данные чата безвозвратно!

        Args:
            chat_id: ID чата
        """
        if chat_id in self._stop_events:
            self.stop_worker(chat_id)

        self.foREDIS_CLIENTe_flush_chat(chat_id)

        keys_to_delete = [
            self.raw_stream_tpl.format(chat_id=chat_id),
            self.final_stream_tpl.format(chat_id=chat_id),
            self.agg_hash_tpl.format(chat_id=chat_id),
            self.conf_hash_tpl.format(chat_id=chat_id),
            self.metrics_hash_tpl.format(chat_id=chat_id),
        ]

        for key in keys_to_delete:
            self.redis.delete(key)

        self.redis.zrem(self.sched_zset, chat_id)

        logger.debug(f"[REDIS_CLIENT] 🧹 Чат {chat_id} полностью очищен")

    def get_system_config(self) -> dict[str, Any]:
        """
        Получить системную конфигурацию.

        Returns:
            Словарь с системной конфигурацией
        """
        return {
            "redis": self.config["redis"],
            "keys": self.config["keys"],
            "aggregation": self.config["aggregation"],
            "workers": self.config["workers"],
            "scheduler": self.config["scheduler"],
        }

    def get_system_info(self) -> dict[str, Any]:
        """
        Получить общую информацию о системе.

        Returns:
            Словарь с информацией о системе
        """
        redis_info = self.redis.info()

        active_chats = len(self.redis.zrange(self.sched_zset, 0, -1))

        running_workers = len([k for k in self._stop_events.keys() if k != "scheduler"])

        return {
            "redis_version": redis_info.get("redis_version"),
            "connected_clients": redis_info.get("connected_clients"),
            "used_memory": redis_info.get("used_memory_human"),
            "active_chats": active_chats,
            "running_workers": running_workers,
            "scheduler_running": "scheduler" in self._stop_events,
            "total_threads": len(self._threads),
        }
