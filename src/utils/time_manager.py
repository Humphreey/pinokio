from datetime import datetime, time
from typing import Dict, Any
from loguru import logger

def should_process_message_by_time(message_time: str, chat_config: Dict[str, Any]) -> bool:
    """
    Проверяет, должно ли сообщение обрабатываться по времени.

    Анализирует время сообщения относительно настроек чата:
    - Проверяет, включен ли чат (enabled)
    - Проверяет, попадает ли время в рабочие часы (start_time, end_time)
    - Проверяет, является ли день рабочим (days)

    Args:
        message_time: Время сообщения в формате ISO (например, "2025-01-15 10:30:00.000000")
        chat_config: Конфигурация чата с настройками времени работы

    Returns:
        bool: True если сообщение должно обрабатываться, False если заблокировано
    """
    
    if not chat_config.get('enabled', True):
        logger.warning("[TIME_MANAGER] Сообщение заблокировано: enabled=False")
        return False
    
    msg_dt = datetime.fromisoformat(message_time.replace(' ', 'T'))
    msg_time_utc = msg_dt.utctimetuple()
    msg_time = time(msg_time_utc.tm_hour, msg_time_utc.tm_min, msg_time_utc.tm_sec)

    if 'start_time' in chat_config and 'end_time' in chat_config:
        start = time.fromisoformat(chat_config['start_time'])
        end = time.fromisoformat(chat_config['end_time'])
        if not (start <= msg_time <= end):
            return False
    
    day_names = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    current_day = day_names[msg_dt.weekday()]
    
    if 'days' in chat_config:
        if current_day not in chat_config['days']:
            return False
    return True
