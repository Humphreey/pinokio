"""

Glossary:
    ERP System - external CRM ticket system
    AMBK - AI Messengers Bot Kernel (core of the bot)
    ETM - ERP Ticket Manager (microservice of the bot)
    ETL - ERP Ticket Listener (microservice of the bot)

Pydantic best practices for optional and Optional fields:
    my_int: int = Field(default=10, description="Type = int, default 10")
    my_list: List[str] = Field(default_factory=list, description="Type = list, default = []")    
    maybe_list: Optional[List[str]] = Field(default=None, description="Type = list|None, default = None")
Notes: 
1) specifying Optional in a type hint does NOT make the field optional for Pydantic.
Example: my_int: Optional[int] = Field(..., description="Blah-blah") -> error if my_int is omitted in .ctor
2) there are two meanings of "optional" word, not to be confused:
    - the parameter may be omitted
    - the parameter may be None
"""

from typing import Dict, List, Optional, Literal
from pydantic import BaseModel, Field


class AMBK_MediaModel(BaseModel):
    """AMBK - AI Messengers Bot Kernel (core of the bot)"""
    media_id: str = Field(..., description="File name in S3 bucket, may be uuid")
    media_type: Optional[Literal['photo', 'audio', 'video', 'document', 'sticker', 'voice']] = Field(
        None, description="Тип медиа (фото, аудио, видео, документ, стикер, голосовое сообщение)")
    media_filename: Optional[str] = Field(default=None, description="Название присланного файла (например: 'Скриншот1.png')")
    # mime_type: Optional[str] = Field(default=None, description="Файл (MIME тип).")


# ===== POST: «Входящее сообщение» =====
class IncomingFromMsRequest(BaseModel):
    """
    MS = Message Service
    """

    messages__id: str = Field(..., description="Ключ, идентификатор сообщения, uuid (pk)")
    messages__parent_message_id: Optional[str] = Field(default=None, description="ID сообщения, на которое ответили, uuid")
    messages__media_type: Optional[Literal['photo', 'audio', 'video', 'document', 'sticker', 'voice']] = Field(
        None, description="Тип медиа (фото, аудио, видео, документ, стикер, голосовое сообщение)"
    )
    messages__user_id: str = Field(..., description="ID пользователя (FK на users.id). UUID.")
    messages__username: Optional[str] = Field(default=None, description="TG username (например @vpupkin). Может отсутствовать в редких"
                                              "случаях (если пользователь его скрыл и т.п.")
    messages__media_id: Optional[str] = Field(default=None, description="File name in S3 bucket, may be uuid")
    messages__media_filename: Optional[str] = Field(default=None, description="Название присланного файла (например: 'Скриншот1.png')")
    # messages__date: datetime = Field(..., description="Время создания сообщения UTC (timestamp), '2025-08-12 11:25:39.365821'")
    messages__date: str = Field(..., description="Время создания сообщения UTC (timestamp), '2025-08-12 11:25:39.365821'")
    messages__group_id: Optional[str] = Field(default=None, description="ID группы сообщений")
    text_histories__text: Optional[str] = Field(default=None, description="Текст сообщения. Может быть None например если шлют картинку.")
    text_histories__id: str = Field(..., description="Ключ, идентификатор записи истории текста, uuid")

    messages__status: Optional[str] = Field(default=None, description="Статус сообщения")

    # TBD-2025-09-29: 
    #text_histories__change_text: Optional[str] = Field(default=None, description="ID сообщения, которое было изменено")
    text_histories__change_id: Optional[str] = Field(default=None, description="ID сообщения, которое было изменено")

    entities__custom_emoji_id: Optional[str] = Field(default=None, description="ID эмодзи")
    
    messages__chat_id: str = Field(..., description="Внешний ключ для связи с chats")
    thread_id: Optional[str] = Field(default=None, description="ID цепочки сообщений, определяется на стороне ИИ")

    chats__client_type: Optional[str] = Field(default=None, description="Тип пользовательского чата (Мерчант, Трейдер). Используется как erp_customer_id")
    chats__client_id: Optional[str] = Field(default=None, description="ID клиента/мерчанта/трейдера. Используется как erp_customer_id")

    users__in_house: Optional[bool] = Field(default=None, description="Признак свой/чужой для user_id")
    users__is_bot: Optional[bool] = Field(default=None, description="Признак, обозначающий бота")

    # Internal AMBK fields (absent in original message, filled AFTER msg receiving)
    ambk__media_group: Optional[List[AMBK_MediaModel]] = Field(default=None, description="Накопленный буфером список файлов для обработки (внутреннее поле AMBK)")
    ambk__username: Optional[str] = Field(default=None, description="Имя юзера в ТГ (@)")
    ambk__buffer_start_monotonic_dt: Optional[float] = Field(default=None, description="Время начала накопительного буфера"
                                                             " через time.monotonic(); необходимо для таймеров.")


# ===== POST: «Исходящее сообщение» =====
class OutgoingToMsResponse(BaseModel):
    """
    MS = Message Service
    """
    chats__id: str = Field(..., description="Ключ для связи с chats")

    thread_id: Optional[str] = Field(default=None, description="ID цепочки сообщений, определяется на стороне ИИ")

    text_histories__text: Optional[str] = Field(default=None, description="Текст сообщения, которое отправляет ИИ. Может быть None например если надо проставить эмоцию.")

    messages__id: Optional[str] = Field(default=None, description="ID сообщения, на которое нужно ответить или отправить эмодзи. Может быть None (новое сообщение).")

    users__id: str = Field(..., description="ID аккаунта, от чьего имени надо отправить сообщение")

    ts_tickets__id: Optional[str] = Field(default=None, description="ID тикета (из ERP)")

    invoice_id: Optional[str] = Field(default=None, description="Номер инвойса")

    entities__custom_emoji_id: Optional[str] = Field(default=None, description="ID эмодзи")



# ===== GET метод API «Запрос дополнительных данных» из БД Сервиса сообщений.  =====
class AdditionalDataToMsRequest(BaseModel):
    """
    MS = Message Service
    Модель для запроса доп. данных из MS.
    """
    messages__id: Optional[str] = Field(
        default=None,
        description="Ключ, идентификатор сообщения (PK)"
    )
    messages__user_id: Optional[str] = Field(
        default=None,
        description="ID автора сообщения (bigint unsigned)"
    )
    chat_id: Optional[str] = Field(
        default=None,
        description="ID чата"
    )
    topic_id: Optional[str] = Field(
        default=None,
        description="ID топика"
    )
    ts_tickets__id: Optional[str] = Field(
        default=None,
        description="Id тикета. В ответе отправляются все сообщения, связанные с тикетом"
    )
    invoice_id: Optional[str] = Field(
        default=None,
        description="Номер инвойса. В ответе отправляются все сообщения, связанные с инвойсом"
    )
    last_n: Optional[int] = Field(
        default=None,
        description="N последних сообщений по дате сообщений UTC"
    )
    # period_start: Optional[datetime] = Field(
    #     default=None,
    #     description="Начальная дата запрашиваемого периода, UTC (пример: '2025-08-12 11:25:39.365821')"
    # )
    # period_end: Optional[datetime] = Field(
    #     default=None,
    #     description="Конечная дата запрашиваемого периода, UTC (пример: '2025-08-12 11:25:39.365821')"
    # )
    period_start: Optional[str] = Field(
        default=None,
        description="Начальная дата запрашиваемого периода, UTC (пример: '2025-08-12 11:25:39.365821')"
    )
    period_end: Optional[str] = Field(
        default=None,
        description="Конечная дата запрашиваемого периода, UTC (пример: '2025-08-12 11:25:39.365821')"
    )


class GetConfigToMsRequest(BaseModel):
    """
    MS = Message Service
    Модель для запроса доп. данных из MS.
    """
    ...


class ConfigFromMsResponse(BaseModel):
    """
    MS = Message Service
    Модель для запроса доп. данных из MS.
    """
    erp_queue_id: Optional[str] = Field(
        default=None,
        description="Queue id from ERP"
    )
    erp_topic_id: Optional[str] = Field(
        default=None,
        description="Topic id from ERP"
    )
    bot_user_id: Optional[str] = Field(
        default=None,
        description="Bot id for MS"
    )


class KafkaSenderResponse(BaseModel):
    topic: str = Field(..., description="Топик")
    message_id: Optional[str] = Field(default=None, description="ID сообщения. None, если мы отправили в Кафку сообщение без reply")
    partition: Optional[int] = Field(default=None, description="Партиция")
    offset: Optional[int] = Field(default=None, description="Позиция в потоке сообщений")


# NEW 2025-10-19: models for accounts, etc
class MS_AccountInfoModel(BaseModel):
    """MS = Message Service; This is a model for account information of a user"""
    username: str = Field(..., description="Username in TG, etc. Ex: '@vpupkin'")
    name: str = Field(..., description="Name of user in TG, etc; not yet used. Ex: 'Vasya Pupkin'")
    inhouse: bool = Field(..., description="True: user is from our company; False: some external user (merchant, trader, etc.)")

class MS_ChatCfgModel(BaseModel):
    """MS = Message Service; contains dict of user accounts info and some other data"""
    chat_name: str = Field(..., description="Name of the chat; Ex: 'Libertex Chat'")
    invoice_for_payment__url_prefix: str = Field(..., description="Prefix for payment invoice URL; Ex: 'https://servicedesk.test.hgtp12tests.com/payment-invoice/'")
    invoice_for_withdrawal__url_prefix: str = Field(..., description="Prefix for withdrawal invoice URL; Ex: 'https://servicedesk.test.hgtp12tests.com/withdrawal/'")

    accounts: Dict[str, MS_AccountInfoModel] = Field(default_factory=dict, description="Mapping of account UUID to MS_AccountInfoModel;")
    inhouse_usernames: List[str] = Field(default_factory=list, description="List of inhouse usernames, for second-priority checking; Ex: ['@vpupkin', '@musk']")

    def get_account_info_for_user_or_none(self, user_id: str) -> Optional[MS_AccountInfoModel]:
        """Get account by user id key, returns None if not found"""
        return self.accounts.get(user_id)    
