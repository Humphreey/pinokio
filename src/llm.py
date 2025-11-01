import json
import re
from typing import Any

import openai
import yaml
from config import settings


class LLM:
    """Классификатор текста на основе OpenAI API."""

    def __init__(self, config_path: str = "configs/prompts.yaml"):
        """
        Инициализация классификатора.

        Загружает конфигурацию промптов и настраивает OpenAI клиент
        для работы с ИИ моделями.

        Args:
            config_path: Путь к файлу конфигурации промптов (по умолчанию "configs/prompts.yaml")
        """
        self._load_config(config_path)
        self._setup_client()

    def _load_config(self, config_path: str) -> None:
        """
        Загружает конфигурацию из YAML файла.
        
        Args:
            config_path: Путь к файлу конфигурации
        """
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        self.api_key = settings.LLM_API_KEY
        self.url = settings.LLM_URL
        self.model = settings.LLM_MODEL

        self.schema = config["classification_schema"]
        self.system_prompt = config["system_prompt"]
        self.qa_link_system_prompt: str | None = config.get("qa_link_system_prompt")
        self.qa_link_schema: dict[str, Any] | None = config.get("qa_link_schema")

    def _setup_client(self) -> None:
        """
        Настраивает OpenAI клиент.
        
        Инициализирует клиент с настройками из конфигурации.
        """
        self.client = openai.OpenAI(base_url=self.url, api_key=self.api_key)
        self.model = self.model

    def classify_text(self, text: str) -> dict[str, Any]:
        """
        Классифицирует текст на предмет необходимости ответа.

        Использует ИИ для определения, требует ли сообщение ответа от оператора.
        Анализирует текст по различным критериям: длина, наличие вопросов,
        упоминаний, ссылок, телефонов и временных ссылок.

        Args:
            text: Текст сообщения для классификации

        Returns:
            dict: Результат классификации с ключами:
                - class: 0 (не требует ответа) или 1 (требует ответа)
                - confidence: Уверенность модели (0.0-1.0)
        """
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": f"Классифицируй следующий текст:\n\n{text}"},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": "classification", "schema": self.schema},
            },
            temperature=0.0,
        )

        raw_content = response.choices[0].message.content

        json_match = re.search(r"\{.*\}", raw_content, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            result = json.loads(json_str)
        else:
            result = json.loads(raw_content)

        return result

    def match_answer_to_question(self, messages: list[dict[str, Any]], answer: str) -> dict[str, Any]:
        """
        Определяет, к какому вопросу относится ответ.

        Использует ИИ для сопоставления ответа оператора с вопросом клиента.
        Анализирует языковую совместимость, лексическое и семантическое сходство,
        временную близость и контекстные связи.

        Args:
            messages: Список сообщений от клиентов с полями:
                - redis_stream_id: ID сообщения в Redis
                - username: Имя пользователя
                - text: Текст сообщения
            answer: Текст ответа оператора

        Returns:
            dict: Результат сопоставления с ключом:
                - matched_message_id: ID сопоставленного сообщения или None
        """

        system_msg = self.qa_link_system_prompt

        candidates_str = "\n".join([f"{m.get('redis_stream_id')}: merchant: {m.get('text')}" for m in messages])
        answer_str = f"PP: {answer}"

        user_msg = "Candidates:\n" f"{candidates_str}\n\n" "Answer:\n" f"{answer_str}\n\n" "Return strict JSON only."

        max_attempts = 3
        last_parsed: dict[str, Any] | None = None
        last_parsed = {"matched_message_id": None}
        for _ in range(max_attempts):
            if _ > 0:
                user_msg = f"Last attempt failed. Try again:\n\n{user_msg}"
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {"name": "qa_link", "schema": self.qa_link_schema},
                },
                temperature=0.0,
            )

            raw_content = response.choices[0].message.content
            parsed_any = self._parse_llm_json(raw_content)
            if isinstance(parsed_any, dict):
                matched = parsed_any.get("matched_message_id")
                if matched is None or isinstance(matched, int):
                    return parsed_any
                # если тип неверный — попробуем ещё раз
                last_parsed = {**parsed_any, "matched_message_id": None}

        # если все попытки неудачны
        return last_parsed

    @staticmethod
    def _parse_llm_json(raw_content: str | None) -> Any | None:
        """
        Парсит JSON ответ от LLM, обрабатывая различные форматы.

        Обрабатывает ответы в различных форматах:
        - Обычный JSON
        - JSON в markdown блоках (```json ... ```)
        - Специальные значения (null, none)

        Args:
            raw_content: Сырой ответ от LLM

        Returns:
            Any | None: Распарсенный JSON или None при ошибке
        """
        if raw_content is None:
            return None

        content = raw_content.strip()
        if not content:
            return None

        fenced = re.match(r"^```[a-zA-Z]*\s*([\s\S]*?)\s*```$", content, flags=re.IGNORECASE)
        if fenced:
            content = fenced.group(1).strip()

        content = content.strip("`").strip()

        if content.lower() in {"null", "none"}:
            return None

        try:
            return json.loads(content)
        except Exception:
            return None
