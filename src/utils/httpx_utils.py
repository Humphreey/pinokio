"""
Wrappers for httpx.AsyncClient methods.
Important: this module should be synched between these places:
"""

import json
import ssl
from typing import Any
import httpx
from httpx._types import HeaderTypes


async def get_httpx_request(
    url: str, 
    headers: httpx._types.HeaderTypes | None = None,
    timeout: float = 30,
    error_max_len: int = 1000, 
    verify: ssl.SSLContext | str | bool = True
    ) -> httpx.Response:
    """
    Простая обертка для httpx.AsyncClient.get.

    Выполняет HTTP GET запрос с обработкой ошибок и логированием.

    Args:
        url: URL эндпоинта
        headers: Заголовки запроса (например, {"Authorization": f"Bearer {token}"})
        timeout: Таймаут в секундах (по умолчанию 30)
        error_max_len: Максимальная длина сообщения об ошибке (по умолчанию 1000)
        verify: Настройки SSL верификации

    Returns:
        httpx.Response: Ответ сервера

    Raises:
        RuntimeError: При ошибках запроса или HTTP статусах != 200
    """    
    async with httpx.AsyncClient(verify=verify) as client:
        try:
            response = await client.get(url=url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except httpx.RequestError as exc:
            raise RuntimeError(f"Request error for {url=}: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Error HTTP status={exc.response.status_code} for {url=}: {exc.response.text[:error_max_len]}; Request: N/A (get method)"
            ) from exc


async def post_httpx_request(
    url: str,
    data: Any | None = None,
    files: Any | None = None,  # NEW 2025-09: New parameter for file uploads
    headers: HeaderTypes | None = None,
    timeout: float = 30,
    error_max_len: int = 1000,
    verify: ssl.SSLContext | str | bool = True
) -> httpx.Response:
    """
    Простая обертка для httpx.AsyncClient.post.

    Выполняет HTTP POST запрос с обработкой ошибок и логированием.
    Поддерживает отправку JSON данных.

    Args:
        url: URL эндпоинта
        data: Данные для отправки (например, pydantic_model.model_dump())
        files: Файлы для загрузки (пока не реализовано)
        headers: Заголовки запроса (например, {"Authorization": f"Bearer {token}"})
        timeout: Таймаут в секундах (по умолчанию 30)
        error_max_len: Максимальная длина сообщения об ошибке (по умолчанию 1000)
        verify: Настройки SSL верификации

    Returns:
        httpx.Response: Ответ сервера

    Raises:
        RuntimeError: При ошибках запроса или HTTP статусах != 200
    """
    async with httpx.AsyncClient(verify=verify) as client:
        try:
            # TBD-2025-09-30: currently the files working is implemented in erp_manager via requests, this
            # approach could be an alternative
            # Send request (use `files` parameter for multipart uploads, `data` for regular json data)
            #response = await client.post(url=url, files=files, data=data, headers=headers, timeout=timeout)
            response = await client.post(url=url, json=data, headers=headers, timeout=timeout)
            # Check status
            response.raise_for_status()
            return response
        except httpx.RequestError as exc:
            raise RuntimeError(f"Request error for {url=}: {exc}; Request:\n{json.dumps(data, indent=2)}") from exc
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Error HTTP status={exc.response.status_code} for {url=}: {exc.response.text[:error_max_len]}; Request:\n{json.dumps(data, indent=2)}"
            ) from exc
