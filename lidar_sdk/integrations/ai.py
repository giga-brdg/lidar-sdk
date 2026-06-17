"""AI-обёртка: вызов LLM → событие kind="ai_call" (модель, длительность, ok, токены).

Использование (минимально инвазивно у продукта):

    async with track_ai("claude-opus-4-8", client=lidar.default_client) as call:
        resp = await anthropic.messages.create(...)
        call.set_usage(resp.usage)        # необязательно — добор токенов

Контекст-менеджер сам мерит время и фиксирует успех/исключение. Токены вытягиваем из
объекта usage Anthropic (input_tokens/output_tokens) защитно. Никогда не глотает исключение
самого вызова — только оборачивает его телеметрией (re-raise).
"""

from __future__ import annotations

import contextlib
import time


class _AiCall:
    def __init__(self, model: str) -> None:
        self.model = model
        self.input_tokens: int | None = None
        self.output_tokens: int | None = None
        self.extra: dict = {}

    def set_usage(self, usage) -> None:
        """Принять объект usage (Anthropic) или dict; вытянуть токены защитно."""
        try:
            if usage is None:
                return
            self.input_tokens = getattr(usage, "input_tokens", None)
            self.output_tokens = getattr(usage, "output_tokens", None)
            if self.input_tokens is None and isinstance(usage, dict):
                self.input_tokens = usage.get("input_tokens")
                self.output_tokens = usage.get("output_tokens")
        except Exception:  # noqa: BLE001
            pass

    def set(self, **kwargs) -> None:
        """Добавить произвольные атрибуты в data события."""
        self.extra.update(kwargs)


@contextlib.asynccontextmanager
async def track_ai(model: str, client, **attrs):
    call = _AiCall(model)
    start = time.monotonic()
    ok = True
    try:
        yield call
    except Exception:
        ok = False
        raise  # вызов продукта важнее телеметрии — пробрасываем
    finally:
        try:
            # пользовательские attrs/extra кладём ПЕРВЫМИ, затем служебные поля —
            # чтобы коллизия ключа не затёрла model/duration_ms/ok
            data: dict = {}
            data.update(attrs)
            data.update(call.extra)
            data["model"] = model
            data["duration_ms"] = round((time.monotonic() - start) * 1000, 1)
            data["ok"] = ok
            if call.input_tokens is not None:
                data["input_tokens"] = call.input_tokens
            if call.output_tokens is not None:
                data["output_tokens"] = call.output_tokens
            client.capture_event("ai_call", data=data)
        except Exception:  # noqa: BLE001
            pass
