"""lidar-SDK — телеметрия продуктов giga-brdg → lidar.

Что делает: тихо собирает у продукта web-запросы (FastAPI), апдейты Telegram, AI-вызовы и SQL,
батчит и шлёт в приёмники lidar (/ingest/events, /ingest/sql). Принцип fail-open: любой сбой
SDK/сети/lidar не влияет на продукт. Конфиг — из env (см. config.py); нет URL/токена → no-op.

Минимальная встройка у продукта:

    import lidar_sdk as lidar

    lidar.instrument_fastapi(app)              # web-события
    lidar.instrument_telegram(application)      # telegram-события
    lidar.instrument_sqlalchemy(engine)         # SQL  (НЕ на самом lidar — см. integrations/sqlalchemy)

    async with lidar.track_ai("claude-opus-4-8") as call:   # AI-вызовы
        resp = await anthropic.messages.create(...)
        call.set_usage(resp.usage)

Версия пакета пинится тегом в продукте (requirements: lidar-sdk @ git+…@vX.Y.Z).
"""

from __future__ import annotations

from .client import LidarClient
from .config import LidarConfig

__version__ = "0.2.0"

# дефолтный клиент, сконфигурированный из окружения продукта
default_client = LidarClient(LidarConfig.from_env())


# --- ручной захват ---------------------------------------------------------

def event(kind, data=None, person_token=None, ip=None, ts=None) -> None:
    default_client.capture_event(kind, data=data, person_token=person_token, ip=ip, ts=ts)


def sql(statement, duration_ms=None, rows=None, ts=None) -> None:
    default_client.capture_sql(statement, duration_ms=duration_ms, rows=rows, ts=ts)


def stats() -> dict:
    return default_client.stats()


async def aclose() -> None:
    await default_client.aclose()


def start() -> None:
    """Поднять флэшер вручную (для worker без FastAPI; web/бот стартуют его сами)."""
    default_client.ensure_started()


# --- интеграции (ленивые импорты — нет жёсткой зависимости от fastapi/ptb/…) -

def instrument_fastapi(app, skip_paths=None, skip_prefixes=None, client=None):
    from .integrations.fastapi import instrument_fastapi as _f
    return _f(app, client=client or default_client, skip_paths=skip_paths, skip_prefixes=skip_prefixes)


def instrument_telegram(application, client=None):
    from .integrations.telegram import instrument_telegram as _t
    return _t(application, client=client or default_client)


def instrument_sqlalchemy(engine, max_len=4000, skip_substrings=None, client=None):
    from .integrations.sqlalchemy import instrument_sqlalchemy as _s
    return _s(engine, client=client or default_client, max_len=max_len, skip_substrings=skip_substrings)


def track_ai(model, client=None, **attrs):
    from .integrations.ai import track_ai as _a
    return _a(model, client=client or default_client, **attrs)


__all__ = [
    "__version__",
    "LidarClient",
    "LidarConfig",
    "default_client",
    "event",
    "sql",
    "stats",
    "start",
    "aclose",
    "instrument_fastapi",
    "instrument_telegram",
    "instrument_sqlalchemy",
    "track_ai",
]
