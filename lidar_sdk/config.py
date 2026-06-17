"""Конфигурация SDK из окружения продукта.

Все настройки — через env, чтобы продукт ничего не хардкодил:
- LIDAR_INGEST_URL    — базовый https-URL lidar-web (напр. https://lidar-production-….up.railway.app)
- LIDAR_INGEST_TOKEN  — секрет приёмников (= INGEST_SECRET на lidar-web); шлётся в заголовке X-Lidar-Token
- LIDAR_PROJECT       — имя продукта-источника (source), напр. "lidar", "city-payback-dashboard"
- LIDAR_ENABLED       — выключатель (0/false → SDK no-op); по умолчанию вкл, но без URL+TOKEN всё равно off
- LIDAR_BATCH_SIZE / LIDAR_FLUSH_INTERVAL / LIDAR_QUEUE_MAX / LIDAR_TIMEOUT — тюнинг батчера

Принцип: нет URL/токена → enabled=False → весь SDK тихо no-op (продукт без lidar просто работает).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Приёмник lidar режет пачку до 1000 записей — не шлём больше за раз.
_RECEIVER_CAP = 1000

# Адрес lidar-web по умолчанию (railway-домен с валидным TLS) — чтобы продукту не задавать
# LIDAR_INGEST_URL вручную. Переопределяется env LIDAR_INGEST_URL при необходимости.
_DEFAULT_URL = "https://lidar-production-c6f6.up.railway.app"


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in ("0", "false", "no", "off", "")


def _as_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value else default
    except (TypeError, ValueError):
        return default


def _as_float(value: str | None, default: float) -> float:
    try:
        return float(value) if value else default
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class LidarConfig:
    url: str | None
    token: str | None
    project: str
    enabled: bool
    batch_size: int
    flush_interval: float
    queue_max: int
    timeout: float

    @classmethod
    def from_env(cls) -> "LidarConfig":
        url = os.getenv("LIDAR_INGEST_URL") or _DEFAULT_URL
        token = os.getenv("LIDAR_INGEST_TOKEN")
        # имя источника: явное LIDAR_PROJECT, иначе имя Railway-сервиса, иначе unknown
        project = (
            os.getenv("LIDAR_PROJECT")
            or os.getenv("RAILWAY_SERVICE_NAME")
            or os.getenv("RAILWAY_PROJECT_NAME")
            or "unknown"
        )
        # без адреса и секрета SDK не может слать — выключаем (а не падаем)
        enabled = _as_bool(os.getenv("LIDAR_ENABLED"), True) and bool(url) and bool(token)
        queue_max = max(100, _as_int(os.getenv("LIDAR_QUEUE_MAX"), 10000))
        # batch не больше кэпа приёмника И не больше очереди (иначе ветка big не срабатывает)
        batch = min(_as_int(os.getenv("LIDAR_BATCH_SIZE"), 100), _RECEIVER_CAP, queue_max)
        return cls(
            url=url.rstrip("/") if url else None,
            token=token,
            project=str(project)[:100],
            enabled=enabled,
            batch_size=max(1, batch),
            flush_interval=max(0.5, _as_float(os.getenv("LIDAR_FLUSH_INTERVAL"), 5.0)),
            queue_max=queue_max,
            timeout=max(1.0, _as_float(os.getenv("LIDAR_TIMEOUT"), 10.0)),
        )
