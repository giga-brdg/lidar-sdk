"""FastAPI/ASGI-обёртка: каждый HTTP-запрос → событие kind="web".

Сырое ASGI-middleware (не BaseHTTPMiddleware) — чтобы:
1) видеть lifespan-сообщения и чисто поднять/остановить флэшер (не зависим от того,
   использует продукт `lifespan=` или `on_event` — в первом случае startup-хуки FastAPI молчат);
2) не буферизовать тело ответа.

Захват: метод, путь, статус, длительность (мс), ip клиента. Пути из skip (точные или префиксы)
пропускаем без события — туда кладут служебное (healthz) и machine-to-machine эндпоинты приёмников,
иначе на самом lidar получился бы цикл (POST в /ingest → web-событие → POST в /ingest → …).
"""

from __future__ import annotations

import time


# что пропускаем по умолчанию у любого продукта (служебка)
_DEFAULT_SKIP = {"/healthz", "/favicon.ico", "/metrics"}


class LidarASGIMiddleware:
    def __init__(self, app, client, skip_paths=None, skip_prefixes=None) -> None:
        self.app = app
        self.client = client
        self.skip_paths = _DEFAULT_SKIP | set(skip_paths or ())
        self.skip_prefixes = tuple(skip_prefixes or ())

    def _skip(self, path: str) -> bool:
        if path in self.skip_paths:
            return True
        return bool(self.skip_prefixes) and path.startswith(self.skip_prefixes)

    async def __call__(self, scope, receive, send):
        stype = scope.get("type")

        # lifespan: поднимаем флэшер после старта, гасим перед остановкой
        if stype == "lifespan":
            async def send_wrapper(message):
                mtype = message.get("type")
                # гасим флэшер перед любым исходом остановки (complete или failed),
                # иначе таск+httpx-клиент утекут и финальная пачка потеряется
                if mtype in ("lifespan.shutdown.complete", "lifespan.shutdown.failed"):
                    try:
                        await self.client.aclose()
                    except Exception:  # noqa: BLE001
                        pass
                await send(message)
                if mtype == "lifespan.startup.complete":
                    try:
                        self.client.ensure_started()
                    except Exception:  # noqa: BLE001
                        pass
            await self.app(scope, receive, send_wrapper)
            return

        if stype != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if self._skip(path):
            await self.app(scope, receive, send)
            return

        try:
            self.client.ensure_started()
        except Exception:  # noqa: BLE001
            pass

        start = time.monotonic()
        holder = {"status": None}

        async def send_capture(message):
            if message.get("type") == "http.response.start":
                holder["status"] = message.get("status")
            await send(message)

        try:
            await self.app(scope, receive, send_capture)
        finally:
            try:
                dur_ms = round((time.monotonic() - start) * 1000, 1)
                client_addr = scope.get("client")
                ip = client_addr[0] if isinstance(client_addr, (list, tuple)) and client_addr else None
                self.client.capture_event(
                    "web",
                    data={
                        "method": scope.get("method"),
                        "path": path,
                        "status": holder["status"],
                        "duration_ms": dur_ms,
                    },
                    ip=ip,
                )
            except Exception:  # noqa: BLE001 — телеметрия не валит ответ
                pass


def instrument_fastapi(app, client, skip_paths=None, skip_prefixes=None):
    """Обернуть FastAPI/ASGI-приложение middleware'ом lidar. Возвращает app (для чейнинга)."""
    app.add_middleware(
        LidarASGIMiddleware,
        client=client,
        skip_paths=skip_paths,
        skip_prefixes=skip_prefixes,
    )
    return app
