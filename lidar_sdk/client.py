"""Клиент-батчер: копит события в памяти и шлёт пачками в lidar.

Главный принцип — fail-open: НИ один сбой телеметрии не должен влиять на продукт.
- capture_* никогда не блокирует и не бросает (всё в try; переполнение очереди — тихий дроп старого).
- фоновый флэшер раз в flush_interval (или при наборе batch_size) POST'ит пачку; ошибка сети →
  пачка дропается (не копим бесконечно, не ретраим до посинения), продукт не замечает.
- очереди — collections.deque(maxlen): append атомарен под GIL (можно звать и из sync-листенеров
  SQLAlchemy, и из async-кода), переполнение само вытесняет старейшее.

ts событий — ISO-8601 UTC (приёмник lidar разбирает их parse_dt).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone

import httpx

from .config import LidarConfig

_log = logging.getLogger("lidar_sdk")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LidarClient:
    def __init__(self, config: LidarConfig) -> None:
        self.cfg = config
        self._events: deque = deque(maxlen=config.queue_max)
        self._sql: deque = deque(maxlen=config.queue_max)
        self._task: asyncio.Task | None = None
        self._stop: asyncio.Event | None = None
        self._http: httpx.AsyncClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None  # петля-владелец task/event/http
        self._dropped = 0          # сколько пачек не доставлено (для диагностики)
        self._warned = False       # «один раз пожаловались» на устойчивую ошибку доставки

    # --- захват (никогда не бросает, никогда не блокирует) --------------------

    def capture_event(
        self,
        kind: str,
        data: dict | None = None,
        person_token: str | None = None,
        ip: str | None = None,
        ts: str | None = None,
    ) -> None:
        if not self.cfg.enabled:
            return
        try:
            e: dict = {"kind": str(kind)[:50], "ts": ts or _now_iso()}
            if person_token:
                e["person_token"] = str(person_token)[:255]
            if ip:
                e["ip"] = str(ip)[:64]
            if data is not None:
                e["data"] = data if isinstance(data, dict) else {"value": data}
            self._events.append(e)
            self._maybe_lazy_start()
        except Exception:  # noqa: BLE001 — телеметрия не имеет права падать
            pass

    def capture_sql(
        self,
        statement: str,
        duration_ms: float | None = None,
        rows: int | None = None,
        ts: str | None = None,
    ) -> None:
        if not self.cfg.enabled:
            return
        try:
            entry: dict = {"statement": str(statement)[:4000], "ts": ts or _now_iso()}
            if duration_ms is not None:
                entry["duration_ms"] = duration_ms
            if rows is not None:
                entry["rows"] = rows
            self._sql.append(entry)
            self._maybe_lazy_start()
        except Exception:  # noqa: BLE001
            pass

    # --- жизненный цикл флэшера ----------------------------------------------

    def _maybe_lazy_start(self) -> None:
        # стартуем фоновый флэшер, если уже крутится event loop (web/бот). В sync-контексте
        # без петли (напр. alembic) — тихо пропускаем: там телеметрию никто и не собирает.
        if self._task is not None and not self._task.done():
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self.ensure_started()

    def ensure_started(self) -> None:
        """Идемпотентно поднять фоновый флэшер в текущем event loop."""
        if not self.cfg.enabled:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        # сменился event loop (повторный asyncio.run / тестовые петли) — объекты, привязанные
        # к старой (мёртвой) петле, сбрасываем, чтобы не ждать её и не слать через её пул
        if self._loop is not None and self._loop is not loop:
            self._task = None
            self._stop = None
            self._http = None
            self._warned = False
        self._loop = loop
        if self._task is not None and not self._task.done():
            return
        self._stop = asyncio.Event()
        self._task = loop.create_task(self._run())

    async def aclose(self) -> None:
        """Остановить флэшер и дослать остаток (вызывать на shutdown продукта)."""
        if self._stop is not None:
            self._stop.set()
        if self._task is not None:
            try:
                await self._task
            except Exception:  # noqa: BLE001
                pass
            self._task = None
        await self._flush_all()  # финальный добор
        if self._http is not None:
            try:
                await self._http.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._http = None

    # --- внутреннее: цикл флэша и доставка ------------------------------------

    async def _run(self) -> None:
        last = time.monotonic()
        assert self._stop is not None
        while not self._stop.is_set():
            try:
                try:
                    # просыпаемся либо по таймеру (тик 0.5с), либо по сигналу остановки
                    await asyncio.wait_for(self._stop.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
                now = time.monotonic()
                due = (now - last) >= self.cfg.flush_interval
                big = (
                    len(self._events) >= self.cfg.batch_size
                    or len(self._sql) >= self.cfg.batch_size
                )
                if (due or big) and (self._events or self._sql):
                    await self._flush_all()
                    last = now
            except asyncio.CancelledError:
                raise  # отмена — наружу (корректная остановка)
            except Exception as exc:  # noqa: BLE001 — флэшер неубиваем (fail-open)
                _log.warning("lidar-sdk flush loop error: %s", type(exc).__name__)
                await asyncio.sleep(0.5)
        await self._flush_all()  # финальный добор при штатной остановке

    def _drain(self, buf: deque, n: int) -> list:
        out: list = []
        for _ in range(min(len(buf), n)):
            try:
                out.append(buf.popleft())
            except IndexError:
                break
        return out

    async def _flush_all(self) -> None:
        # дренируем до опустошения, но с потолком раундов — не зависаем во флэше навечно
        # при шторме событий; остаток доберём на следующем тике
        max_rounds = max(1, self.cfg.queue_max // self.cfg.batch_size + 1)
        for _ in range(max_rounds):
            events = self._drain(self._events, self.cfg.batch_size)
            if events:
                await self._safe_post(
                    "/ingest/events", {"source": self.cfg.project, "events": events}
                )
            sql = self._drain(self._sql, self.cfg.batch_size)
            if sql:
                await self._safe_post(
                    "/ingest/sql", {"source": self.cfg.project, "entries": sql}
                )
            if not events and not sql:
                break

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=self.cfg.timeout,
                follow_redirects=True,  # 3xx (http→https / trailing slash) не считаем доставкой молча
                headers={
                    "X-Lidar-Token": self.cfg.token or "",
                    "Content-Type": "application/json",
                },
            )
        return self._http

    async def _safe_post(self, path: str, payload: dict) -> None:
        """POST пачки; любая ошибка → дроп пачки (fail-open), без ретраев до посинения."""
        if not self.cfg.url:
            return
        try:
            # default=str: один несериализуемый объект в data не роняет всю пачку
            body = json.dumps(payload, default=str)
            resp = await self._client().post(self.cfg.url + path, content=body)
            if not (200 <= resp.status_code < 300):
                self._dropped += 1
                if not self._warned:
                    # один раз сообщаем о устойчивой проблеме (напр. 403 — не тот токен),
                    # дальше молчим, чтобы не засорять лог продукта. Секрет не печатаем.
                    self._warned = True
                    _log.warning("lidar-sdk ingest %s -> HTTP %s (dropping batch)", path, resp.status_code)
        except Exception as exc:  # noqa: BLE001 — сеть/таймаут/сериализация: дропаем пачку, продукт жив
            self._dropped += 1
            if not self._warned:
                self._warned = True
                _log.warning("lidar-sdk ingest %s failed: %s (dropping batch)", path, type(exc).__name__)

    # --- диагностика ----------------------------------------------------------

    def stats(self) -> dict:
        return {
            "enabled": self.cfg.enabled,
            "project": self.cfg.project,
            "queued_events": len(self._events),
            "queued_sql": len(self._sql),
            "dropped_batches": self._dropped,
        }
