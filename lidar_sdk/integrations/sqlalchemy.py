"""SQLAlchemy-обёртка: каждый SQL-запрос продукта → raw_sql_log (kind через /ingest/sql).

Вешаем слушатели before/after_cursor_execute на engine. Работает и с async-движком
(берём его .sync_engine). Захватываем текст запроса (усечён), длительность, rowcount.

ВНИМАНИЕ (цикл на самом lidar): инструментировать БД нельзя у приёмника телеметрии — запись
в raw_sql_log/events сама породит новый SQL → новый захват → лавина. Поэтому на lidar SQL НЕ
инструментируем; на обычных продуктах (city и пр.) их БД к ingest-таблицам lidar отношения не
имеет — цикла нет. Доп. защита: skip_substrings гасит запросы по подстроке (имена ingest-таблиц).
"""

from __future__ import annotations

import time


def instrument_sqlalchemy(engine, client, max_len: int = 4000, skip_substrings=None):
    """Повесить слушатели SQL на engine (sync или async). Возвращает engine."""
    from sqlalchemy import event

    target = getattr(engine, "sync_engine", engine)  # async-движок → его sync-ядро
    skip = tuple(s.lower() for s in (skip_substrings or ()))

    @event.listens_for(target, "before_cursor_execute")
    def _before(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        try:
            context._lidar_t0 = time.monotonic()
        except Exception:  # noqa: BLE001
            pass

    @event.listens_for(target, "after_cursor_execute")
    def _after(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        try:
            if skip and isinstance(statement, str):
                low = statement.lower()
                if any(s in low for s in skip):
                    return
            t0 = getattr(context, "_lidar_t0", None)
            dur_ms = round((time.monotonic() - t0) * 1000, 2) if t0 is not None else None
            rc = getattr(cursor, "rowcount", None)
            rows = rc if isinstance(rc, int) and rc >= 0 else None
            client.capture_sql(str(statement)[:max_len], duration_ms=dur_ms, rows=rows)
        except Exception:  # noqa: BLE001 — телеметрия не валит запрос
            pass

    return engine
