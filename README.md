# lidar-sdk

Телеметрия продуктов **giga-brdg** → **lidar**. Тихо собирает у продукта web-запросы (FastAPI),
апдейты Telegram, AI-вызовы и SQL, батчит в памяти и шлёт пачками в приёмники lidar.

**Принцип fail-open:** любой сбой SDK / сети / lidar **не влияет** на продукт. Нет конфига
(URL/токен) → SDK тихо выключается (no-op). Стандартный пункт ТЗ каждого проекта (gCLAUDE.md §8).

## Установка

Репо **публичный** — токен сборки не нужен. В `requirements.txt` продукта пин по тегу
(обновление SDK не ломает продукт внезапно):

```
lidar-sdk @ git+https://github.com/giga-brdg/lidar-sdk.git@v0.2.0
```

## Конфигурация (env продукта)

**Минимум — одна переменная:** `LIDAR_INGEST_TOKEN`. Остальное имеет умолчания.

| env | смысл |
|---|---|
| `LIDAR_INGEST_TOKEN` | **(обязательна)** секрет приёмников (= `INGEST_SECRET` на lidar-web); шлётся в `X-Lidar-Token`. Нет токена → SDK no-op |
| `LIDAR_INGEST_URL` | адрес lidar-web; по умолчанию зашит (railway-домен), переопределять не нужно |
| `LIDAR_PROJECT` | имя источника (`source`); по умолчанию = имя Railway-сервиса |
| `LIDAR_ENABLED` | `0/false` → no-op (по умолчанию вкл) |
| `LIDAR_BATCH_SIZE` / `LIDAR_FLUSH_INTERVAL` / `LIDAR_QUEUE_MAX` / `LIDAR_TIMEOUT` | тюнинг батчера |

## Встройка

```python
import lidar_sdk as lidar

lidar.instrument_fastapi(app)                 # web-события (kind="web")
lidar.instrument_telegram(application)         # telegram-события (kind="telegram")
lidar.instrument_sqlalchemy(engine)            # SQL → raw_sql_log

async with lidar.track_ai("claude-opus-4-8") as call:   # AI-вызовы (kind="ai_call")
    resp = await anthropic.messages.create(...)
    call.set_usage(resp.usage)

lidar.event("custom_kind", data={...})         # произвольное событие вручную
```

`instrument_fastapi` сам поднимает/гасит фоновый флэшер через lifespan ASGI. Для сервиса без
FastAPI (worker) — `lidar.start()` на старте и `await lidar.aclose()` на остановке.

### Важно про SQL на самом lidar

Инструментировать SQL **нельзя у приёмника телеметрии** (lidar): запись в `raw_sql_log`/`events`
сама породит SQL → новый захват → лавина. На lidar SQL не включаем; на обычных продуктах их БД к
ingest-таблицам lidar отношения не имеет — цикла нет. Доп. защита — `skip_substrings=[...]`.

Контракт приёмников (заполняет SDK):
- `POST /ingest/events` — `{"source", "events":[{"kind","ts","person_token","ip","data"}]}`
- `POST /ingest/sql` — `{"source", "entries":[{"statement","duration_ms","rows","ts"}]}`
- заголовок `X-Lidar-Token: <LIDAR_INGEST_TOKEN>`.
