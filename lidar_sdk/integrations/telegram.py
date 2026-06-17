"""Telegram-обёртка (python-telegram-bot): каждый апдейт → событие kind="telegram".

Неинвазивно: добавляем TypeHandler в group=-100. В PTB обработчики разных групп выполняются
все по очереди групп — наблюдатель в самой ранней группе видит КАЖДЫЙ апдейт и не мешает
рабочим хендлерам (group=0). Наблюдатель никогда не бросает (иначе залогируется как ошибка бота).

В data кладём тип апдейта, команду, tg_id/username/тип чата. person_token не ставим —
резолв человека по tg_id делает сам lidar (реестр people).
"""

from __future__ import annotations

# самая ранняя группа: наблюдатель отрабатывает раньше рабочих хендлеров и не блокирует их
_OBSERVE_GROUP = -100


def _describe(update) -> dict:
    data: dict = {}
    try:
        msg = getattr(update, "effective_message", None)
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)
        cq = getattr(update, "callback_query", None)

        if cq is not None:
            data["update_type"] = "callback_query"
            # для приватности — только префикс до ':' (имя действия), без полезной нагрузки
            cdata = getattr(cq, "data", None)
            if cdata:
                data["callback"] = str(cdata).split(":", 1)[0][:50]
        elif msg is not None:
            text = getattr(msg, "text", None) or ""
            if text.startswith("/"):
                data["update_type"] = "command"
                data["command"] = text.split()[0][:50]
            else:
                data["update_type"] = "message"
        else:
            data["update_type"] = "other"

        if user is not None:
            data["tg_user_id"] = getattr(user, "id", None)
            uname = getattr(user, "username", None)
            if uname:
                data["username"] = str(uname)[:100]
        if chat is not None:
            data["chat_type"] = getattr(chat, "type", None)
    except Exception:  # noqa: BLE001
        pass
    return data


def instrument_telegram(application, client):
    """Повесить наблюдатель апдейтов на PTB Application (одна строка у продукта)."""
    from telegram import Update
    from telegram.ext import TypeHandler

    async def _observe(update, context):
        try:
            client.capture_event("telegram", data=_describe(update))
        except Exception:  # noqa: BLE001 — наблюдатель не имеет права падать
            pass

    application.add_handler(TypeHandler(Update, _observe), group=_OBSERVE_GROUP)
    return application
