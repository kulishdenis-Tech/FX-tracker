import os
import asyncio
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import save_to_supabase

# === ЧАСОВА ЗОНА ===
os.environ["TZ"] = "Europe/Kyiv"
try:
    time.tzset()
except Exception:
    pass
TZ = ZoneInfo("Europe/Kyiv")

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

# === ENV ===
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
USER_SESSION = os.getenv("TG_USER_SESSION", "")
if not (API_ID and API_HASH and USER_SESSION):
    logging.error("❌ TG_API_ID / TG_API_HASH / TG_USER_SESSION відсутні")
    raise SystemExit(1)

# === КАНАЛИ ===
CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd",
}


# === ДОПОМІЖНІ ===
def local_time(dt):
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def build_block(channel, msg_id, ver, date, edited, text):
    """Формат блоку як у локальній версії"""
    return (
        "=" * 100 + "\n"
        + f"[CHANNEL] {channel}\n"
        + f"[MESSAGE_ID] {msg_id}\n"
        + f"[VERSION] v{ver}\n"
        + f"[DATE] {date}\n"
        + (f"[EDITED] {edited}\n" if edited else "")
        + "-" * 100 + "\n"
        + (text.strip() if text else "[NO TEXT]") + "\n"
        + "=" * 100 + "\n\n"
    )


# === HEARTBEAT ===
async def heartbeat(versions):
    while True:
        total_msgs = sum(len(v) for v in versions.values())
        logging.info(f"💓 Worker alive — оброблено {total_msgs} повідомлень")
        await asyncio.sleep(600)


# === ГОЛОВНА ===
async def main():
    logging.info("🚀 Telegram Fetcher (Render Worker) стартує...")
    client = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)

    async with client:
        me = await client.get_me()
        logging.info(f"✅ Telegram авторизовано як: @{getattr(me, 'username', None) or me.id}")

        versions = {name: {} for name in CHANNELS}

        async def handle_message(msg, name, init_mode=False):
            txt = msg.message or ""
            mid = str(msg.id)
            date = local_time(msg.date)
            edited = local_time(msg.edit_date) if msg.edit_date else ""

            if mid in versions[name] and edited:
                versions[name][mid] += 1
                action = "оновлено існуюче"
            elif mid not in versions[name]:
                versions[name][mid] = 1
                action = "нове повідомлення"
            else:
                action = "пропущено без змін"
                return

            block = build_block(name, mid, versions[name][mid], date, edited, txt)
            await save_to_supabase(name, block)

            prefix = "🔄 [INIT]" if init_mode else "📩"
            logging.info(
                f"{prefix} [{name}] {action} id={mid} (v{versions[name][mid]}) | "
                f"Дата {date} | Довжина блоку {len(block)}"
            )
            logging.info(f"💾 [{name}] Оновлено файл у Supabase ✅")

        # === Ініціалізація — підтягнути останні 10 повідомлень ===
        logging.info("📊 Ініціалізація: зчитую останні 10 повідомлень з кожного каналу...")
        for name, ref in CHANNELS.items():
            try:
                ent = await client.get_entity(ref)
                count = 0
                async for msg in client.iter_messages(ent, limit=10):
                    await handle_message(msg, name, init_mode=True)
                    count += 1
                logging.info(f"📗 [{name}] Ініціалізовано {count} повідомлень.")
            except Exception as e:
                logging.error(f"⚠️ Ініціалізація {name} не вдалася: {e}")

        # === Підключення до каналів у реальному часі ===
        for name, ref in CHANNELS.items():
            try:
                ent = await client.get_entity(ref)
                logging.info(f"📡 Читаю канал: {name} ({ref}) — очікування нових повідомлень...")
            except Exception as e:
                logging.error(f"⚠️ Не вдалося отримати entity для {name}: {e}")
                continue

            @client.on(events.NewMessage(chats=[ent]))
            async def new_message(event, _n=name):
                await handle_message(event.message, _n)

            @client.on(events.MessageEdited(chats=[ent]))
            async def edited_message(event, _n=name):
                await handle_message(event.message, _n)

        logging.info("✅ Усі канали активні. Очікуємо оновлення...")
        asyncio.create_task(heartbeat(versions))
        await client.run_until_disconnected()


# === РЕСТАРТЕР ===
if __name__ == "__main__":
    backoff = 5
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.exception("💥 Помилка виконання: %s", e)
            logging.info(f"♻️ Рестарт через {backoff} сек...")
            asyncio.run(asyncio.sleep(backoff))
            backoff = min(backoff * 2, 60)
        else:
            backoff = 5
