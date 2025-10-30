import os
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import save_to_supabase

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

# === TIMEZONE ===
TZ = ZoneInfo("Europe/Kyiv")

# === ENV ===
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
USER_SESSION = os.getenv("TG_USER_SESSION", "")

if not (API_ID and API_HASH and USER_SESSION):
    logging.error("❌ TG_API_ID / TG_API_HASH / TG_USER_SESSION відсутні")
    raise SystemExit(1)

# === КАНАЛИ ДЛЯ МОНІТОРИНГУ ===
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
def local(dt):
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def build_block(channel, msg_id, ver, date, edited, text):
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
async def heartbeat():
    while True:
        logging.info("💓 Worker alive — все стабільно")
        await asyncio.sleep(600)


# === ГОЛОВНА ФУНКЦІЯ ===
async def main():
    logging.info("🚀 Telegram Fetcher (Render Worker) стартує...")
    client = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)

    async with client:
        me = await client.get_me()
        logging.info(f"✅ Telegram авторизовано як: @{getattr(me, 'username', None) or me.id}")

        versions = {name: {} for name in CHANNELS}

        async def handle_message(msg, name):
            txt = msg.message or ""
            mid = str(msg.id)
            date = local(msg.date)
            edited = local(msg.edit_date) if msg.edit_date else ""

            # версії
            if mid in versions[name] and edited:
                versions[name][mid] += 1
            elif mid not in versions[name]:
                versions[name][mid] = 1
            else:
                return

            block = build_block(name, mid, versions[name][mid], date, edited, txt)
            await save_to_supabase(name, block)
            logging.info(f"💾 [{name}] збережено id={mid} (v{versions[name][mid]}) — довжина {len(block)}")

        # підписка на канали
        for name, ref in CHANNELS.items():
            try:
                ent = await client.get_entity(ref)
                logging.info(f"📡 Моніторимо канал: {name} ({ref})")
            except Exception as e:
                logging.error(f"⚠️ Не вдалося отримати entity для {name}: {e}")
                continue

            @client.on(events.NewMessage(chats=[ent]))
            async def new_message(event, _n=name):
                await handle_message(event.message, _n)

            @client.on(events.MessageEdited(chats=[ent]))
            async def edited_message(event, _n=name):
                await handle_message(event.message, _n)

        asyncio.create_task(heartbeat())
        await client.run_until_disconnected()


# === БЕЗПЕРЕРВНИЙ ЦИКЛ (РЕСТАРТ ПРИ ЗБОЇ) ===
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
