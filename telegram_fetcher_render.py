# === telegram_fetcher_render.py ===
# Версія для Render Background Worker
# Слухає Telegram канали і зберігає повідомлення у Supabase Storage (bucket 'raw')

import os
import re
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import save_to_supabase

# ======== Налаштування логів ========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ======== Змінні середовища (Render ENV) ========
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
USER_SESSION = os.getenv("TG_USER_SESSION", "")
TZ = ZoneInfo("Europe/Kyiv")

if not API_ID or not API_HASH or not USER_SESSION:
    logging.error("❌ Відсутні обов’язкові TG_API_ID / TG_API_HASH / TG_USER_SESSION.")
    raise SystemExit(1)

# ======== Канали для моніторингу ========
CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd",
}

HISTORY_LIMIT = 200


# ======== Допоміжні ========
def now_str():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def local_dt(dt):
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def build_block(channel_name, message_id, version, date, edited, text):
    """Формує блок як у локальному RAW"""
    block = (
        "=" * 100 + "\n"
        + f"[CHANNEL] {channel_name}\n"
        + f"[MESSAGE_ID] {message_id}\n"
        + f"[VERSION] v{version}\n"
        + f"[DATE] {date}\n"
    )
    if edited:
        block += f"[EDITED] {edited}\n"
    block += (
        "-" * 100 + "\n"
        + (text.strip() if text else "[NO TEXT]") + "\n"
        + "=" * 100 + "\n\n"
    )
    return block


# ======== Основна логіка ========
async def parse_message(message, channel_name, versions):
    text = message.message or ""
    msg_id = str(message.id)
    date = local_dt(message.date)
    edited = local_dt(message.edit_date) if message.edit_date else ""

    # нова або оновлена версія
    if msg_id in versions and edited:
        versions[msg_id] += 1
    elif msg_id not in versions:
        versions[msg_id] = 1
    else:
        return

    block = build_block(channel_name, msg_id, versions[msg_id], date, edited, text)
    await save_to_supabase(channel_name, block)
    logging.info(f"[{channel_name}] id={msg_id} v{versions[msg_id]} збережено.")


async def get_entity_safe(client, ref):
    try:
        if isinstance(ref, int) or str(ref).startswith("-100"):
            return await client.get_input_entity(ref)
        else:
            return await client.get_entity(ref)
    except Exception as e:
        logging.warning(f"[WARN] Не вдалося отримати entity для {ref}: {e}")
        return None


async def load_existing_versions(client, channel_name):
    """Читає існуючі message_id → version (через метадані RAW у Supabase)"""
    # Поки спрощено — без завантаження з Supabase, версії формуються наново
    return {}


async def monitor_channel(client, channel_name, ref):
    versions = await load_existing_versions(client, channel_name)
    entity = await get_entity_safe(client, ref)
    if not entity:
        logging.error(f"[ERROR] Пропуск {channel_name}: немає доступу.")
        return

    logging.info(f"📡 Запуск моніторингу {channel_name}")

    @client.on(events.NewMessage(chats=[entity]))
    async def new_handler(event):
        await parse_message(event.message, channel_name, versions)

    @client.on(events.MessageEdited(chats=[entity]))
    async def edit_handler(event):
        await parse_message(event.message, channel_name, versions)


async def main():
    logging.info("🚀 Telegram Fetcher запущено (Render Worker Mode)")
    client = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)

    async with client:
        for name, ref in CHANNELS.items():
            await monitor_channel(client, name, ref)
        logging.info("✅ Усі канали активовані, очікуємо повідомлення.")
        await client.run_until_disconnected()


if __name__ == "__main__":
    delay = 5
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.exception("❌ Worker впав: %s", e)
            logging.info("♻️ Перезапуск через %s сек...", delay)
            asyncio.run(asyncio.sleep(delay))
            delay = min(delay * 2, 60)
        else:
            delay = 5
