# === telegram_fetcher_render.py ===
import os
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import save_to_supabase  # твоя функція для збереження

# ======== ЛОГИ ========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ======== ЗМІННІ СЕРЕДОВИЩА ========
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
USER_SESSION = os.getenv("TG_USER_SESSION", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# список каналів (через кому)
CHANNELS_RAW = os.getenv("CHANNELS", "@mirvaluty,@obmen_kyiv")
CHANNELS = [x.strip() for x in CHANNELS_RAW.split(",") if x.strip()]

# ======== ПЕРЕВІРКА НАЯВНОСТІ ========
if not all([API_ID, API_HASH, USER_SESSION, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    logging.error("❌ Відсутні необхідні змінні середовища. Перевір налаштування Render ENV.")
    raise SystemExit(1)

# ======== СТВОРЕННЯ КЛІЄНТА ========
def make_client() -> TelegramClient:
    try:
        return TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)
    except Exception as e:
        logging.exception("Помилка при створенні Telegram клієнта: %s", e)
        raise

# ======== ОСНОВНА ФУНКЦІЯ ========
async def run():
    client = make_client()
    await client.connect()
    logging.info("🔌 Підключення до Telegram виконано")

    if not await client.is_user_authorized():
        logging.error("❌ USER_SESSION недійсний. Згенеруй новий TG_USER_SESSION.")
        return

    logging.info("✅ Авторизація успішна")
    logging.info(f"📡 Слухаємо канали: {', '.join(CHANNELS)}")

    @client.on(events.NewMessage(chats=CHANNELS))
    async def handler(event):
        try:
            chat = getattr(event.chat, "username", "невідомо")
            text = event.message.message or ""
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"💬 [{chat}] {text[:100].replace(chr(10), ' ')}")

            await save_to_supabase(
                chat=chat,
                message=text,
                timestamp=ts,
            )

        except Exception as e:
            logging.exception("Помилка під час обробки повідомлення: %s", e)

    logging.info("🚀 Worker запущено. Очікуємо нові повідомлення...")
    await client.run_until_disconnected()

# ======== ЦИКЛ З АВТОРЕСТАРТОМ ========
if __name__ == "__main__":
    delay = 5
    while True:
        try:
            asyncio.run(run())
        except Exception as e:
            logging.exception("❗ Worker впав: %s", e)
            logging.info("♻️ Перезапуск через %s сек...", delay)
            asyncio.run(asyncio.sleep(delay))
            delay = min(delay * 2, 60)
        else:
            delay = 5
