import os
import asyncio
from telethon import TelegramClient
from storage_utils import upload_text, download_text  # cloud-сторедж Supabase
from dotenv import load_dotenv
from datetime import datetime

# --- Завантаження змінних оточення ---
load_dotenv()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Telegram
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
API_ID = int(os.getenv("TG_API_ID", 6))  # Тестовий ID (Telegram default)
API_HASH = os.getenv("TG_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")

if not TG_BOT_TOKEN:
    raise ValueError("❌ TG_BOT_TOKEN не знайдено у змінних оточення Render!")

SESSION_NAME = "render_fetcher_session"

# --- Канали ---
CHANNELS = {
    "MIRVALUTY": "mirvaluty",
    "GARANT": "obmen_kyiv",
    "KIT_GROUP": "obmenka_kievua",
    "CHANGE_KYIV": "kiev_change",
    "VALUTA_KIEV": "valuta_kiev",
    "UACOIN": "uacoin",
    "SWAPS": "obmen_usd",
}

HISTORY_LIMIT = 300  # скільки повідомлень тягнути при ініціалізації


# --- Основна функція ---
async def fetch_channel_history():
    print("[START] Telegram Fetcher (BOT mode, Render)")

    # створюємо клієнт із фіктивними API_ID та API_HASH
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start(bot_token=TG_BOT_TOKEN)
    print("[TG] ✅ Авторизація через бот-токен успішна")

    for label, username in CHANNELS.items():
        try:
            print(f"[TG] 📥 Читаю канал: {label} / @{username}")
            messages = []

            async for msg in client.iter_messages(username, limit=HISTORY_LIMIT):
                if msg.text:
                    line = (
                        f"[{msg.date.strftime('%Y-%m-%d %H:%M:%S')}]\n"
                        f"{msg.text}\n"
                        + "-" * 80
                    )
                    messages.append(line)

            if messages:
                raw_text = "\n".join(reversed(messages))  # хронологічно
                fname = f"{label}_raw.txt"
                upload_text(fname, raw_text, upsert=True)
                print(f"[STORE] ☁️ {fname} збережено у Supabase Storage ({len(messages)} msgs)")
            else:
                print(f"[TG] ⚠️ {label}: немає текстових повідомлень.")
        except Exception as e:
            print(f"[ERR] {label}: {e}")

    await client.disconnect()
    print("[DONE] ✅ Завершено")


if __name__ == "__main__":
    asyncio.run(fetch_channel_history())
