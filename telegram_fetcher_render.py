import os
import asyncio
from datetime import datetime
from telethon import TelegramClient
from telethon.sessions import StringSession
from storage_utils import upload_text, download_text
from dotenv import load_dotenv

# ──────────────── ЗМІННІ ОТОЧЕННЯ ────────────────
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TG_API_ID = int(os.getenv("TG_API_ID", "6"))  # резервне значення — офіційний Telegram test ID
TG_API_HASH = os.getenv("TG_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_USER_SESSION = os.getenv("TG_USER_SESSION")  # 🔸 якщо є — працюємо як юзер
SESSION_NAME = "render_fetcher_session"

# ──────────────── КАНАЛИ ────────────────
CHANNELS = {
    "MIRVALUTY": "mirvaluty",
    "GARANT": "obmen_kyiv",
    "KIT_GROUP": "obmenka_kievua",
    "CHANGE_KYIV": "kiev_change",
    "VALUTA_KIEV": "valuta_kiev",
    "UACOIN": "uacoin",
    "SWAPS": "obmen_usd",
}

HISTORY_LIMIT = 300


# ──────────────── ОСНОВНА ЛОГІКА ────────────────
async def fetch_channel_history():
    print("[START] Telegram Fetcher (Render)")
    client = None

    # ✅ Якщо є user session — підключаємось як юзер
    if TG_USER_SESSION:
        print("[MODE] Using USER session (StringSession). Full access ✅")
        client = TelegramClient(StringSession(TG_USER_SESSION), TG_API_ID, TG_API_HASH)
        await client.start()
    # ⚠️ Інакше — fallback на бот-токен
    elif TG_BOT_TOKEN:
        print("[MODE] Using BOT token (restricted mode ⚠️)")
        client = TelegramClient(SESSION_NAME, TG_API_ID, TG_API_HASH)
        await client.start(bot_token=TG_BOT_TOKEN)
    else:
        raise ValueError("❌ No TG_USER_SESSION or TG_BOT_TOKEN provided!")

    print("[TG] ✅ Авторизація успішна")

    for label, username in CHANNELS.items():
        try:
            print(f"[TG] 📥 Читаю канал: {label} / @{username}")
            messages = []

            async for msg in client.iter_messages(username, limit=HISTORY_LIMIT):
                if msg.text:
                    block = (
                        f"[{msg.date.strftime('%Y-%m-%d %H:%M:%S')}]\n"
                        f"{msg.text}\n"
                        + "-" * 80
                    )
                    messages.append(block)

            if messages:
                content = "\n".join(reversed(messages))
                fname = f"{label}_raw.txt"
                upload_text(fname, content, upsert=True)
                print(f"[STORE] ☁️ {fname} → Supabase ({len(messages)} msgs)")
            else:
                print(f"[TG] ⚠️ {label}: немає текстових повідомлень")

        except Exception as e:
            print(f"[ERR] {label}: {e}")

    await client.disconnect()
    print("[DONE] ✅ Завершено")


# ──────────────── ЗАПУСК ────────────────
if __name__ == "__main__":
    asyncio.run(fetch_channel_history())
