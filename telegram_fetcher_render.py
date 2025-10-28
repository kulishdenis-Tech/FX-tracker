import os
import asyncio
from telethon import TelegramClient, events
from storage_utils import uploadtext, downloadtext  # твій cloud-сторедж
from dotenv import load_dotenv
from datetime import datetime

# --- Завантажуємо змінні оточення ---
load_dotenv()
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not TG_BOT_TOKEN:
    raise ValueError("❌ TG_BOT_TOKEN не знайдено у змінних оточення Render!")

SESSION_NAME = "render_fetcher_session"

# --- Параметри каналів ---
CHANNELS = {
    "MIRVALUTY": "mirvaluty",
    "GARANT": "obmenkyiv",
    "KITGROUP": "obmenkakievua",
    "CHANGEKYIV": "kievchange",
    "VALUTAKIEV": "valutakiev",
    "UACOIN": "uacoin",
    "SWAPS": "obmenusd"
}
HISTORY_LIMIT = 200  # вказуй скільки історії качати

async def fetch_channel_history():
    client = TelegramClient(SESSION_NAME, api_id=None, api_hash=None).start(bot_token=TG_BOT_TOKEN)
    await client.connect()
    if not await client.is_user_authorized():
        print("[ERROR] Не вдалося авторизуватись ботом.")

    for label, username in CHANNELS.items():
        print(f"[TG] Читаю: {label} / @{username} ...")
        messages = []
        async for msg in client.iter_messages(username, limit=HISTORY_LIMIT):
            if msg.text:
                messages.append(f"{msg.date.strftime('%Y-%m-%d %H:%M:%S')}
{msg.text}
{'-'*40}")
        if messages:
            raw_text = "
".join(messages)
            fname = f"{label}_raw.txt"
            uploadtext(fname, raw_text)   # запис у cloud storage (Supabase Storage/API)
            print(f"[STORE] {fname} збережено у cloud.")
        else:
            print(f"[TG] {label}: немає текстових повідомлень.")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(fetch_channel_history())
