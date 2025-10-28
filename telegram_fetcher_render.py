# telegram_fetcher_render.py
# Render version — авторизація через BOT TOKEN
import os
import asyncio
from datetime import datetime
from telethon import TelegramClient, events
from storage_utils import upload_text, download_text

# === ENV VARIABLES ===
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")

CHANNELS = [
    "MIRVALUTY",
    "GARANT",
    "KIT_GROUP",
    "UACOIN",
    "SWAPS",
    "CHANGE_KYIV",
    "VALUTA_KIEV",
]

SESSION_NAME = "render_fetcher_session"


def make_block(channel: str, message_id: int, version: int, text: str, ts: datetime, edited_ts=None) -> str:
    lines = []
    lines.append(f"[CHANNEL] {channel}")
    lines.append(f"[MESSAGE_ID] {message_id}")
    lines.append(f"[VERSION] v{version}")
    lines.append(f"[DATE] {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    if edited_ts:
        lines.append(f"[EDITED] {edited_ts.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("-" * 100)
    lines.append(text or "")
    lines.append("=" * 120)
    return "\n".join(lines) + "\n"


def detect_next_version(existing: str, message_id: int) -> int:
    v = 0
    key = f"[MESSAGE_ID] {message_id}"
    for line in existing.splitlines():
        if line.strip() == key:
            v = max(v, 1)
        if v >= 1 and line.startswith("[VERSION] v"):
            try:
                num = int(line.split("v")[-1].strip())
                if num > v:
                    v = num
            except Exception:
                pass
    return v + 1 if v > 0 else 1


async def bootstrap_history(client: TelegramClient, name: str):
    filename = f"{name}_raw.txt"
    current = download_text(filename)
    if current:
        return
    entity = await client.get_entity(name)
    msgs = []
    async for m in client.iter_messages(entity, limit=300):
        if not m.message:
            continue
        ts = m.date
        block = make_block(name, m.id, 1, m.message, ts, None)
        msgs.append(block)
    msgs.reverse()
    content = "".join(msgs)
    upload_text(filename, content, upsert=True)
    print(f"[INIT] Snapshot → {filename} ({len(msgs)} msgs)")


async def main():
    print("[FETCHER] Starting (Render, BOT mode)...")
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        # Авторизація через BOT TOKEN
        await client.start(bot_token=BOT_TOKEN)
        print("[FETCHER] Connected via bot token. Bootstrapping...")

        for ch in CHANNELS:
            try:
                await bootstrap_history(client, ch)
            except Exception as e:
                print(f"[WARN] Snapshot failed for {ch}: {e}")

        @client.on(events.NewMessage(chats=CHANNELS))
        async def on_new_message(event):
            try:
                name = (await event.get_chat()).username or (await event.get_chat()).title
                name = str(name).upper().replace("@", "").replace(" ", "_")
                filename = f"{name}_raw.txt"
                existing = download_text(filename)
                version = detect_next_version(existing, event.id)
                block = make_block(name, event.id, version, event.message.message or "", event.date, None)
                new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
                upload_text(filename, new_content, upsert=True)
                print(f"[NEW] {name} #{event.id} v{version} → {filename}")
            except Exception as e:
                print(f"[ERR] on_new_message: {e}")

        @client.on(events.MessageEdited(chats=CHANNELS))
        async def on_edit_message(event):
            try:
                name = (await event.get_chat()).username or (await event.get_chat()).title
                name = str(name).upper().replace("@", "").replace(" ", "_")
                filename = f"{name}_raw.txt"
                existing = download_text(filename)
                version = detect_next_version(existing, event.id)
                block = make_block(name, event.id, version, event.message.message or "", event.date, event.edit_date or event.date)
                new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
                upload_text(filename, new_content, upsert=True)
                print(f"[EDIT] {name} #{event.id} v{version} → {filename}")
            except Exception as e:
                print(f"[ERR] on_edit_message: {e}")

        print("[FETCHER] Listening for new messages...")
        await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
