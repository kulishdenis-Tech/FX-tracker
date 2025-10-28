# telegram_fetcher_render.py
import os
import asyncio
from datetime import datetime
from telethon import TelegramClient, events
from storage_utils import upload_text, download_text

# 🔐 Авторизація через BOT_TOKEN (Render не підтримує interactive login)
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]

# 🔸 Оригінальна структура каналів (dict)
CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd"
}

SESSION_NAME = "render_fetcher_session"

# ========= Форматування блоку =========
def make_block(channel: str, message_id: int, version: int, text: str, ts: datetime, edited_ts=None) -> str:
    lines = [
        f"[CHANNEL] {channel}",
        f"[MESSAGE_ID] {message_id}",
        f"[VERSION] v{version}",
        f"[DATE] {ts.strftime('%Y-%m-%d %H:%M:%S')}",
    ]
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
                v = max(v, num)
            except:
                pass
    return v + 1 if v > 0 else 1


# ========= Початкове завантаження =========
async def bootstrap_history(client: TelegramClient, alias: str, username: str):
    filename = f"{alias}_raw.txt"
    current = download_text(filename)
    if current:
        return
    entity = await client.get_entity(username)
    msgs = []
    async for m in client.iter_messages(entity, limit=300):
        if not m.message:
            continue
        block = make_block(alias, m.id, 1, m.message, m.date)
        msgs.append(block)
    msgs.reverse()
    upload_text(filename, "".join(msgs), upsert=True)
    print(f"[INIT] Snapshot → {filename} ({len(msgs)} msgs)")


# ========= Основна логіка =========
async def main():
    print("[FETCHER] Starting in Render BOT mode...")
    client = TelegramClient(SESSION_NAME, 0, "")
    await client.start(bot_token=BOT_TOKEN)
    print("[FETCHER] Connected. Bootstrapping channels...")

    for alias, username in CHANNELS.items():
        try:
            await bootstrap_history(client, alias, username)
        except Exception as e:
            print(f"[WARN] Snapshot failed for {alias}: {e}")

    # Нові повідомлення
    @client.on(events.NewMessage(chats=list(CHANNELS.values())))
    async def on_new_message(event):
        try:
            chat = await event.get_chat()
            alias = next((a for a, u in CHANNELS.items() if u.lower().lstrip("@") == chat.username.lower()), chat.username)
            filename = f"{alias}_raw.txt"
            existing = download_text(filename)
            version = detect_next_version(existing, event.id)
            block = make_block(alias, event.id, version, event.message.message or "", event.date)
            new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
            upload_text(filename, new_content, upsert=True)
            print(f"[NEW] {alias} #{event.id} v{version} → {filename}")
        except Exception as e:
            print(f"[ERR] on_new_message: {e}")

    # Редагування повідомлень
    @client.on(events.MessageEdited(chats=list(CHANNELS.values())))
    async def on_edit_message(event):
        try:
            chat = await event.get_chat()
            alias = next((a for a, u in CHANNELS.items() if u.lower().lstrip("@") == chat.username.lower()), chat.username)
            filename = f"{alias}_raw.txt"
            existing = download_text(filename)
            version = detect_next_version(existing, event.id)
            block = make_block(alias, event.id, version, event.message.message or "", event.date, event.edit_date)
            new_content = (existing + ("\n" if existing and not existing.endswith("\n") else "") + block)
            upload_text(filename, new_content, upsert=True)
            print(f"[EDIT] {alias} #{event.id} v{version} → {filename}")
        except Exception as e:
            print(f"[ERR] on_edit_message: {e}")

    print("[FETCHER] Listening for updates...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
