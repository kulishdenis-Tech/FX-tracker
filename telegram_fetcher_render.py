import os, asyncio, logging
from datetime import datetime
from zoneinfo import ZoneInfo
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from storage_utils import save_to_supabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
TZ = ZoneInfo("Europe/Kyiv")

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
USER_SESSION = os.getenv("TG_USER_SESSION", "")
if not (API_ID and API_HASH and USER_SESSION):
    logging.error("❌ TG_API_ID / TG_API_HASH / TG_USER_SESSION відсутні")
    raise SystemExit(1)

CHANNELS = {
    "MIRVALUTY": "@mirvaluty",
    "GARANT": "@obmen_kyiv",
    "KIT_GROUP": "@obmenka_kievua",
    "CHANGE_KYIV": "@kiev_change",
    "VALUTA_KIEV": "@valuta_kiev",
    "UACOIN": "@uacoin",
    "SWAPS": "@Obmen_usd",
}

def local(dt): return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if dt else ""

def block(channel, msg_id, ver, date, edited, text):
    s =  "="*100 + "\n"
    s += f"[CHANNEL] {channel}\n"
    s += f"[MESSAGE_ID] {msg_id}\n"
    s += f"[VERSION] v{ver}\n"
    s += f"[DATE] {date}\n"
    if edited: s += f"[EDITED] {edited}\n"
    s += "-"*100 + "\n"
    s += (text.strip() if text else "[NO TEXT]") + "\n"
    s += "="*100 + "\n\n"
    return s

async def heartbeat():
    while True:
        logging.info("💓 Worker alive")
        await asyncio.sleep(600)

async def main():
    logging.info("🚀 Telegram Fetcher (Render Worker)")
    client = TelegramClient(StringSession(USER_SESSION), API_ID, API_HASH)
    async with client:
        me = await client.get_me()
        logging.info(f"✅ Telegram авторизовано як: @{getattr(me,'username', None) or me.id}")

        versions = {name:{} for name in CHANNELS}  # простий лічильник версій в пам'яті

        async def handle(msg, name):
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
            blk = block(name, mid, versions[name][mid], date, edited, txt)
            await save_to_supabase(name, blk)
            logging.info(f"[{name}] saved id={mid} v{versions[name][mid]}")

        # підписка на кожен канал
        for name, ref in CHANNELS.items():
            try:
                ent = await client.get_entity(ref)
            except Exception as e:
                logging.error(f"⚠️ {name}: не отримав entity ({e})")
                continue

            @client.on(events.NewMessage(chats=[ent]))
            async def _nm(event, _n=name):
                await handle(event.message, _n)

            @client.on(events.MessageEdited(chats=[ent]))
            async def _ed(event, _n=name):
                await handle(event.message, _n)

            logging.info(f"📡 Моніторимо: {name} {ref}")

        asyncio.create_task(heartbeat())
        await client.run_until_disconnected()

if __name__ == "__main__":
    backoff = 5
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logging.exception("💥 Crash: %s", e)
            logging.info("♻️ Рестарт через %s сек", backoff)
            asyncio.run(asyncio.sleep(backoff))
            backoff = min(backoff*2, 60)
        else:
            backoff = 5
