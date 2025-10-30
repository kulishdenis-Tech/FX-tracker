# === storage_utils.py ===
# Зберігає RAW блоки у Supabase Storage → bucket 'raw'
import os
import logging
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("❌ Не задані SUPABASE_URL або SUPABASE_SERVICE_ROLE_KEY.")
    raise SystemExit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    logging.info("✅ Supabase клієнт ініціалізовано (bucket 'raw').")
except Exception as e:
    logging.exception("❌ Помилка створення клієнта Supabase: %s", e)
    raise


async def save_to_supabase(channel_name: str, block: str):
    """
    Додає блок зверху у файл Supabase Storage:
      raw/{CHANNEL}_raw.txt
    """
    try:
        filename = f"{channel_name.upper()}_raw.txt"

        # 1️⃣ Завантажуємо існуючий вміст
        try:
            existing_bytes = supabase.storage.from_("raw").download(filename)
            existing_text = existing_bytes.decode("utf-8")
        except Exception:
            existing_text = ""

        # 2️⃣ Новий контент зверху
        new_content = block + existing_text

        # 3️⃣ Завантаження назад у Storage
        supabase.storage.from_("raw").upload(
            filename,
            new_content.encode("utf-8"),
            upsert=True
        )

        logging.info(f"🗄️ Оновлено Supabase файл: raw/{filename}")

    except Exception as e:
        logging.exception("❌ Помилка при записі у Supabase Storage (raw): %s", e)
