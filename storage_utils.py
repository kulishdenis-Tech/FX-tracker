# === storage_utils.py ===
# Зберігає RAW блоки у Supabase Storage → bucket 'raw'
import os
import logging
from supabase import create_client, Client

# ======== Ініціалізація клієнта Supabase ========
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


# ======== Функція збереження RAW ========
async def save_to_supabase(channel_name: str, block: str):
    """
    Додає блок зверху у файл Supabase Storage:
      raw/{CHANNEL}_raw.txt
    Нові повідомлення зверху, старі нижче.
    """
    try:
        filename = f"{channel_name.upper()}_raw.txt"

        # 1️⃣ Завантажуємо існуючий вміст (якщо файл є)
        try:
            existing_bytes = supabase.storage.from_("raw").download(filename)
            existing_text = existing_bytes.decode("utf-8")
        except Exception:
            existing_text = ""

        # 2️⃣ Додаємо новий блок зверху
        new_content = block + existing_text

        # 3️⃣ Завантажуємо оновлений файл у Supabase Storage
        #    (метод upload автоматично перезапише існуючий файл)
        supabase.storage.from_("raw").upload(
            filename,
            new_content.encode("utf-8")
        )

        logging.info(f"🗄️ Оновлено Supabase файл: raw/{filename}")

    except Exception as e:
        logging.exception("❌ Помилка при записі у Supabase Storage (raw): %s", e)
