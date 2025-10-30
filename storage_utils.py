import os, logging
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("❌ SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY відсутні")
    raise SystemExit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
logging.info("✅ Supabase клієнт ініціалізовано (bucket 'raw').")

def _read_current(filename: str) -> str:
    try:
        b = supabase.storage.from_("raw").download(filename)  # 200 OK або помилка
        return b.decode("utf-8")
    except Exception:
        return ""  # файла ще нема

async def save_to_supabase(channel_name: str, block: str):
    """
    Препендимо блок у raw/{CHANNEL}_raw.txt
    - якщо файл існує -> update (перезапис)
    - якщо нема        -> upload (створення)
    """
    filename = f"{channel_name.upper()}_raw.txt"
    try:
        old = _read_current(filename)
        new_content = (block or "") + (old or "")
        data = new_content.encode("utf-8")

        if old == "":  # файла ще не було
            # створюємо
            supabase.storage.from_("raw").upload(filename, data, file_options={"contentType": "text/plain; charset=utf-8"})
            logging.info(f"🆕 Створено raw/{filename}")
        else:
            # перезаписуємо існуючий
            supabase.storage.from_("raw").update(filename, data, file_options={"contentType": "text/plain; charset=utf-8"})
            logging.info(f"♻️ Оновлено raw/{filename}")

    except Exception as e:
        logging.exception("❌ Помилка запису у Supabase Storage (raw/%s): %s", filename, e)
