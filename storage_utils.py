import os
import logging
import time
from supabase import create_client, Client
from storage3.exceptions import StorageApiError

# === 1. ENV ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("❌ Відсутні SUPABASE_URL або SUPABASE_SERVICE_ROLE_KEY")
    raise SystemExit(1)

# === 2. Ініціалізація клієнта ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
BUCKET = "raw"

logging.info(f"✅ Supabase Storage ініціалізовано (bucket: '{BUCKET}')")


# === 3. Допоміжна функція — зчитування існуючого файлу ===
def read_current_file(filename: str) -> str:
    """Повертає поточний вміст файлу з Supabase Storage, якщо існує"""
    try:
        data = supabase.storage.from_(BUCKET).download(filename)
        return data.decode("utf-8")
    except Exception:
        return ""


# === 4. Основна функція запису ===
async def save_to_supabase(channel_name: str, new_block: str):
    """
    Додає новий блок у початок RAW-файлу (оновлює або створює його в Supabase Storage).
    """
    filename = f"{channel_name.upper()}_raw.txt"
    retries = 3

    for attempt in range(1, retries + 1):
        try:
            old_content = read_current_file(filename)
            new_content = (new_block or "") + (old_content or "")
            data = new_content.encode("utf-8")

            # спробуємо upload — якщо файл існує, буде 409 (Duplicate)
            try:
                supabase.storage.from_(BUCKET).upload(
                    filename,
                    data,
                    file_options={"contentType": "text/plain; charset=utf-8"}
                )
                logging.info(f"🆕 [{channel_name}] Створено новий файл {filename}")
            except StorageApiError as e:
                if e.args and "Duplicate" in str(e):
                    # оновлюємо файл якщо вже існує
                    supabase.storage.from_(BUCKET).update(
                        filename,
                        data,
                        file_options={"contentType": "text/plain; charset=utf-8"}
                    )
                    logging.info(f"♻️ [{channel_name}] Оновлено існуючий файл {filename}")
                else:
                    raise

            return  # успішно завершено — вихід

        except Exception as e:
            logging.error(f"⚠️ [{channel_name}] Помилка запису (спроба {attempt}/{retries}): {e}")
            if attempt < retries:
                sleep_time = attempt * 3
                logging.info(f"🔁 Повтор через {sleep_time} сек...")
                time.sleep(sleep_time)
            else:
                logging.exception(f"❌ [{channel_name}] Не вдалося записати після {retries} спроб.")
