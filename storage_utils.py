# storage_utils.py
import os
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def upload_text(name: str, content: str, upsert: bool = True) -> None:
    sb = get_client()
    sb.storage.from_("raw").upload(name, content.encode("utf-8"), {"upsert": upsert})

def download_text(name: str) -> str:
    sb = get_client()
    try:
        data = sb.storage.from_("raw").download(name)
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""  # якщо файла ще нема
