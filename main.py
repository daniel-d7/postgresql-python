import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from psycopg2.extras import Json

from modules import connect

FOLDER = "./raw_tiki_input/"
TABLE_NAME = "d_products"
LOGS_FILE = "logs.txt"
MAX_WORKERS = 2
CHECKPOINT_DB = "checkpoint.db"

logging.basicConfig(
    filename=LOGS_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

in_progress = set()
lock = threading.Lock()


def create_ckptdb():
    import sqlite3

    conn_ckpt = sqlite3.connect(CHECKPOINT_DB)
    cur = conn_ckpt.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS checkpoints (file VARCHAR(255))")
    conn_ckpt.commit()
    conn_ckpt.close()


def load_ckpt():
    import sqlite3

    create_ckptdb()
    conn_ckpt = sqlite3.connect(CHECKPOINT_DB)
    cur = conn_ckpt.cursor()
    cur.execute("SELECT * FROM checkpoints")
    rows = cur.fetchall()
    conn_ckpt.close()
    return [row[0] for row in rows]


def save_checkpoint(pid):
    import sqlite3

    conn_ckpt = sqlite3.connect(CHECKPOINT_DB)
    cur = conn_ckpt.cursor()
    cur.execute("INSERT INTO checkpoints (pid) VALUES (?)", (pid,))
    conn_ckpt.commit()
    conn_ckpt.close()


def check_table(conn, table_name):
    cur = conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGINT PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price INTEGER,
        description TEXT
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()


def check_conn_db():
    try:
        conn = connect()
        conn.close()
        logger.info("Connected to database successfully!")
    except Exception:
        logger.error("Failed to connect to database!", exc_info=True)
        exit(1)


def load_json(json_path, table_name):
    conn = connect()
    cursor = conn.cursor()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)  # Assumes array of objects
    for record in data:
        # Convert nested dicts to JSON strings for PostgreSQL JSON columns
        processed_record = {
            k: Json(v) if isinstance(v, dict) else v for k, v in record.items()
        }
        columns = ", ".join(processed_record.keys())
        placeholders = ", ".join(["%s"] * len(processed_record))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({
            placeholders
        }) ON CONFLICT (id) DO NOTHING"
        cursor.execute(query, list(processed_record.values()))
    conn.commit()
    cursor.close()
    conn.close()


def load2db(file, table_name):
    with lock:
        if file in in_progress:
            return (file, False)  # Already being processed
        in_progress.add(file)
    try:
        load_json(os.path.join(FOLDER, file), table_name)
        save_checkpoint(file)
        return (file, True)
    except Exception:
        return (file, False)
    finally:
        with lock:
            in_progress.remove(file)


def main():
    logger.info("Loading sequence started!")
    loaded = load_ckpt()
    json_files = [
        file
        for file in os.listdir(FOLDER)
        if file.endswith("json") and os.path.isfile(os.path.join(FOLDER, file))
    ]
    if len(json_files) == 0:
        logger.warning("JSON folder is empty!")
        exit(1)
    else:
        conn = connect()
        check_table(conn, TABLE_NAME)
        conn.close()
        unloaded = [file for file in json_files if file not in loaded]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file = {
                executor.submit(load2db, file, TABLE_NAME): file for file in unloaded
            }
            for future in as_completed(future_to_file):
                file_name, success = future.result()
                if success:
                    logger.info(f"{file_name} loaded to database successfully!")
                else:
                    logger.error(f"Error loading {file_name} - see logs for details")
    logger.info("Completed!")


if __name__ == "__main__":
    main()
