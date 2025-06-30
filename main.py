import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from modules import connect

FOLDER = "./raw_tiki_input/"
table_name = "d_products"
logs_file = "logs.txt"
MAX_WORKERS = 2
CHECKPOINT_FILE = "checkpoint.txt"


def load_checkpoint():
    processed = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            for line in f:
                processed.add(line.strip())
    return processed


def save_checkpoint(file):
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(f"{file}\n")
        f.flush()


def check_table(conn, table_name):
    conn = connect()
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGINT PRIMARY KEY,
    name TEXT,
    url_key TEXT,
    price INTEGER,
    description TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def load_json(json_path, table_name):
    conn = connect()
    cursor = conn.cursor()

    with open(json_path, "r") as f:
        data = json.load(f)  # Assumes array of objects

    for record in data:
        # Convert nested dicts to JSON strings
        processed_record = {
            k: json.dumps(v) if isinstance(v, dict) else v for k, v in record.items()
        }
        columns = ", ".join(processed_record.keys())
        placeholders = ", ".join(["%s"] * len(processed_record))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, list(processed_record.values()))

    conn.commit()
    cursor.close()
    conn.close()


try:
    connect()
    print("Connect to database successfully!")
except Exception as e:
    print("Failed to connect to database!")
    print(e)
    exit


def load2db(file, table_name, logs_file):
    try:
        load_json(os.path.join(FOLDER, file), table_name)
        print(f"{file} loaded to database successfully!")
        with open(logs_file, mode="a") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] - File {file} loaded successfully\n")
        save_checkpoint(file)
    except Exception as e:
        print(f"Error in loading file {file}: {e}")
        with open(logs_file, mode="a") as f:
            f.write(f"[{timestamp}] - File {file} failed to load - {e}\n")


def main():
    loaded = load_checkpoint()
    json_files = [
        file
        for file in os.listdir(FOLDER)
        if file.endswith("json") and os.path.isfile(os.path.join(FOLDER, file))
    ]

    if len(json_files) <= 0:
        print("json folder is empty!")
        exit
    else:
        check_table(connect(), table_name)

    unloaded = [file for file in json_files if file not in loaded]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {}
        for file in unloaded:
            full_path = file
            future = executor.submit(load2db, full_path, table_name, logs_file)
            future_to_file[future] = file

        for future in as_completed(future_to_file):
            file_name, success = future.result()
            if success:
                print(f"{file_name} loaded to database successfully!")
            else:
                print(f"Error loading {file_name} - see logs for details")


if __name__ == "__main__":
    main()
