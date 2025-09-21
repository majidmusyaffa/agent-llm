from typing import List, Dict, Any, Optional
import os
import sqlite3
from io import BytesIO
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

DB_PATH = "uploaded.db"
_table_name = "uploaded_table"

def execute_sql_query(query: str) -> List[Dict[str, Any]]:
    """
    Execute an SQL query and return the results as a list of dictionaries.
    Aman untuk dipanggil di thread berbeda karena selalu buka koneksi baru.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(query)

        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            result = [{k: row[k] for k in row.keys()} for row in rows]
        else:
            result = [{"affected_rows": cursor.rowcount}]
            conn.commit()

        conn.close()
        return result

    except sqlite3.Error as e:
        return [{"error": str(e)}]

    
import os
import sqlite3
import pandas as pd
from io import BytesIO

def init_database(file_bytes: BytesIO, filename: str) -> str:
    """Inisialisasi database SQLite dari file upload (CSV/XLSX)."""
    global _table_name

    # hapus database lama biar fresh
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # load file ke pandas
    if filename.endswith(".csv"):
        df = pd.read_csv(file_bytes)
    elif filename.endswith(".xlsx"):
        df = pd.read_excel(file_bytes)
    else:
        raise ValueError("Format file tidak didukung. Gunakan CSV atau XLSX.")

    # simpan ke sqlite file
    conn = sqlite3.connect(DB_PATH)
    df.to_sql(_table_name, conn, index=False, if_exists="replace")
    conn.close()

    return f"Database berhasil diinisialisasi dengan tabel '{_table_name}' dan {len(df)} baris."

def get_database_info() -> dict:
    """
    Mengembalikan schema tabel dan sample data dalam bentuk dict.
    Aman dipanggil lintas thread.
    """
    if not os.path.exists(DB_PATH):
        return {"error": "Database belum diinisialisasi."}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Ambil schema
    cursor.execute(f"PRAGMA table_info({_table_name})")
    schema = cursor.fetchall()
    schema_dict = {col[1]: col[2] for col in schema}

    # Ambil sample data
    df_sample = pd.read_sql_query(f"SELECT * FROM {_table_name} LIMIT 5", conn)
    sample_data = df_sample.to_dict(orient="records")

    conn.close()

    return {
        "schema": schema_dict,
        "sample_data": sample_data
    }


def get_database_metadata():
    """Mengembalikan schema tabel dan sample data sebagai dataframe."""
    if not os.path.exists(DB_PATH):
        return {"error": "Database belum diinisialisasi."}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ambil schema
    cursor.execute(f"PRAGMA table_info({_table_name})")
    schema = cursor.fetchall()
    schema_str = "Schema:\n"
    for col in schema:
        schema_str += f"- {col[1]} ({col[2]})\n"

    # ambil sample dataframe
    df_sample = pd.read_sql_query(f"SELECT * FROM {_table_name} LIMIT 5", conn)

    return schema_str, df_sample


def text_to_sql(query: str):
    """Eksekusi query SQL di database upload."""
    if not os.path.exists(DB_PATH):
        return {"error": "Database belum diinisialisasi."}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        # ambil nama kolom
        col_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # return sebagai string
        result = [dict(zip(col_names, row)) for row in rows] if col_names else []
        return str(result) if result else "Query berhasil dieksekusi, tapi tidak ada hasil."
    except Exception as e:
        return f"Error saat eksekusi query: {e}"
