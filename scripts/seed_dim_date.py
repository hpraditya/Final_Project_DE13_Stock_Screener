"""
Isi tabel gold.dim_date dari 2024-01-01 hingga 2030-12-31.
Idempoten: ON CONFLICT DO NOTHING.

Usage:
    python scripts/seed_dim_date.py
"""
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def main():
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    seed_sql = (Path(__file__).parent.parent / "warehouse" / "seeds" / "dim_date_seed.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(seed_sql)
    conn.commit()
    conn.close()
    print("dim_date seeded successfully.")


if __name__ == "__main__":
    main()
