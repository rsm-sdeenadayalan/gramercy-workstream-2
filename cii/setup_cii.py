import os, sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


def create_db_if_not_exists(host, port, user, password):
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'cii'")
        if cur.fetchone() is None:
            cur.execute('CREATE DATABASE cii')
            print("  Created database: cii")
        else:
            print("  Database cii already exists — skipping create")
    conn.close()


def apply_schema(host, port, user, password):
    schema_path = os.path.join(os.path.dirname(__file__), "cii_schema.sql")
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname="cii")
    conn.autocommit = True
    with conn.cursor() as cur:
        with open(schema_path) as f:
            cur.execute(f.read())
    conn.close()
    print("  Schema applied.")


if __name__ == "__main__":
    h, p, u, pw = (DB_CONFIG["host"], DB_CONFIG["port"],
                   DB_CONFIG["user"], DB_CONFIG["password"])
    print("Setting up CII database...")
    create_db_if_not_exists(h, p, u, pw)
    apply_schema(h, p, u, pw)
    print("Done.")
