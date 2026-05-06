import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

_HERE = os.path.dirname(os.path.abspath(__file__))

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}
# Existing database to connect to for CREATE DATABASE (server has no 'postgres' db)
BOOTSTRAP_DB = os.environ.get("POSTGRES_BOOTSTRAP_DB", "subindex_1")


def create_db_if_not_exists(host, port, user, password):
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname=BOOTSTRAP_DB)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = 'cii'")
            if cur.fetchone() is None:
                cur.execute('CREATE DATABASE cii')
                print("  Created database: cii")
            else:
                print("  Database cii already exists — skipping create")
    finally:
        conn.close()


def apply_schema(host, port, user, password):
    schema_path = os.path.join(_HERE, "cii_schema.sql")
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname="cii")
    try:
        conn.autocommit = True  # schema SQL has explicit BEGIN/COMMIT; autocommit required
        with conn.cursor() as cur:
            with open(schema_path) as f:
                cur.execute(f.read())
    finally:
        conn.close()
    print("  Schema applied.")


if __name__ == "__main__":
    h, p, u, pw = (DB_CONFIG["host"], DB_CONFIG["port"],
                   DB_CONFIG["user"], DB_CONFIG["password"])
    print("Setting up CII database...")
    create_db_if_not_exists(h, p, u, pw)
    apply_schema(h, p, u, pw)
    print("Done.")
