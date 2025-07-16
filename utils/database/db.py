import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": "localhost",
    "database": "scrapper",
    "user": "postgres",
    "password": "1234"
}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)
