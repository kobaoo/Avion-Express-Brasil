import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres")
CONNECT_ARGS = {"options": "-c search_path=olist,public"}

engine = create_engine(DB_URL, connect_args=CONNECT_ARGS, future=True)
