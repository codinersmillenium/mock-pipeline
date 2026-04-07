import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base

# Get database URL from environment, fallback to None
DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,          # drop stale connections before use
    pool_size=5,                 # keep 5 persistent connections
    max_overflow=10,             # allow up to 10 extra under burst load
    connect_args={"connect_timeout": 5},  # connection timeout in seconds
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()