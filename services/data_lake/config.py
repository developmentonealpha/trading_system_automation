import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")

    # PostgreSQL Connection
    DB_NAME = os.getenv("DB_NAME", "Trading_System")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "onealpha12345")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False
