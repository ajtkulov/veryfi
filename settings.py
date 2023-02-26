from pydantic import BaseSettings


class Settings(BaseSettings):
    pg_connection_string: str = f'postgresql://airflow:airflow@localhost:5433/veryfidev'


settings = Settings()
