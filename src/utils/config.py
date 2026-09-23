import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _resolve_data_dir() -> Path:
    """DATA_DIR is /opt/airflow/data inside Docker; ./data locally."""
    env = os.getenv("DATA_DIR")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[2] / "data"


class Settings:
    def __init__(self):
        self.data_dir = _resolve_data_dir()

        self.postgres_user = os.getenv("POSTGRES_USER", "ra_user")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "ra_password")
        self.postgres_db = os.getenv("POSTGRES_DB", "ra_db")
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")

        self.lawphil_index_url = os.getenv(
            "LAWPHIL_BASE_URL",
            "https://lawphil.net/statutes/repacts/repacts.html",
        )
        self.lawphil_sample_size = int(os.getenv("LAWPHIL_SAMPLE_SIZE", "200"))

        self.bettergov_parquet_url = os.getenv(
            "BETTERGOV_PARQUET_URL",
            "https://huggingface.co/datasets/bettergovph/gov-library/resolve/main/repacts.parquet",
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def curated_dir(self) -> Path:
        return self.data_dir / "curated"

    @property
    def postgres_uri(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()