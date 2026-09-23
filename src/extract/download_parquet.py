from pathlib import Path

from huggingface_hub import hf_hub_download

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

REPO_ID = "bettergovph/gov-library"
FILENAME = "repacts.parquet"


def download_parquet(force: bool = False) -> Path:
    """Download the BetterGov repacts Parquet into data/raw/bettergov/.

    Uses huggingface_hub, which handles redirects, retries, and caching.
    If `force` is True, the cached file is bypassed and re-downloaded.
    """
    dest_dir = settings.raw_dir / "bettergov"
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching %s/%s via huggingface_hub", REPO_ID, FILENAME)

    local_path = hf_hub_download(
        repo_id=REPO_ID,
        filename=FILENAME,
        repo_type="dataset",
        local_dir=str(dest_dir),
        force_download=force,
    )

    path = Path(local_path)
    size_mb = path.stat().st_size / (1024 * 1024)
    logger.info("Saved to %s (%.2f MB)", path, size_mb)
    return path


if __name__ == "__main__":
    download_parquet()