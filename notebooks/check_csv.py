import pandas as pd
import hashlib

parquet = pd.read_parquet("data/curated/ra_master.parquet")
csv_back = pd.read_csv(
    "outputs/ra_master.csv",
    dtype=str, keep_default_na=False, na_values=[""],
)

# Structural
assert len(parquet) == len(csv_back), "row count mismatch"
assert list(parquet.columns) == list(csv_back.columns), "column mismatch"

# Content — spot-check the mega-statute with embedded newlines and quotes
mega = parquet[parquet["ra_id"] == "RA-386"].iloc[0]
mega_csv = csv_back[csv_back["ra_id"] == "RA-386"].iloc[0]
for col in ["content_clean", "content_normalized"]:
    h_parquet = hashlib.sha256(mega[col].encode("utf-8")).hexdigest()
    h_csv = hashlib.sha256(mega_csv[col].encode("utf-8")).hexdigest()
    assert h_parquet == h_csv, f"{col} corrupted through CSV round-trip"
    print(f"{col}: SHA-256 match ({len(mega[col])} chars)")

# Full-frame content check (ignoring dtype differences)
print("Full content hash match:",
      hashlib.sha256(
          parquet.sort_values("ra_id").to_csv(index=False).encode("utf-8")
      ).hexdigest() ==
      hashlib.sha256(
          csv_back.sort_values("ra_id").to_csv(index=False).encode("utf-8")
      ).hexdigest())