import pandas as pd

df = pd.read_parquet("data/raw/bettergov/repacts.parquet")

print("SHAPE:", df.shape)
print()
print("COLUMNS & DTYPES:")
print(df.dtypes)
print()
print("NULL COUNTS:")
print(df.isna().sum())
print()
print("SAMPLE ROW (first record, truncated):")
row = df.iloc[0].to_dict()
for k, v in row.items():
    s = str(v)
    print(f"  {k}: {s[:300]}{'...' if len(s) > 300 else ''}")
print()
print("UNIQUE COUNT PER COLUMN:")
print(df.nunique())
print()
# Look for RA-number-like and year-like columns
for col in df.columns:
    sample = df[col].dropna().astype(str).head(3).tolist()
    print(f"{col} sample: {sample}")