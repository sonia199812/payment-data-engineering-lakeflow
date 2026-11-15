
import pandas as pd
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--count", type=int, default=100)
parser.add_argument("--skip", type=int, default=0)
args = parser.parse_args()

# Read only the columns required for this project
columns = [
    "trans_num",
    "trans_date_trans_time",
    "merchant",
    "category",
    "amt",
    "is_fraud"
]

df = pd.read_csv(
    args.input,
    usecols=columns,
    skiprows=range(1, args.skip + 1),
    nrows=args.count
)

# Verify that transaction IDs are unique
assert df["trans_num"].notna().all()
assert df["trans_num"].is_unique

# Create output folder if needed
output_path = Path(args.output)
output_path.parent.mkdir(parents=True, exist_ok=True)

# Save Batch 1
df.to_csv(output_path, index=False)

print(f"Records created: {len(df)}")
print(f"Unique transactions: {df['trans_num'].nunique()}")
print(f"Saved to: {output_path}")
