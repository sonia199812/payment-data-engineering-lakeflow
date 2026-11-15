
import pandas as pd
from pathlib import Path

# Read 100 transactions already delivered in Batch 2
source = Path("data/batch_02.csv")
target = Path("data/batch_06.csv")

df = pd.read_csv(source)

# Replay the first 50 transactions from Batch 2
replayed = df.head(50)

# Add 50 genuinely new transactions from the Kaggle source
kaggle_path = Path.home() / "Downloads" / "archive (1)" / "fraudTrain.csv"

new_records = pd.read_csv(
    kaggle_path,
    usecols=df.columns.tolist(),
    skiprows=range(1, 10301),
    nrows=50
)

# Combine replayed and new transactions
batch_06 = pd.concat([replayed, new_records], ignore_index=True)

batch_06.to_csv(target, index=False)

print("Batch 6 records:", len(batch_06))
print("Unique IDs within Batch 6:", batch_06["trans_num"].nunique())
print("Replayed IDs from Batch 2:", len(
    set(batch_06["trans_num"]) & set(df["trans_num"])
))
print("Saved to:", target)
