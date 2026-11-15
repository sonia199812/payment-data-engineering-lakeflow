
import pandas as pd
import json
from pathlib import Path

source = Path("data/batch_05_source.csv")
target = Path("data/batch_05.json")

df = pd.read_csv(source)

with open(target, "w", encoding="utf-8") as file:
    for _, row in df.iterrows():
        record = {
            "trans_num": row["trans_num"],
            "trans_date_trans_time": row["trans_date_trans_time"],
            "transaction": {
                "amount": (
    "INVALID_AMOUNT"
    if _ == 0
    else float(row["amt"])
),
                "merchant": row["merchant"],
                "category": row["category"]
            },
            "payment_details": {
                "channel": "online",
                "currency": "USD"
            },
            "tags": ["payment", "online"],
            "is_fraud": int(row["is_fraud"])
        }

        file.write(json.dumps(record) + "\n")

print(f"JSON records created: {len(df)}")
print(f"Saved to: {target}")
