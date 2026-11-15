"""Generate reproducible synthetic JSONL payment events from a config file."""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


def generate(config_path: Path, batch_number: int, record_count: int | None = None) -> Path:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    count = record_count if record_count is not None else config["normal_batch_size"]
    if count < 0 or batch_number < 1:
        raise ValueError("record count must be nonnegative and batch number positive")
    rng = random.Random(config["seed"] + batch_number)
    start = datetime.fromisoformat(config["start_timestamp_utc"].replace("Z", "+00:00"))
    statuses = list(config["status_weights"])
    weights = list(config["status_weights"].values())
    outdir = config_path.parent / config["output_directory"]
    outdir.mkdir(parents=True, exist_ok=True)
    output = outdir / f"payment_events_batch_{batch_number:02d}.jsonl"
    with output.open("w", encoding="utf-8") as stream:
        for index in range(count):
            event_number = (batch_number - 1) * 100000 + index + 1
            minor_units = rng.randint(config["amount_min_minor_units"], config["amount_max_minor_units"])
            amount = str(Decimal(minor_units) / Decimal(100))
            status = rng.choices(statuses, weights=weights, k=1)[0]
            event = {
                "event_id": f"EVT{event_number:08d}",
                "payment_id": f"PAY{event_number:08d}",
                "event_timestamp": (start + timedelta(minutes=index, days=batch_number - 1)).isoformat().replace("+00:00", "Z"),
                "amount": amount,
                "currency": config["currency"],
                "status": status,
                "merchant": {"merchant_id": f"M{rng.randint(1, config['merchant_count']):03d}", "category": rng.choice(["Retail", "Food", "Travel", "Services"])},
                "items": [{"item_id": f"ITEM{event_number:08d}", "quantity": 1, "unit_price": amount}]
            }
            stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--count", type=int)
    args = parser.parse_args()
    print(generate(args.config, args.batch, args.count))
