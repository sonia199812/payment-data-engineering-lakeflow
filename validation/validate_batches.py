
import json
from pathlib import Path

# Folder containing our transaction batches
data_folder = Path("data")

# Read the expected batch information
with open("config/delivery_manifest.json") as file:
    manifest = json.load(file)

missing_batches = []

# Check each expected file
for batch in manifest["batches"]:
    file_path = data_folder / batch["file_name"]

    if not file_path.is_file():
        missing_batches.append(batch["batch_id"])
        print(f"MISSING: {batch['batch_id']}")
    else:
        print(f"FOUND: {batch['batch_id']}")

# Fail validation if any expected file is missing
if missing_batches:
    raise SystemExit(
        f"VALIDATION FAILED: Missing batches: {missing_batches}"
    )

print("VALIDATION PASSED: All expected files arrived")
