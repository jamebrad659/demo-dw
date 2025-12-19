import os
import sys
import subprocess
import logging
from pathlib import Path

SCRIPTS = [
    "scripts/load_products.py",
    "scripts/load_customers.py",
    "scripts/load_orders.py",
    "scripts/load_marketing.py",
    "scripts/load_returns.py",
]

def run_one(script: str):
    logging.info("Running: %s", script)
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("FAILED: %s", script)
        logging.error("STDOUT:\n%s", result.stdout)
        logging.error("STDERR:\n%s", result.stderr)
        raise SystemExit(result.returncode)
    logging.info("OK: %s\n%s", script, result.stdout.strip())

def main():
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Safety checks (helps beginners)
    if not os.path.exists("data/raw"):
        logging.error("Missing folder: data/raw (did you generate/copy your raw files?)")
        raise SystemExit(1)

    # If DATABASE_URL is not set, scripts will fall back to local .env values.
    logging.info("DATABASE_URL set: %s", bool(os.getenv("DATABASE_URL")))

    for s in SCRIPTS:
        if not os.path.exists(s):
            logging.error("Missing script: %s", s)
            raise SystemExit(1)
        run_one(s)

    logging.info("✅ Pipeline complete.")

if __name__ == "__main__":
    main()
