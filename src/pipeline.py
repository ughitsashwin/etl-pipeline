# src/pipeline.py
#
# WHAT THIS FILE DOES:
# Orchestrates the full ETL pipeline for all configured cities.
# Now also sends a city comparison summary to Slack on every
# successful run, so you get proactive visibility into your data
# — not just alerts when things break.

import sys
import os
import pandas as pd
from src.extract import fetch_weather
from src.transform import transform_weather
from src.load import load_weather, get_engine, create_tables
from src.validate import validate
from sqlalchemy import text
import urllib.request
import json


CITIES = [
    {"name": "Dublin",    "lat": 53.33,   "lon": -6.25},
    {"name": "London",    "lat": 51.51,   "lon": -0.13},
    {"name": "New York",  "lat": 40.71,   "lon": -74.01},
    {"name": "Chennai",   "lat": 13.0843, "lon": 80.2705},
    {"name": "Kozhikode", "lat": 11.2488, "lon": 75.7839},
]


def send_slack_message(message: str):
    """
    Send a message to Slack via the webhook URL.

    The webhook URL is read from the SLACK_WEBHOOK_URL environment variable.
    On your Mac: export SLACK_WEBHOOK_URL=https://hooks.slack.com/...
    In GitHub Actions: stored as a repository secret, injected automatically.

    We use urllib (built into Python) instead of requests so this function
    has zero extra dependencies.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    # If no webhook URL is set, skip silently.
    # This means the pipeline still works locally without Slack configured.
    if not webhook_url:
        print("  [Slack] No webhook URL set — skipping notification")
        return

    payload = json.dumps({"text": message}).encode("utf-8")

    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                print("  [Slack] Notification sent successfully")
            else:
                print(f"  [Slack] Unexpected status: {response.status}")
    except Exception as e:
        # Never let a Slack failure crash the pipeline
        print(f"  [Slack] Failed to send notification: {e}")


def build_summary_message(results: list, total_rows: int, failed_cities: list) -> str:
    """
    Build a formatted Slack message with a city comparison table.

    Args:
        results:      List of dicts with per-city stats
        total_rows:   Total rows loaded across all cities
        failed_cities: List of city names that failed

    Returns:
        A formatted string ready to send to Slack
    """
    ran_at = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    status = "Pipeline completed successfully" if not failed_cities else "Pipeline completed with errors"
    icon   = "✅" if not failed_cities else "⚠️"

    lines = [
        f"{icon} *{status}*",
        f"🕐 Ran at: {ran_at}",
        f"📊 Rows loaded: {total_rows} | Cities: {len(CITIES) - len(failed_cities)}/{len(CITIES)}",
        "",
        "🏙️ *City Summary (last 7 days):*",
        "```",
    ]

    # Build a fixed-width table so it lines up neatly in Slack
    # Slack renders text inside triple backticks as monospace
    header = f"{'City':<12} {'Avg Temp':>8} {'Total Rain':>11} {'Rainy Days':>11}"
    lines.append(header)
    lines.append("-" * len(header))

    for r in results:
        rainy_icon = "💧" if r["rainy_days"] > 0 else "☀️"
        row = (
            f"{r['city']:<12}"
            f"{r['avg_temp']:>7.1f}°C"
            f"{r['total_rain']:>10.1f}mm"
            f"  {rainy_icon} {r['rainy_days']} days"
        )
        lines.append(row)

    lines.append("```")

    if failed_cities:
        lines.append(f"❌ Failed cities: {', '.join(failed_cities)}")

    return "\n".join(lines)


def log_failure(engine, error_message: str):
    """Log a pipeline failure to the pipeline_runs table."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs (ran_at, rows_loaded, status, message)
            VALUES (:ran_at, :rows_loaded, :status, :message)
        """), {
            "ran_at":      str(pd.Timestamp.utcnow()),
            "rows_loaded": 0,
            "status":      "failed",
            "message":     error_message,
        })


def run() -> int:
    """
    Run the full ETL pipeline for all configured cities.
    Sends a summary to Slack on completion.

    Returns:
        Total number of rows loaded across all cities.
    """
    engine = get_engine()
    create_tables(engine)

    total_rows   = 0
    failed_cities = []
    city_results  = []

    for city in CITIES:
        city_name = city["name"]

        try:
            # EXTRACT
            print(f"[{city_name}] Extracting...")
            raw_df = fetch_weather(city_name, city["lat"], city["lon"])
            print(f"[{city_name}] Got {len(raw_df)} raw rows")

            # TRANSFORM
            print(f"[{city_name}] Transforming...")
            clean_df = transform_weather(raw_df)

            # VALIDATE
            print(f"[{city_name}] Validating...")
            clean_df = validate(clean_df)

            # LOAD
            print(f"[{city_name}] Loading...")
            rows = load_weather(clean_df)
            print(f"[{city_name}] Done — {rows} rows loaded")

            total_rows += rows

            # Collect stats for the Slack summary table
            city_results.append({
                "city":       city_name,
                "avg_temp":   clean_df["temp_avg"].mean(),
                "total_rain": clean_df["precip_mm"].sum(),
                "rainy_days": int(clean_df["is_rainy"].sum()),
            })

        except Exception as e:
            error_msg = f"{city_name} failed: {str(e)}"
            print(f"[{city_name}] ERROR — {error_msg}")
            failed_cities.append(city_name)
            log_failure(engine, error_msg)

    # Print summary to console
    print()
    print("=" * 40)
    print("Pipeline complete")
    print(f"  Cities succeeded : {len(CITIES) - len(failed_cities)}/{len(CITIES)}")
    print(f"  Total rows loaded: {total_rows}")
    if failed_cities:
        print(f"  Failed cities    : {', '.join(failed_cities)}")
    print("=" * 40)

    # Send Slack summary
    print()
    print("Sending Slack summary...")
    message = build_summary_message(city_results, total_rows, failed_cities)
    send_slack_message(message)

    return total_rows


if __name__ == "__main__":
    rows = run()
    sys.exit(0 if rows > 0 else 1)
