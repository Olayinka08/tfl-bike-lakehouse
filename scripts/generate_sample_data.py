"""Generate a synthetic TfL-format journey CSV for tests, CI and a first dashboard.

Matches the current TfL usage-stats layout exactly (same headers, same date
format) and deliberately injects bad rows and duplicates so the quarantine and
de-duplication logic have something to catch.

Usage:
    python scripts/generate_sample_data.py --days 14 --trips-per-day 3000 --out data/sample
"""

import argparse
import csv
import math
import os
import random
from datetime import datetime, timedelta

HEADER = [
    "Number", "Start date", "Start station number", "Start station",
    "End date", "End station number", "End station", "Bike number",
    "Bike model", "Total duration", "Total duration (ms)",
]

# (terminal number, name, popularity weight, commuter hub? arrivals in AM)
STATIONS = [
    ("001001", "Waterloo Station 3, Waterloo", 9, "hub"),
    ("001002", "Waterloo Station 1, Waterloo", 8, "hub"),
    ("001003", "Hyde Park Corner, Hyde Park", 7, "leisure"),
    ("001004", "Black Lion Gate, Kensington Gardens", 5, "leisure"),
    ("001005", "Belgrove Street, King's Cross", 8, "hub"),
    ("001006", "Holborn Circus, Holborn", 6, "office"),
    ("001007", "Bank of England Museum, Bank", 6, "office"),
    ("001008", "Liverpool Street, Liverpool Street", 7, "hub"),
    ("001009", "Duke Street Hill, London Bridge", 6, "hub"),
    ("001010", "Wormwood Street, Liverpool Street", 5, "office"),
    ("001011", "Southwark Street, Bankside", 5, "office"),
    ("001012", "Albert Gate, Hyde Park", 5, "leisure"),
    ("001013", "Soho Square, Soho", 5, "office"),
    ("001014", "Craven Street, Strand", 4, "office"),
    ("001015", "Speakers' Corner 1, Hyde Park", 4, "leisure"),
    ("001016", "Queen's Gate, Kensington Gardens", 4, "leisure"),
    ("001017", "Brushfield Street, Liverpool Street", 4, "office"),
    ("001018", "Finsbury Circus, Liverpool Street", 4, "office"),
    ("001019", "Clapham Common North Side, Clapham Common", 3, "residential"),
    ("001020", "Wandsworth Road, Vauxhall", 3, "residential"),
    ("001021", "Peckham Road, Peckham", 2, "residential"),
    ("001022", "Kennington Lane Rail Bridge, Vauxhall", 3, "residential"),
    ("001023", "Shoreditch High Street, Shoreditch", 4, "residential"),
    ("001024", "Hoxton Street, Hoxton", 3, "residential"),
    ("001025", "Bethnal Green Road, Shoreditch", 3, "residential"),
    ("001026", "Islington Green, Angel", 3, "residential"),
    ("001027", "Battersea Park Road, Nine Elms", 3, "residential"),
    ("001028", "Embankment (Savoy), Strand", 4, "leisure"),
    ("001029", "Tower Gardens, Tower", 4, "leisure"),
    ("001030", "Canada Square, Canary Wharf", 5, "office"),
]

WEEKDAY_CURVE = [0.2, 0.1, 0.05, 0.05, 0.1, 0.5, 2.0, 5.5, 8.0, 4.5, 2.5, 2.8,
                 3.4, 3.3, 3.0, 3.4, 4.8, 7.8, 7.0, 4.5, 3.0, 2.2, 1.3, 0.6]
WEEKEND_CURVE = [0.6, 0.4, 0.25, 0.15, 0.1, 0.2, 0.4, 0.9, 1.8, 3.0, 4.5, 5.6,
                 6.2, 6.4, 6.3, 6.0, 5.4, 4.6, 3.6, 2.8, 2.1, 1.6, 1.2, 0.8]


def _pick_station(rng, hour, weekend, direction):
    weights = []
    for _, _, w, kind in STATIONS:
        factor = 1.0
        if not weekend:
            morning, evening = 6 <= hour <= 10, 16 <= hour <= 19
            if kind in ("hub", "residential"):
                factor = 1.8 if (morning and direction == "start") or (evening and direction == "end") else 0.8
            if kind == "office":
                factor = 1.8 if (morning and direction == "end") or (evening and direction == "start") else 0.8
        elif kind == "leisure":
            factor = 2.2
        weights.append(w * factor)
    return rng.choices(STATIONS, weights=weights, k=1)[0]


def generate(days: int, trips_per_day: int, start: datetime, seed: int = 42):
    rng = random.Random(seed)
    rows, rental_id = [], 140_000_000
    for d in range(days):
        day = start + timedelta(days=d)
        weekend = day.weekday() >= 5
        curve = WEEKEND_CURVE if weekend else WEEKDAY_CURVE
        volume = int(trips_per_day * (0.85 if weekend else 1.0) * rng.uniform(0.9, 1.1))
        hours = rng.choices(range(24), weights=curve, k=volume)
        for hour in hours:
            rental_id += 1
            begin = day.replace(hour=hour, minute=rng.randint(0, 59))
            src = _pick_station(rng, hour, weekend, "start")
            dst = src if rng.random() < (0.12 if weekend else 0.04) else _pick_station(rng, hour, weekend, "end")
            ebike = rng.random() < 0.18
            mean_min = (24 if weekend else 15) * (0.8 if ebike else 1.0)
            minutes = max(1.0, rng.lognormvariate(math.log(mean_min), 0.55))
            ms = int(minutes * 60_000)
            end = begin + timedelta(milliseconds=ms)
            rows.append([
                str(rental_id), begin.strftime("%Y-%m-%d %H:%M"), src[0], src[1],
                end.strftime("%Y-%m-%d %H:%M"), dst[0], dst[1], str(rng.randint(10_000, 60_000)),
                "PBSC_EBIKE" if ebike else "CLASSIC",
                f"{int(minutes)}m {int((minutes % 1) * 60)}s", str(ms),
            ])

    # ---- deliberate data quality problems (about 1 in 100 rows) ----
    bad = []
    for i in range(max(5, len(rows) // 400)):
        r = list(rng.choice(rows))
        r[0] = str(900_000_000 + i)
        problem = i % 4
        if problem == 0:
            r[1] = "not-a-date"                       # unparseable start
        elif problem == 1:
            r[10] = "-60000"                          # negative duration
        elif problem == 2:
            r[2] = ""                                 # missing start station
        else:
            r[10] = str(30 * 60 * 60 * 1000)          # 30 hours, over the 24h limit
        bad.append(r)
    dupes = [list(r) for r in rng.sample(rows, k=max(5, len(rows) // 500))]  # exact re-sends
    return rows + bad + dupes


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--trips-per-day", type=int, default=3000)
    p.add_argument("--start", default="2026-06-01")
    p.add_argument("--out", default="data/sample")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    start = datetime.strptime(a.start, "%Y-%m-%d")
    rows = generate(a.days, a.trips_per_day, start, a.seed)
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"SYNTHETIC_JourneyDataExtract{start:%d%b%Y}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"wrote {len(rows):,} rows to {path}")


if __name__ == "__main__":
    main()
