"""
stream_simulator.py
Simulates a real-time stream by dripping the sample CSV into a *watched directory*
(input_stream/) in small, time-ordered chunks. Spark's readStream(format="csv")
picks up each new file as a micro-batch.

Files are written to a .tmp name first and then atomically renamed, so Spark never
reads a half-written file.

A final "flush" file with a far-future timestamp is written last. This advances the
event-time watermark past every real session so that all in-progress session windows
close and emit (required because streaming session windows only support append mode).

Env vars:
  CHUNK    rows per file   (default 800)
  INTERVAL seconds between files (default 2)
"""
import csv, os, time, shutil

SRC = "data/sample_events.csv"
OUT = "input_stream"
CHUNK = int(os.environ.get("CHUNK", "800"))
INTERVAL = float(os.environ.get("INTERVAL", "2"))

if os.path.exists(OUT):
    shutil.rmtree(OUT)
os.makedirs(OUT, exist_ok=True)

with open(SRC) as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = list(reader)

print(f"Streaming {len(rows)} rows in chunks of {CHUNK} every {INTERVAL}s -> {OUT}/")

def write_file(name, data_rows):
    path = os.path.join(OUT, name)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(data_rows)
    os.rename(tmp, path)           # atomic -> Spark sees only complete files
    print(f"  wrote {name} ({len(data_rows)} rows)")

part = 0
for i in range(0, len(rows), CHUNK):
    part += 1
    write_file(f"events_part_{part:05d}.csv", rows[i:i + CHUNK])
    time.sleep(INTERVAL)

# Watermark flush sentinel (far future) so every session closes and emits.
flush_row = ["2019-11-02 00:00:00", "view", "0", "0", "flush", "flush", "0.0", "0", "flush-session"]
write_file("events_part_99999_flush.csv", [flush_row])
print("Done streaming all chunks (+ watermark flush sentinel).")
