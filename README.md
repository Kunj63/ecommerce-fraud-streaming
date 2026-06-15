# Real-Time E-Commerce Fraud Detection — Spark Structured Streaming

**Course:** ENGR 5785G — Real-time Data Analytics for IoT
**Assignment:** Real-Time Stream Processing
**Scenario:** **C — E-Commerce Fraud Detection** (Session window, 30-min gap)

A Spark Structured Streaming pipeline that groups each user's clickstream into
**session windows** and raises a fraud alert when a user adds **more than 5 items
to cart within a single session but never completes a purchase**
(a classic bot / cart-abuse pattern).

---

## Scenario mapping (from the assignment)

| Requirement | Where it is implemented |
|---|---|
| `readStream` with a watched directory | `fraud_detection_stream.py` → `spark.readStream.format("csv").load("input_stream")` |
| ≥1 window aggregation **with `withWatermark`** | `withWatermark("event_time","10 minutes")` + `session_window(event_time,"30 minutes")` |
| ≥1 alert as a **filtered output stream** | `filter(num_cart > 5 AND num_purchase == 0)` written to the console sink |
| Alert content: user ID + session start time | columns `user_id`, `session_start` in the alert stream |

---

## Project layout

```
ecommerce-fraud-streaming/
├── fraud_detection_stream.py   # the Spark Structured Streaming job
├── stream_simulator.py         # drips the sample CSV into input_stream/ (simulated stream)
├── generate_demo_data.py       # creates data/sample_events.csv (demo data, seeded fraud)
├── requirements.txt
├── sample_console_output.txt   # captured alert output (reference for the screenshot)
├── data/
│   └── sample_events.csv        # generated input sample (Kaggle schema)
└── input_stream/                # created at runtime: the watched directory Spark reads
```

---

## Prerequisites

- **Java 11 / 17 / 21** (`java -version`)
- **Python 3.9+**
- Install deps:
  ```bash
  pip install -r requirements.txt
  ```

---

## How to run

Open **two terminals** in the project folder.

**1. Generate the sample data** (once):
```bash
python generate_demo_data.py
```
This writes `data/sample_events.csv` (~10k rows) and prints the user IDs it
deliberately seeded as fraud, so you can confirm the alerts match.

**2. Terminal A — start the Spark streaming job:**
```bash
python fraud_detection_stream.py
```
It begins watching `./input_stream`. Leave it running.

**3. Terminal B — start the stream simulator:**
```bash
python stream_simulator.py
```
This drips `data/sample_events.csv` into `input_stream/` in small, time-ordered
files (one micro-batch each). The last file is a far-future "flush" sentinel that
advances the watermark so every session closes and emits.

Within a few seconds of the flush file, the **fraud alerts appear in Terminal A**
(see `sample_console_output.txt`). Take your screenshot there.

> Single-terminal alternative: run `python stream_simulator.py` first to fully
> populate `input_stream/`, then run `python fraud_detection_stream.py`. The job
> uses `maxFilesPerTrigger=1`, so it still processes the files one batch at a time.

---

## Using the real Kaggle dataset (optional)

The job is schema-identical to the Kaggle **“eCommerce behavior data from multi
category store”** dataset, so no code change is needed:

1. Download the dataset and take a 10k–50k row sample
   (`event_time, event_type, product_id, category_id, category_code, brand,
   price, user_id, user_session`).
2. Save it as `data/sample_events.csv`.
3. Run `stream_simulator.py` then `fraud_detection_stream.py` exactly as above.

`generate_demo_data.py` exists only so the pipeline is runnable (and the alert is
reproducible) without the multi-GB download.

---

## Design explanation (required write-up)

### Why a **session window**?

Fraud/bot abuse is defined by *what a single user does inside one continuous
visit*, not by fixed clock intervals. A shopping visit naturally ends after a
period of inactivity — here, a **30-minute gap**. A **session window** models
exactly this: it dynamically opens when a user becomes active and closes after
30 minutes of silence, with each user getting their own independently-sized
window.

The alert condition (“>5 carts **and** no purchase **in the same visit**”) is a
per-session property, so the session window lets me evaluate it directly. A
**tumbling** or **sliding** window (fixed length) would cut visits at arbitrary
clock boundaries: a single abusive burst could be split across two windows
(hiding the count), or two unrelated visits could be merged into one window
(false alarm). Session windows avoid both problems by aligning the window to the
user's actual behavior.

### Where the pipeline requires **state**

The one stateful operator is the **session-window aggregation**
(`groupBy(session_window(...), user_id).agg(...)`). Spark keeps state in its
**state store**, keyed by `(user_id, session)`:

- For every open session it holds the running aggregates — `num_cart`,
  `num_purchase`, `num_events`, and the session's start/end — and **updates them
  across micro-batches** as new files (events) arrive.
- The **watermark** (`event_time`, 10 minutes) bounds this state. A session is
  finalized and emitted only once the watermark passes `session_end + gap`; its
  state is then evicted. Without the watermark, session state would accumulate
  forever (unbounded state).
- Because streaming **session windows only support *append* output mode**, a
  session's alert is produced exactly once, when the session closes. The
  simulator's far-future "flush" event is what advances the watermark past the
  final sessions so they close during the demo.

The downstream `filter` (the alert) is **stateless** — it simply passes through
the closed sessions that meet the fraud condition.
