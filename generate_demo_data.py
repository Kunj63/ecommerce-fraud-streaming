"""
generate_demo_data.py
Creates a demo sample (data/sample_events.csv) that matches the schema of the
Kaggle "eCommerce behavior data from multi category store" dataset, with a number
of fraud users deterministically seeded (>5 cart events and NO purchase inside a
single < 30-min session). Use this if you don't want to download the multi-GB
Kaggle file; the Spark job is identical for real data (same columns).
"""
import csv, random, datetime, os, uuid

random.seed(42)
os.makedirs("data", exist_ok=True)

BRANDS = ["acme", "globex", "umbrella", "initech", "hooli", "stark", "samsung", "apple"]
CATS = [
    (2053013555631882655, "electronics.smartphone"),
    (2053013553559896355, "electronics.audio.headphone"),
    (2053013554658804075, "computers.notebook"),
    (2053013557024391671, "apparel.shoes"),
    (2053013563810775923, "appliances.kitchen.refrigerators"),
]
BASE = datetime.datetime(2019, 11, 1, 8, 0, 0)
rows = []

def add(uid, sess, t, etype, price):
    cat_id, cat_code = random.choice(CATS)
    rows.append({
        "event_time": t.strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": etype,
        "product_id": random.randint(1000000, 5000000),
        "category_id": cat_id,
        "category_code": cat_code,
        "brand": random.choice(BRANDS),
        "price": round(price, 2),
        "user_id": uid,
        "user_session": sess,
    })

N_NORMAL, N_FRAUD = 1800, 30
uid = 500000000

# Normal users: views, 0-3 carts, often a purchase -> never trigger the alert
for _ in range(N_NORMAL):
    uid += 1
    sess = str(uuid.uuid4())
    t = BASE + datetime.timedelta(minutes=random.randint(0, 45))
    for _ in range(random.randint(1, 6)):
        t += datetime.timedelta(seconds=random.randint(5, 90))
        add(uid, sess, t, "view", random.uniform(10, 900))
    n_cart = random.randint(0, 3)
    for _ in range(n_cart):
        t += datetime.timedelta(seconds=random.randint(10, 120))
        add(uid, sess, t, "cart", random.uniform(10, 900))
    if n_cart > 0 and random.random() < 0.6:
        t += datetime.timedelta(seconds=random.randint(10, 120))
        add(uid, sess, t, "purchase", random.uniform(10, 900))

# Fraud users: 6-12 carts, NO purchase, all inside one < 30-min session -> alert
fraud_ids = []
for _ in range(N_FRAUD):
    uid += 1
    fraud_ids.append(uid)
    sess = str(uuid.uuid4())
    t = BASE + datetime.timedelta(minutes=random.randint(0, 45))
    for _ in range(random.randint(6, 12)):
        t += datetime.timedelta(seconds=random.randint(15, 90))  # stays within 30 min
        add(uid, sess, t, "cart", random.uniform(50, 1500))

rows.sort(key=lambda r: r["event_time"])
FIELDS = ["event_time","event_type","product_id","category_id",
          "category_code","brand","price","user_id","user_session"]
with open("data/sample_events.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)

print(f"Wrote {len(rows)} rows to data/sample_events.csv")
print(f"Seeded {N_FRAUD} fraud users (expect these in alerts): {fraud_ids}")
