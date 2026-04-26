"""
Omnichannel Marketing — RAW DATA Generator
Simulates transactional data as-exported from source systems.
Rules:
  - No surrogate keys, no SCD2 columns (dbt will handle that)
  - creators has intentional duplicate creator_id rows (different follower_count + updated_at)
  - Referential integrity across tables
  - Realistic messiness: nulls, mixed-case, inconsistent formatting
"""

import random
from datetime import date, datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

random.seed(7)

OUT = Path("/home/claude/raw_data")
OUT.mkdir(exist_ok=True)

# ──────────────────────────────────────────
# MASTER LOOKUP DATA
# ──────────────────────────────────────────
PLATFORMS   = ["TikTok", "YouTube", "Instagram", "Facebook", "Shopee Live", "Lazada Live"]
CATEGORIES  = ["Beauty & Skincare", "Fashion", "Food & Beverage", "Technology",
                "Finance", "Health & Fitness", "Home & Living", "Gaming"]
REGIONS     = ["Hà Nội", "TP.HCM", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Bình Dương"]
ACQ_SOURCES = ["TikTok Ads", "Instagram Ads", "Google Ads", "Facebook Ads",
                "Organic Search", "Referral", "Shopee Ads", "Email Campaign"]
OBJECTIVES  = ["Awareness", "Conversion", "Retargeting", "Lead Generation"]
BUDGET_TYPE = ["Daily Budget", "Lifetime Budget"]
PAYMENT     = ["COD", "Credit Card", "MoMo", "ZaloPay", "VNPay", "Bank Transfer"]
GENDERS     = ["Male", "Female", None]          # None → simulates missing data
STATUSES    = ["active", "inactive", "pending"]

CREATOR_NAMES = [
    "Trinh Phạm", "Linh Nguyễn", "Bảo Trần", "Mai Lê", "Đức Hoàng",
    "Trang Vũ",   "Khánh Đỗ",   "Hương Bùi", "Nam Phan","Thu Đặng",
    "Minh Lý",    "Lan Cao",     "Tuấn Đinh", "Hoa Ngô", "Việt Trương",
]

def tier(f):
    if f >= 1_000_000: return "Mega"
    if f >= 500_000:   return "Macro"
    if f >= 100_000:   return "Mid-tier"
    if f >= 10_000:    return "Micro"
    return "Nano"

def rand_dt(start: date, end: date) -> datetime:
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, delta))
    return datetime(d.year, d.month, d.day,
                    random.randint(0,23), random.randint(0,59), random.randint(0,59))

# ──────────────────────────────────────────
# 1. CREATORS  (raw — with SCD2-testable duplicates)
# ──────────────────────────────────────────
def gen_creators(n_base=15):
    rows = []
    creator_ids = [f"CRE_{str(i+1).zfill(4)}" for i in range(n_base)]

    for i, cid in enumerate(creator_ids):
        followers_v1 = random.randint(8_000, 3_000_000)
        platform     = random.choice(PLATFORMS)
        category     = random.choice(CATEGORIES)
        country      = "Vietnam"
        updated_v1   = rand_dt(date(2024, 1, 1), date(2024, 4, 30))

        # Version 1 row
        rows.append({
            "creator_id":      cid,
            "name":            CREATOR_NAMES[i % len(CREATOR_NAMES)],
            "platform":        platform,
            "follower_count":  followers_v1,
            "category":        category,
            "engagement_rate": round(random.uniform(0.008, 0.13), 4),
            "country":         country,
            "tier":            tier(followers_v1),
            "status":          random.choice(STATUSES),
            "updated_at":      updated_v1,
        })

        # ~40% creators get a later update row (simulates re-export / CDC)
        if random.random() < 0.4:
            followers_v2 = int(followers_v1 * random.uniform(1.05, 2.8))
            rows.append({
                "creator_id":      cid,
                "name":            CREATOR_NAMES[i % len(CREATOR_NAMES)],
                "platform":        platform,           # same platform
                "follower_count":  followers_v2,       # ← changed
                "category":        category,
                "engagement_rate": round(random.uniform(0.008, 0.13), 4),
                "country":         country,
                "tier":            tier(followers_v2), # ← may change tier
                "status":          "active",
                "updated_at":      rand_dt(date(2024, 5, 1), date(2024, 12, 31)),
            })

    return pd.DataFrame(rows), creator_ids

# ──────────────────────────────────────────
# 2. CAMPAIGNS
# ──────────────────────────────────────────
def gen_campaigns(n=20):
    NAMES = [
        "Super Sale Tháng 1", "Tết 2024 Mega Campaign", "Valentine Special",
        "Flash Deal 3.3", "Mid-Year Clearance", "Awareness Q2",
        "Launch Skincare Line", "Gaming Creator Collab", "11.11 Siêu Sale",
        "12.12 Year-End", "Retarget Loyal Fans", "Finance Gen Z Drive",
        "Food & Bev Summer", "Home Living Q3", "Health Month October",
        "TikTok Brand Lift", "YouTube Long-form Series", "Shopee Live Boost",
        "Lead Gen Finance", "Back to School",
    ]
    rows = []
    for i in range(n):
        start = date(2024, 1, 1) + timedelta(days=random.randint(0, 300))
        end   = min(start + timedelta(days=random.randint(7, 60)), date(2024, 12, 31))
        rows.append({
            "campaign_id":        f"CMP_{str(i+1).zfill(4)}",
            "campaign_name":      NAMES[i % len(NAMES)],
            "source_platform":    random.choice(PLATFORMS),
            "marketing_objective":random.choice(OBJECTIVES),
            "budget_type":        random.choice(BUDGET_TYPE),
            "total_budget_vnd":   random.randint(5_000_000, 500_000_000),
            "target_audience":    random.choice(["Gen Z", "Millennials", "Gen X", "All Ages"]),
            "product_category":   random.choice(CATEGORIES),
            "start_date":         start,
            "end_date":           end,
            "created_at":         rand_dt(date(2023, 11, 1), start),
        })
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# 3. CUSTOMERS
# ──────────────────────────────────────────
def gen_customers(n=80):
    rows = []
    for i in range(n):
        rows.append({
            "customer_id":        f"CST_{str(i+1).zfill(6)}",
            "acquisition_source": random.choice(ACQ_SOURCES),
            "region":             random.choice(REGIONS),
            "age_group":          random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
            "gender":             random.choice(GENDERS),     # includes None (missing)
            "registered_at":      rand_dt(date(2023, 1, 1), date(2024, 12, 31)),
        })
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# 4. DATES  (spine — all days of 2024)
# ──────────────────────────────────────────
def gen_dates():
    rows = []
    d = date(2024, 1, 1)
    while d <= date(2024, 12, 31):
        rows.append({"date_day": d})
        d += timedelta(days=1)
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# 5. AD_PERFORMANCE  (daily grain, raw)
# ──────────────────────────────────────────
def gen_ad_performance(campaigns_df, creator_ids, n_rows=100):
    rows = []
    camp_list = campaigns_df.to_dict("records")

    for i in range(n_rows):
        camp = random.choice(camp_list)
        delta = (camp["end_date"] - camp["start_date"]).days
        perf_date = camp["start_date"] + timedelta(days=random.randint(0, max(delta, 0)))
        spend = round(random.uniform(500_000, 12_000_000), -3)
        imp   = int(spend / random.uniform(25_000, 100_000) * 1000)

        rows.append({
            "performance_id": f"PERF_{str(i+1).zfill(6)}",
            "campaign_id":    camp["campaign_id"],
            "creator_id":     random.choice(creator_ids),
            "date":           perf_date,
            "spend_vnd":      spend,
            "impressions":    imp,
            "clicks":         int(imp * random.uniform(0.005, 0.06)),
            "video_views":    int(imp * random.uniform(0.05, 0.55)),
            "reach":          int(imp * random.uniform(0.4, 0.95)),
            # raw system sometimes has these as strings — intentional messiness
            "platform":       camp["source_platform"],
            "loaded_at":      datetime.now().replace(microsecond=0),
        })
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# 6. CONVERSIONS  (order grain, raw)
# ──────────────────────────────────────────
def gen_conversions(campaigns_df, customers_df, creator_ids, n_rows=100):
    import math
    rows = []
    camp_list  = campaigns_df.to_dict("records")
    cust_ids   = customers_df["customer_id"].tolist()

    for i in range(n_rows):
        camp = random.choice([c for c in camp_list if c["marketing_objective"] in ("Conversion", "Retargeting")] or camp_list)
        delta = (camp["end_date"] - camp["start_date"]).days
        ts = rand_dt(camp["start_date"], camp["end_date"] if delta > 0 else camp["end_date"])
        amount = round(random.lognormvariate(math.log(350_000), 0.9), -3)
        amount = max(50_000, min(amount, 30_000_000))

        rows.append({
            "order_id":         f"ORD_{str(i+1).zfill(7)}",
            "customer_id":      random.choice(cust_ids),
            "campaign_id":      camp["campaign_id"],
            "creator_id":       random.choice(creator_ids),
            "order_amount_vnd": amount,
            "currency":         "VND",
            "payment_method":   random.choice(PAYMENT),
            # raw status codes as they come from the order system
            "order_status":     random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "RETURNED", "CANCELLED"]),
            "platform":         camp["source_platform"],
            "timestamp":        ts,
            "loaded_at":        datetime.now().replace(microsecond=0),
        })
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# 7. CREATOR_REVIEWS  (bonus — raw NPS/review dump)
# ──────────────────────────────────────────
def gen_creator_reviews(creator_ids, campaigns_df, n=60):
    camp_ids = campaigns_df["campaign_id"].tolist()
    SENTIMENTS = ["positive", "positive", "positive", "neutral", "negative"]
    COMMENTS = [
        "Creator rất chuyên nghiệp, video chất lượng cao.",
        "Tỷ lệ chuyển đổi tốt hơn mong đợi.",
        "Audience engagement rất tốt, comment tích cực.",
        "Nội dung phù hợp với thương hiệu, sẽ hợp tác lại.",
        "Hiệu quả trung bình, cần cải thiện CTA.",
        "Reach tốt nhưng conversion thấp hơn kỳ vọng.",
        "Creator không deliver đúng timeline.",
        "ROI âm trong tháng đầu, cần theo dõi thêm.",
        "Script cần chỉnh sửa nhiều, mất thêm thời gian.",
        "Video viral, vượt KPI 200%.",
    ]
    rows = []
    for i in range(n):
        rows.append({
            "review_id":    f"REV_{str(i+1).zfill(5)}",
            "creator_id":   random.choice(creator_ids),
            "campaign_id":  random.choice(camp_ids),
            "reviewer_role":random.choice(["Brand Manager", "Media Planner", "Campaign Lead"]),
            "rating":       random.randint(1, 5),
            "sentiment":    random.choice(SENTIMENTS),
            "comment":      random.choice(COMMENTS),
            "reviewed_at":  rand_dt(date(2024, 1, 1), date(2024, 12, 31)),
        })
    return pd.DataFrame(rows)

# ──────────────────────────────────────────
# SAVE PARQUET
# ──────────────────────────────────────────
def save(df: pd.DataFrame, name: str):
    path = OUT / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    mb = path.stat().st_size / 1024
    print(f"  ✓  {name:<30}  {len(df):>4} rows   {mb:>6.1f} KB   →  {path.name}")

def main():
    print("\n🏗  Generating RAW Omnichannel Data (transactional, unprocessed)\n")

    creators_df, creator_ids = gen_creators(15)
    campaigns_df  = gen_campaigns(20)
    customers_df  = gen_customers(80)
    dates_df      = gen_dates()
    ad_perf_df    = gen_ad_performance(campaigns_df, creator_ids, 100)
    conversions_df= gen_conversions(campaigns_df, customers_df, creator_ids, 100)
    reviews_df    = gen_creator_reviews(creator_ids, campaigns_df, 60)

    print("📁 Saving Parquet files:\n")
    save(creators_df,    "creators")
    save(campaigns_df,   "campaigns")
    save(customers_df,   "customers")
    save(dates_df,       "dates")
    save(ad_perf_df,     "ad_performance")
    save(conversions_df, "conversions")
    save(reviews_df,     "creator_reviews")

    # Quick SCD2 check
    dupes = creators_df[creators_df.duplicated("creator_id", keep=False)]
    print(f"\n🔁 SCD2 test rows (duplicate creator_id): {len(dupes)} rows across {dupes['creator_id'].nunique()} creators")
    print(dupes[["creator_id","name","follower_count","tier","updated_at"]].sort_values("creator_id").to_string(index=False))

if __name__ == "__main__":
    main()
