from pathlib import Path
import re
import pandas as pd
import numpy as np

IN_FILE = Path("datasets/intermediate/all_categories.csv")
OUT_FILE = Path("datasets/cleaned_data/products_clean.csv")

CSV_SEP = ","
CSV_ENCODING_OUT = "utf-8-sig"

NULL_TOKENS = {"", "none", "null", "nan", "n/a", "na", "-", ".."}


def read_csv_safely(fp: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1258"):
        try:
            return pd.read_csv(fp, encoding=enc, sep=CSV_SEP, low_memory=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(fp, encoding="utf-8", sep=CSV_SEP, errors="ignore", low_memory=False)


def normalize_nulls_obj(s: pd.Series) -> pd.Series:
    if s.dtype != "object":
        return s
    x = s.astype(str).str.strip()
    mask = x.str.lower().isin(NULL_TOKENS)
    out = s.copy()
    out[mask] = np.nan
    return out


def to_num(s: pd.Series):
    return pd.to_numeric(s, errors="coerce")


def parse_brand(text) -> tuple:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return (np.nan, np.nan)
    if not isinstance(text, str):
        text = str(text)
    m_id = re.search(r"\bid\s*:\s*([0-9]+)", text)
    m_name = re.search(r"\bname\s*:\s*(.+)$", text.strip())
    brand_id = int(m_id.group(1)) if m_id else np.nan
    brand_name = m_name.group(1).strip() if m_name else np.nan
    return (brand_id, brand_name)


def parse_kv_semicolon(text) -> dict:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return {}
    if not isinstance(text, str):
        text = str(text)
    out = {}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    for p in parts:
        if ":" not in p:
            continue
        k, v = p.split(":", 1)
        k = k.strip()
        v = v.strip()
        if v.lower() in {"true", "false"}:
            out[k] = (v.lower() == "true")
        else:
            out[k] = v
    return out


def parse_stock_item(text) -> dict:
    d = parse_kv_semicolon(text)
    out = {}
    for k, v in d.items():
        if isinstance(v, bool):
            out[k] = v
            continue
        if isinstance(v, str):
            vv = v.replace(",", "").strip()
            if re.fullmatch(r"-?\d+(\.\d+)?", vv):
                out[k] = float(vv) if "." in vv else int(vv)
            else:
                out[k] = v
        else:
            out[k] = v
    return out


def parse_stars(text) -> dict:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return {}
    if not isinstance(text, str):
        text = str(text)
    out = {}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    for p in parts:
        m = re.match(r"^(\d)\s*:\s*count_(\d+)\s*,\s*percent_(\d+)", p)
        if not m:
            continue
        star = int(m.group(1))
        out[f"star_{star}_count"] = int(m.group(2))
        out[f"star_{star}_percent"] = int(m.group(3))
    return out


def choose_best_row(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp["_rk_reviews"] = to_num(tmp["reviews_count"]).fillna(-1) if "reviews_count" in tmp.columns else -1
    tmp["_rk_sold"] = to_num(tmp["quantity_sold"]).fillna(-1) if "quantity_sold" in tmp.columns else -1
    tmp["_rk_av"] = to_num(tmp["availability"]).fillna(0) if "availability" in tmp.columns else 0
    tmp["_rk_price"] = to_num(tmp["price"]).fillna(-1) if "price" in tmp.columns else -1

    tmp = tmp.sort_values(
        ["id", "_rk_reviews", "_rk_sold", "_rk_av", "_rk_price"],
        ascending=[True, False, False, False, False],
    )
    tmp = tmp.drop_duplicates(subset=["id"], keep="first")
    tmp = tmp.drop(columns=["_rk_reviews", "_rk_sold", "_rk_av", "_rk_price"])
    return tmp


def add_badge_flags_fast(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    text = df[col].fillna("").astype(str)
    df[f"{col}_has_video_badge"] = text.str.contains(r"\bvideo_icon\b", regex=True).astype("boolean")
    df[f"{col}_has_authentic_badge"] = text.str.contains(r"\bauthentic_brand\b", regex=True).astype("boolean")
    df[f"{col}_has_variant_count_badge"] = text.str.contains(r"\bvariant_count\b", regex=True).astype("boolean")
    df[f"{col}_has_delivery_badge"] = text.str.contains(r"\bdelivery_info_badge\b", regex=True).astype("boolean")
    return df


def main():
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Not found: {IN_FILE.as_posix()}")

    df = read_csv_safely(IN_FILE)
    original_cols = list(df.columns)

    for c in df.columns:
        df[c] = normalize_nulls_obj(df[c])

    numeric_cols = [
        "id",
        "seller_product_id",
        "seller_id",
        "data_version",
        "day_ago_created",
        "benefits_count",
        "rating_average",
        "reviews_count",
        "productset_id",
        "price",
        "original_price",
        "discount",
        "discount_rate",
        "favourite_count",
        "quantity_sold",
        "product_reco_score",
        "availability",
        "shippable",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = to_num(df[c])

    for c in ["name", "category", "inventory_status", "inventory_type"]:
        if c in df.columns and df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()

    if "brand_or_author" in df.columns:
        parsed = df["brand_or_author"].apply(parse_brand)
        df["brand_id"] = parsed.apply(lambda x: x[0])
        df["brand_name"] = parsed.apply(lambda x: x[1])

    if "tracking_info_amplitude" in df.columns:
        kv = df["tracking_info_amplitude"].apply(parse_kv_semicolon)
        for k in ["is_authentic", "is_freeship_xtra", "is_top_brand", "is_hero"]:
            df[k] = pd.Series(kv.apply(lambda d: d.get(k, pd.NA)), dtype="boolean")

    if "stock_item" in df.columns:
        stock = df["stock_item"].apply(parse_stock_item)
        for k in ["qty", "min_sale_qty", "max_sale_qty"]:
            df[f"stock_{k}"] = stock.apply(lambda d: d.get(k, np.nan))
            df[f"stock_{k}"] = to_num(df[f"stock_{k}"])

    if "stars" in df.columns:
        stars = df["stars"].apply(parse_stars)
        for i in range(1, 6):
            df[f"star_{i}_count"] = stars.apply(lambda d: d.get(f"star_{i}_count", np.nan))
            df[f"star_{i}_percent"] = stars.apply(lambda d: d.get(f"star_{i}_percent", np.nan))
            df[f"star_{i}_count"] = to_num(df[f"star_{i}_count"])
            df[f"star_{i}_percent"] = to_num(df[f"star_{i}_percent"])

    df = add_badge_flags_fast(df, "badges_new")
    df = add_badge_flags_fast(df, "badges_v3")

    if "discount_rate" in df.columns:
        bad = df["discount_rate"].notna() & ((df["discount_rate"] < 0) | (df["discount_rate"] > 100))
        df.loc[bad, "discount_rate"] = np.nan

    if "rating_average" in df.columns:
        bad = df["rating_average"].notna() & ((df["rating_average"] < 0) | (df["rating_average"] > 5))
        df.loc[bad, "rating_average"] = np.nan

    if "id" in df.columns:
        df = choose_best_row(df).reset_index(drop=True)

    if "quantity_sold" in df.columns:
        df["log_quantity_sold"] = np.log1p(df["quantity_sold"])
        sold = df["quantity_sold"]
        best = pd.Series(pd.NA, index=df.index, dtype="boolean")

        sold_non_null = sold.dropna()
        if len(sold_non_null) > 0:
            thr = sold_non_null.quantile(0.8)
            mask = sold.notna()
            best[mask] = (sold[mask] >= thr).astype("boolean")

        df["best_seller"] = best

    new_cols = [c for c in df.columns if c not in original_cols]
    df = df[original_cols + new_cols]

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False, encoding=CSV_ENCODING_OUT, sep=CSV_SEP)


if __name__ == "__main__":
    main()