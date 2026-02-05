# merge_category_files_to_raw_data.py
# Input:  datasets/crawl_data/<category>/*.csv
# Output: datasets/raw_data/<category>.csv

from pathlib import Path
import pandas as pd

CRAWL_DIR = Path("datasets/crawl_data")
RAW_DIR = Path("datasets/raw_data")

def read_csv_safely(fp: Path) -> pd.DataFrame:
    # Thử vài encoding phổ biến (Windows + tiếng Việt)
    for enc in ("utf-8-sig", "utf-8", "cp1258"):
        try:
            return pd.read_csv(fp, encoding=enc)
        except UnicodeDecodeError:
            continue
    # fallback
    return pd.read_csv(fp, encoding="utf-8", errors="ignore")

def merge_one_category(category_dir: Path) -> pd.DataFrame:
    csv_files = sorted(category_dir.glob("*.csv"))
    if not csv_files:
        return pd.DataFrame()

    frames = []
    category = category_dir.name

    for fp in csv_files:
        df = read_csv_safely(fp)
        df["category"] = category
        df["source_file"] = fp.name  # hoặc str(fp) nếu muốn full path
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True, sort=False)
    return merged

def main():
    if not CRAWL_DIR.exists():
        raise FileNotFoundError(f"Không thấy folder: {CRAWL_DIR.resolve()}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Mỗi folder con trong crawl_data là 1 category
    category_dirs = [p for p in CRAWL_DIR.iterdir() if p.is_dir()]
    if not category_dirs:
        raise FileNotFoundError(f"Không thấy category folder nào trong: {CRAWL_DIR.resolve()}")

    total_rows = 0
    merged_count = 0

    for category_dir in sorted(category_dirs):
        merged_df = merge_one_category(category_dir)
        if merged_df.empty:
            print(f"⚠️  Skip (no csv): {category_dir.name}")
            continue

        out_fp = RAW_DIR / f"{category_dir.name}.csv"
        merged_df.to_csv(out_fp, index=False, encoding="utf-8-sig")
        merged_count += 1
        total_rows += len(merged_df)

        print(f"✅ {category_dir.name}: {len(merged_df):,} rows -> {out_fp.as_posix()}")

    print("\n===== DONE =====")
    print(f"✅ Categories merged: {merged_count}")
    print(f"✅ Total rows written: {total_rows:,}")
    print(f"📁 Output folder: {RAW_DIR.resolve()}")

if __name__ == "__main__":
    main()
