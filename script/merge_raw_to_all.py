from pathlib import Path
import pandas as pd

RAW_DIR = Path("datasets/raw_data")
OUT_FILE = Path("datasets/intermediate/all_categories.csv")

CSV_ENCODING = "utf-8-sig"
CSV_SEP = ","


def read_csv_safely(fp: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1258"):
        try:
            return pd.read_csv(fp, encoding=enc, sep=CSV_SEP, low_memory=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(fp, encoding="utf-8", sep=CSV_SEP, errors="ignore", low_memory=False)


def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Not found: {RAW_DIR.resolve()}")

    csv_files = sorted(
        fp for fp in RAW_DIR.glob("*.csv")
        if "all_categories" not in fp.name.lower()
    )
    if not csv_files:
        raise FileNotFoundError(f"No .csv files in: {RAW_DIR.resolve()}")

    frames = []
    base_cols = None
    extra_cols = []

    for i, fp in enumerate(csv_files):
        df = read_csv_safely(fp)

        if "category" not in df.columns:
            df["category"] = fp.stem
        if "source_file" not in df.columns:
            df["source_file"] = fp.name

        frames.append(df)

        if i == 0:
            base_cols = list(df.columns)
        else:
            for c in df.columns:
                if c not in base_cols and c not in extra_cols:
                    extra_cols.append(c)

    final_cols = base_cols + extra_cols
    frames = [f.reindex(columns=final_cols) for f in frames]
    all_df = pd.concat(frames, ignore_index=True, sort=False)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    all_df.to_csv(OUT_FILE, index=False, encoding=CSV_ENCODING, sep=CSV_SEP)

    print(f"Merged files: {len(csv_files)}")
    print(f"Output: {OUT_FILE.as_posix()}")
    print(f"Rows: {len(all_df)} Columns: {len(all_df.columns)}")


if __name__ == "__main__":
    main()