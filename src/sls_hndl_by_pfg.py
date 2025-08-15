import argparse
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import pandas as pd

EXPECTED_KEYS = [
    "Location",
    "Postion/Title",
    "Primary Functional Group",
    "Secondary Functional Group",
    "Primary Specialization",
    "Secondary Specialization",
    "Shrt_Func",
]

def slugify(text: str) -> str:
    text = re.sub(r"\s+", "_", str(text).strip())
    text = re.sub(r"[^A-Za-z0-9_]+", "", text)
    return text or "PFG"

def list_pfg(df: pd.DataFrame) -> list[str]:
    col = "Primary Functional Group"
    if col not in df.columns:
        return []
    vals = df[col].dropna().astype(str).str.strip()
    vals = vals[vals != ""]
    return sorted(vals.unique())

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # Normalize typo variant for Position/Title -> Postion/Title
    if "Position/Title" in df.columns and "Postion/Title" not in df.columns:
        df = df.rename(columns={"Position/Title": "Postion/Title"})
    return df

def remove_header_like_rows(df: pd.DataFrame, key_cols_present: list[str]) -> pd.DataFrame:
    if not key_cols_present:
        return df.copy()
    def is_header_like(row):
        for c in key_cols_present:
            val = row.get(c)
            if pd.isna(val):
                continue
            if str(val).strip().lower() == c.strip().lower():
                return True
        return False
    mask = df.apply(is_header_like, axis=1)
    return df.loc[~mask].copy()

def build_key_from_row(row: pd.Series) -> str:
    parts = []
    for c in EXPECTED_KEYS:
        if c in row and pd.notna(row[c]):
            parts.append(str(row[c]).strip())
        else:
            parts.append("")
    return "_".join(parts)

def main():
    ap = argparse.ArgumentParser(
        description="Build unique SLS handler rows for ONE Primary Functional Group."
    )
    ap.add_argument("--input", "-i", required=True, help="Path to input .xlsx file (first sheet used unless --sheet is set).")
    ap.add_argument("--sheet", "-s", default=None, help="Sheet name to read (default: first sheet).")
    ap.add_argument("--pfg", "-g", required=True, help="Primary Functional Group to include (case-insensitive).")
    ap.add_argument("--output", "-o", default=None, help="Output path (.xlsx). Default: ./SLS_Hndl_<PFG>_<DDMMYYYY_HHMM>.xlsx")
    ap.add_argument("--list-pfg", action="store_true", help="List Primary Functional Groups from the input and exit.")
    args = ap.parse_args()

    # Read input
    try:
        xls = pd.ExcelFile(args.input)
        sheet_name = args.sheet or xls.sheet_names[0]
        df = pd.read_excel(args.input, sheet_name=sheet_name)
    except Exception as e:
        print(f"[ERROR] Failed to read input: {e}", file=sys.stderr)
        sys.exit(2)

    df = normalize_columns(df)

    # List PFGs if requested
    if args.list_pfg:
        values = list_pfg(df)
        if not values:
            print("(No 'Primary Functional Group' column found or it's empty.)")
        else:
            for v in values:
                print(v)
        sys.exit(0)

    # Validate PFG column
    if "Primary Functional Group" not in df.columns:
        print("[ERROR] Column 'Primary Functional Group' not found in the input.", file=sys.stderr)
        sys.exit(3)

    # Ensure the 7 key columns exist (create blanks if missing to avoid hard errors)
    for c in EXPECTED_KEYS:
        if c not in df.columns:
            df[c] = ""

    # Remove embedded header-like rows
    present_keys = [c for c in EXPECTED_KEYS if c in df.columns]
    df = remove_header_like_rows(df, present_keys)

    # Trim text in key columns
    for c in present_keys:
        df[c] = df[c].astype(str).str.strip().replace({"nan": ""})

    # Filter to one PFG (case-insensitive equality)
    target = str(args.pfg).strip().lower()
    mask = df["Primary Functional Group"].astype(str).str.strip().str.lower() == target
    df_pfg = df.loc[mask].copy()

    # Build KEY and drop duplicates by EXPECTED_KEYS
    df_pfg["KEY"] = df_pfg.apply(build_key_from_row, axis=1)
    df_unique = df_pfg.drop_duplicates(subset=EXPECTED_KEYS, keep="first")

    # Reorder columns with KEY first
    cols = ["KEY"] + [c for c in df_unique.columns if c != "KEY"]
    df_unique = df_unique[cols]

    # Output file path
    if args.output:
        out_path = args.output
    else:
        ts = datetime.now(ZoneInfo("America/New_York")).strftime("%d%m%Y_%H%M")
        out_path = f"SLS_Hndl_{slugify(args.pfg)}_{ts}.xlsx"

    # Write Excel with a Notes sheet
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            df_unique.to_excel(writer, index=False, sheet_name="SLS_Master_Unique")
            # Notes
            note_lines = [
                "SLS Handler — Unique per PFG (first occurrence)",
                f"Source file: {args.input}",
                f"Source sheet: {sheet_name}",
                f"PFG filter: {args.pfg}",
                f"Output file: {out_path}",
                "",
                "Key columns (order): " + ", ".join(EXPECTED_KEYS),
                "Rules:",
                " - Removed header-like rows in key columns.",
                " - Trimmed whitespace in key columns.",
                " - KEY is underscore-joined values of the 7 key columns.",
                " - Duplicates dropped by the 7-key combo; first occurrence kept.",
                " - Timezone for filename: America/New_York (DST-aware).",
            ]
            pd.DataFrame({"Notes": ["\n".join(note_lines)]}).to_excel(
                writer, index=False, header=False, sheet_name="Notes"
            )
    except Exception as e:
        print(f"[ERROR] Failed to write output: {e}", file=sys.stderr)
        sys.exit(4)

    print(f"[OK] Wrote: {out_path}  (rows: {len(df_unique)})")

if __name__ == "__main__":
    main()
