import pdfplumber
import pandas as pd
import re
from datetime import datetime
from pathlib import Path


def clean_number(value):
    if value is None or pd.isna(value):
        return None

    s = str(value).strip()

    if s in ["", "-", "$-"]:
        return None

    if "%" in s:
        return None

    s = s.replace("$", "").replace(",", "")

    try:
        return float(s)
    except:
        return None


def parse_month_year(filename):
    name = filename.stem
    parts = name.split("-")

    month = parts[0].title()
    year = int(parts[1])

    month_map = {
        "January": 1, "February": 2, "March": 3,
        "April": 4, "May": 5, "June": 6,
        "July": 7, "August": 8, "September": 9,
        "October": 10, "November": 11, "December": 12
    }

    return month, year, month_map[month]


def extract_pdf(pdf_path):
    results = {}

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.split("\n")
            rows = []

            for line in lines:
                parts = re.split(r"\s{2,}", line.strip())
                if len(parts) > 1:
                    rows.append(parts)

            if not rows:
                continue

            report_name = rows[0][0]
            raw_lines = [r[0] for r in rows[4:]]

            cleaned_rows = []
            for item in raw_lines:
                split = re.split(r"(\$.*)", item, maxsplit=1)
                row_name = split[0].strip()
                values = split[1].strip() if len(split) > 1 else ""
                cleaned_rows.append((row_name, values))

            df = pd.DataFrame(cleaned_rows, columns=["row_name", "raw_values"])

            if "total" in df["row_name"].str.lower().values:
                last_total = df[df["row_name"].str.lower() == "total"].index[-1]
                df = df.iloc[: last_total + 1]

            split_vals = df["raw_values"].str.split(" ", expand=True)

            df["current_month"] = None
            df["calendar_ytd"] = None
            df["fiscal_ytd"] = None

            for i in range(len(df)):
                vals = [clean_number(v) for v in split_vals.iloc[i].dropna()]
                vals = [v for v in vals if v is not None]

                if len(vals) == 3:
                    df.at[i, "current_month"] = vals[0]
                    df.at[i, "calendar_ytd"] = vals[1]
                    df.at[i, "fiscal_ytd"] = vals[2]

            df = df.drop(columns=["raw_values"])
            results[report_name] = df

    return results


def main():
    pdf_dir = Path("maryland_pdfs")
    pdf_files = sorted(pdf_dir.glob("*-*-CASINO-REVENUE-DATA.pdf"))

    all_rows = []
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for pdf_file in pdf_files:
        month, year, month_num = parse_month_year(pdf_file)

        tables = extract_pdf(pdf_file)

        for report, df in tables.items():
            for _, row in df.iterrows():
                all_rows.append([
                    run_date,
                    report,
                    year,
                    month,
                    month_num,
                    row["row_name"],
                    row["current_month"],
                    row["calendar_ytd"],
                    row["fiscal_ytd"],
                    "USD"
                ])

    final = pd.DataFrame(all_rows, columns=[
        "rundate", "report_name", "year", "month", "month_num",
        "row_name", "current_month", "calendar_ytd", "fiscal_ytd", "currency"
    ])

    final.to_csv("casino_revenue_all_months.csv", index=False)
    print("Saved: casino_revenue_all_months.csv")


if __name__ == "__main__":
    main()
