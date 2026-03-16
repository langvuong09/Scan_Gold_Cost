import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas.errors import EmptyDataError
from flask import Flask, render_template
import datetime
from concurrent.futures import ThreadPoolExecutor
import os

# Trang chính hiển thị giá vàng SJC hiện tại (hôm nay)
BASE_URL = "https://giavang.org/"

MAX_THREADS = 4

# Lịch sử giá vàng theo từng thương hiệu
BASE_HISTORY_URL = "https://giavang.org/trong-nuoc/{brand}/lich-su/{date}.html"
HISTORY_BRANDS = ["pnj"]  # Có thể thêm: "sjc", "doji", ...
START_HISTORY_YEAR = 2016

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}

app = Flask(__name__)


def get_dates():
    today = datetime.date.today()
    return [today.strftime("%d-%m-%Y")]


def crawl_date(date_str):
    # Trang chính không dùng tham số date, nhưng ta vẫn gắn date_str vào dữ liệu
    url = BASE_URL
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"[{date_str}] HTTP {r.status_code}")
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # Chọn đúng bảng có header "Khu vực | Hệ thống | Mua vào | Bán ra"
        target_table = None
        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if not header_row:
                continue
            header_text = " ".join(
                cell.get_text(strip=True)
                for cell in header_row.find_all(["th", "td"])
            )
            if "Khu vực" in header_text and "Hệ thống" in header_text:
                target_table = table
                break

        if not target_table:
            print(f"[{date_str}] No matching table found")
            return []

        rows = target_table.find_all("tr")[1:]  # bỏ header
        data = []

        current_area = None

        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]

            # Dòng đầu của một khu vực: Khu vực | Hệ thống | Mua vào | Bán ra
            if len(cells) == 4:
                current_area = cells[0]
                area = current_area
                system = cells[1]
                buy = cells[2]
                sell = cells[3]
            # Các dòng tiếp theo trong cùng khu vực (do rowspan): Hệ thống | Mua vào | Bán ra
            elif len(cells) == 3:
                area = current_area
                system = cells[0]
                buy = cells[1]
                sell = cells[2]
            else:
                continue

            data.append({
                "date": date_str,
                "area": area,
                "system": system,
                "buy": buy,
                "sell": sell,
            })

        print(f"[{date_str}] rows: {len(data)}")
        return data

    except Exception as e:
        print(f"[{date_str}] error: {e}")
        return []


def crawl_all():
    dates = get_dates()
    all_data = []

    print(f"Total dates: {len(dates)}")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        for rows in ex.map(crawl_date, dates):
            if rows:
                all_data.extend(rows)

    df = pd.DataFrame(all_data)
    df.to_csv("gia_vang_data.csv", index=False)
    print("DONE:", len(df), "rows")


def daterange(start_date: datetime.date, end_date: datetime.date):
    current = start_date
    while current <= end_date:
        yield current
        current += datetime.timedelta(days=1)


def crawl_brand_day(brand: str, day: datetime.date):
    date_path = day.strftime("%Y-%m-%d")  # dạng 2025-02-03
    url = BASE_HISTORY_URL.format(brand=brand, date=date_path)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"[{date_path}][{brand}] HTTP {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")

        # Tìm bảng chính theo cấu trúc cột (ổn định hơn so với text heading)
        # Một số dòng có thể dùng <th> thay cho <td> hoặc thiếu 1 cột "Loại vàng".
        main_table = None
        for table in soup.find_all("table"):
            trs = table.find_all("tr")
            if len(trs) < 2:
                continue

            has_data_row = False
            for tr in trs[1:]:
                cell_count = len(tr.find_all(["th", "td"]))
                if cell_count >= 4:
                    has_data_row = True
                    break

            if has_data_row:
                main_table = table
                break

        if main_table is None:
            print(f"[{date_path}][{brand}] no main table")
            return []

        rows = main_table.find_all("tr")[1:]  # bỏ header
        data = []

        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
            if len(cols) < 4:
                continue

            # Thường là 5 cột: Khu vực | Loại vàng | Mua vào | Bán ra | Thời gian
            # Một số dòng có thể chỉ có 4 cột (thiếu "Loại vàng")
            if len(cols) >= 5:
                area = cols[0]
                gold_type = cols[1]
                buy = cols[2]
                sell = cols[3]
                time_str = cols[4]
            else:
                area = cols[0]
                gold_type = ""
                buy = cols[1]
                sell = cols[2]
                time_str = cols[3]

            # Bỏ qua các dòng "link" ở cuối bảng
            if area.startswith("http"):
                continue

            data.append(
                {
                    "date": day.strftime("%d-%m-%Y"),
                    "brand": brand.upper(),
                    "area": area,
                    "type": gold_type,
                    "buy": buy,
                    "sell": sell,
                    "time": time_str,
                }
            )

        print(f"[{date_path}][{brand}] rows: {len(data)}")
        return data

    except Exception as exc:
        print(f"[{date_path}][{brand}] error: {exc}")
        return []


def crawl_history_all_years():
    start_date = datetime.date(START_HISTORY_YEAR, 1, 1)
    end_date = datetime.date.today()

    all_rows = []

    print(
        f"Start crawling history for brands {HISTORY_BRANDS} "
        f"from {start_date} to {end_date}"
    )

    for day in daterange(start_date, end_date):
        for brand in HISTORY_BRANDS:
            rows = crawl_brand_day(brand, day)
            if rows:
                all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df.to_csv("gia_vang_history_all.csv", index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} rows to gia_vang_history_all.csv")


@app.route("/")
def index():
    today_path = "gia_vang_data.csv"
    history_path = "gia_vang_history_all.csv"

    # Bảng giá hôm nay
    if os.path.exists(today_path) and os.path.getsize(today_path) > 0:
        try:
            df_today = pd.read_csv(today_path)
            today_table = df_today.head(200).to_html(index=False)
        except EmptyDataError:
            today_table = "No data yet (today file is empty)"
    else:
        today_table = "No data yet"

    # Bảng lịch sử
    if os.path.exists(history_path) and os.path.getsize(history_path) > 0:
        try:
            df_hist = pd.read_csv(history_path)
            history_table = df_hist.head(200).to_html(index=False)
        except EmptyDataError:
            history_table = "No history data (file is empty)"
    else:
        history_table = "No history data"

    return render_template(
        "index.html",
        today_table=today_table,
        history_table=history_table,
    )


@app.route("/history")
def full_history():
    history_path = "gia_vang_history_all.csv"

    if os.path.exists(history_path) and os.path.getsize(history_path) > 0:
        try:
            df_hist = pd.read_csv(history_path)
            table = df_hist.to_html(index=False)
        except EmptyDataError:
            table = "No history data (file is empty)"
    else:
        table = "No history data"

    return render_template("history.html", table=table)


if __name__ == "__main__":
    if not os.path.exists("gia_vang_data.csv") or os.path.getsize("gia_vang_data.csv") == 0:
        print("Start crawling gold price data (today)...")
        crawl_all()

    if not os.path.exists("gia_vang_history_all.csv") or os.path.getsize("gia_vang_history_all.csv") == 0:
        print("Start crawling gold price history for all years...")
        crawl_history_all_years()

    app.run(debug=True)