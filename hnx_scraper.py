"""
HNX Bond Trading Data Scraper  (API edition — không cần browser)
=================================================================
Gọi trực tiếp API JSON của HNX, nhanh hơn Playwright 10-20x.

Hỗ trợ 4 loại giao dịch:
  - outright       : Giao dịch Outright
  - repo_buy       : Giao dịch Mua Bán Lại
  - repo_sell      : Giao dịch Bán và Mua Lại
  - bond_lending   : Giao dịch Vay Trái Phiếu

Yêu cầu:
    pip install requests beautifulsoup4 lxml openpyxl

Chạy:
    python hnx_scraper.py                                          # Hôm nay, tất cả loại
    python hnx_scraper.py --date 03/04/2026                        # Ngày cụ thể
    python hnx_scraper.py --date 03/04/2026 -t outright repo_buy   # Nhiều loại
    python hnx_scraper.py --from-date 01/04/2026 --to-date 03/04/2026
    python hnx_scraper.py --month 03/2026
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────── API Config ──────────────────────────────────────

BASE_URL = "https://hnx.vn"
PAGE_REF = "https://hnx.vn/vi-vn/trai-phieu/ket-qua-gd-trong-ngay.html"

# Discovered from Chrome DevTools Network tab
API_ENDPOINTS = {
    "outright":     "/ModuleReportBonds/Bond_KQGD_TrongNgay/GetTradingOutRightInDay",
    "repo_buy":     "/ModuleReportBonds/Bond_KQGD_TrongNgay/GetTradingReposInDay",
    "repo_sell":    "/ModuleReportBonds/Bond_KQGD_TrongNgay/GetTradingSaleAndRepurchaseInDay",
    "bond_lending": "/ModuleReportBonds/Bond_KQGD_TrongNgay/GetTradingLoanInDay",
}

TRANSACTION_TYPES = {
    "outright":     {"label": "Giao dịch Outright"},
    "repo_buy":     {"label": "Giao dịch Mua Bán Lại"},
    "repo_sell":    {"label": "Giao dịch Bán và Mua Lại"},
    "bond_lending": {"label": "Giao dịch Vay Trái Phiếu"},
}

HEADERS = {
    "Accept":           "*/*",
    "Accept-Language":  "vi-VN,vi;q=0.9,en-GB;q=0.8,en;q=0.7",
    "Content-Type":     "application/x-www-form-urlencoded",
    "Origin":           BASE_URL,
    "Referer":          PAGE_REF,
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

RECORDS_PER_PAGE = 100   # max out page size to minimize round trips
MAX_WORKERS      = 4     # parallel days (be gentle to HNX server)

# ─────────────────────────── Session ─────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = False
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # Get a valid session cookie by visiting the page once
    try:
        s.get(PAGE_REF, timeout=15)
    except Exception:
        pass
    return s

# ─────────────────────────── HTML Parser ─────────────────────────────────────

def parse_html_table(html: str) -> list[dict]:
    """Parse the HTML table fragment returned by the API."""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True) for th in table.select("thead th, thead td")]
    rows = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True).replace("\xa0", "") for td in tr.find_all("td")]
        if not any(cells):
            continue
        if headers and len(cells) >= len(headers):
            rows.append(dict(zip(headers, cells[:len(headers)])))
        elif cells:
            rows.append({f"col_{i}": v for i, v in enumerate(cells)})
    return rows


def get_total_from_html(html: str) -> int:
    """Extract total record count from response HTML."""
    soup = BeautifulSoup(html, "lxml")
    # Look for "Tổng số X bản ghi" text
    import re
    text = soup.get_text()
    m = re.search(r"Tổng số\s+(\d+)\s+bản ghi", text)
    if m:
        return int(m.group(1))
    # Also check pagination elements
    for el in soup.select(".pagination, .pager, [class*=page]"):
        m = re.search(r"(\d+)", el.get_text())
        if m:
            pass  # not reliable; stick with text search
    return 0

# ─────────────────────────── Numeric parser ──────────────────────────────────

def try_numeric(val):
    """Parse Vietnamese number format: '3,9767'→3.9767, '500.000'→500000."""
    if not isinstance(val, str):
        return val
    v = val.strip()
    if not v:
        return v
    dots   = v.count(".")
    commas = v.count(",")

    if commas == 1 and dots == 0:          # "3,9767" decimal
        try: return float(v.replace(",", "."))
        except ValueError: pass
    elif commas == 0 and dots >= 1:        # "500.000" thousands
        try: return int(v.replace(".", ""))
        except ValueError: pass
    elif commas == 1 and dots >= 1:        # "1.700,50"
        try: return float(v.replace(".", "").replace(",", "."))
        except ValueError: pass
    elif commas == 0 and dots == 0:
        try: return int(v)
        except ValueError: pass
    return v

# ─────────────────────────── API Fetcher ─────────────────────────────────────

def fetch_one_page(session: requests.Session, tx_type: str,
                   trade_date: str, page: int, per_page: int = RECORDS_PER_PAGE) -> tuple[str, int]:
    """
    POST to HNX API for one page. Returns (html_fragment, total_records).
    trade_date format: dd/MM/yyyy  e.g. "03/04/2026"
    """
    url  = BASE_URL + API_ENDPOINTS[tx_type]
    data = {
        "p_keysearch":   trade_date + "|",
        "pColOrder":     "col_c",
        "pOrderType":    "ASC",
        "pCurrentPage":  str(page),
        "pRecordOnPage": str(per_page),
        "pIsSearch":     "1",
        "pIsChangeTab":  "0",
    }
    resp = session.post(url, data=data, timeout=20)
    resp.raise_for_status()
    html  = resp.text
    total = get_total_from_html(html)
    return html, total


def fetch_all_pages(session: requests.Session, tx_type: str,
                    trade_date: str) -> list[dict]:
    """Fetch all pages for one tx_type + date combination."""
    import math

    # Page 1
    try:
        html, total = fetch_one_page(session, tx_type, trade_date, page=1)
    except Exception as e:
        print(f"    ✗ Lỗi kết nối: {e}")
        return []

    rows = parse_html_table(html)
    if not rows:
        return []

    # If more pages exist, fetch them
    if total > RECORDS_PER_PAGE:
        n_pages = math.ceil(total / RECORDS_PER_PAGE)
        for p in range(2, n_pages + 1):
            try:
                html2, _ = fetch_one_page(session, tx_type, trade_date, page=p)
                rows.extend(parse_html_table(html2))
            except Exception as e:
                print(f"    ✗ Lỗi trang {p}: {e}")
                break

    # Filter: keep only rows where Ngày BĐGD == trade_date
    date_key = next(
        (k for k in (rows[0].keys() if rows else [])
         if any(x in k.lower() for x in ["bđgd", "bdgd", "ngày b"])),
        None
    )
    if date_key:
        before   = len(rows)
        rows     = [r for r in rows if str(r.get(date_key, "")).strip() == trade_date]
        filtered = before - len(rows)
        if filtered:
            print(f"    (Lọc bỏ {filtered} dòng không khớp ngày {trade_date})")

    return rows


def fetch_day(session: requests.Session, trade_date: str,
              types_to_fetch: list) -> dict:
    """Fetch all requested tx_types for one day."""
    results = {}
    for tx_type in types_to_fetch:
        label = TRANSACTION_TYPES[tx_type]["label"]
        rows  = fetch_all_pages(session, tx_type, trade_date)
        results[tx_type] = rows
        status = f"{len(rows)} bản ghi" if rows else "không có dữ liệu"
        print(f"  {label}: {status}")
    return results


def fetch_multi(trading_dates: list, types_to_fetch: list,
                max_workers: int = MAX_WORKERS) -> tuple[list, int, int]:
    """
    Fetch multiple days in parallel using a thread pool.
    Each thread gets its own session (session is not thread-safe).
    Returns (all_fetched, skipped_days, grand_total).
    """
    all_fetched  = []
    skipped_days = 0
    grand_total  = 0
    n = len(trading_dates)

    def fetch_one_day(d: date):
        trade_date = d.strftime("%d/%m/%Y")
        sess = make_session()
        results = fetch_day(sess, trade_date, types_to_fetch)
        return trade_date, results

    print(f"🚀 Chạy song song tối đa {max_workers} ngày cùng lúc...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(fetch_one_day, d): d for d in trading_dates}
        completed  = 0
        # Collect in submission order for sorted Excel output
        results_map = {}
        for future in as_completed(future_map):
            d = future_map[future]
            completed += 1
            try:
                trade_date, fetched = future.result()
                day_total = sum(len(v) for v in fetched.values())
                results_map[d] = (trade_date, fetched, day_total)
                print(f"  [{completed}/{n}] {trade_date} → {day_total} bản ghi")
            except Exception as e:
                results_map[d] = None
                print(f"  [{completed}/{n}] {d:%d/%m/%Y} → LỖI: {e}")

    # Sort by date and build output list
    for d in trading_dates:
        entry = results_map.get(d)
        if entry is None:
            skipped_days += 1
        else:
            trade_date, fetched, day_total = entry
            if day_total == 0:
                skipped_days += 1
            else:
                all_fetched.append((trade_date, fetched))
                grand_total += day_total

    return all_fetched, skipped_days, grand_total

# ─────────────────────────── Excel Export ────────────────────────────────────

NAVY    = "1F4E79"
BLUE    = "2E75B6"
LT_BLUE = "EBF3FB"

def _border():
    s = Side(style="thin", color="BFBFBF")
    return Border(left=s, right=s, top=s, bottom=s)

def _hdr(cell, text, bg=BLUE, fc="FFFFFF", bold=True, size=10):
    cell.value = text
    cell.font  = Font(name="Arial", bold=bold, size=size, color=fc)
    cell.fill  = PatternFill("solid", start_color=bg)
    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    cell.border = _border()

# Cache of real headers discovered from live data
_HEADER_CACHE: dict = {}

FALLBACK_HEADERS = {
    "outright":     ["STT", "Ngày giao kết giao dịch", "Ngày BĐGD", "Kỳ hạn còn lại",
                     "Mã TP", "Tiền tệ", "Giá yết (đồng)", "Lợi suất (%/năm)",
                     "KLGD", "GTGD (đồng)", "Ngày thanh toán"],
    "repo_buy":     ["STT", "Ngày giao kết giao dịch", "Ngày BĐGD", "Kỳ hạn còn lại",
                     "Mã TP", "Tiền tệ", "Giá yết mua lại (đồng)", "Lãi suất (%/năm)",
                     "KLGD", "GTGD (đồng)", "Ngày mua lại", "Ngày thanh toán"],
    "repo_sell":    ["STT", "Ngày giao kết giao dịch", "Ngày BĐGD", "Kỳ hạn còn lại",
                     "Mã TP", "Tiền tệ", "Giá yết bán lại (đồng)", "Lãi suất (%/năm)",
                     "KLGD", "GTGD (đồng)", "Ngày bán lại", "Ngày thanh toán"],
    "bond_lending": ["STT", "Ngày giao kết giao dịch", "Ngày BĐGD", "Kỳ hạn còn lại",
                     "Mã TP", "Tiền tệ", "Giá yết (đồng)", "Phí vay (%/năm)",
                     "KLGD", "GTGD (đồng)", "Ngày hoàn trả", "Ngày thanh toán"],
}


def get_or_create_sheet(wb: Workbook, sheet_name: str, tx_type: str,
                        keys: list) -> tuple:
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        return ws, ws.max_row + 1
    ws = wb.create_sheet(sheet_name)
    ws.row_dimensions[1].height = 28
    for ci, k in enumerate(keys, 1):
        _hdr(ws.cell(1, ci), k)
        ws.column_dimensions[get_column_letter(ci)].width = max(14, len(k) + 3)
    ws.freeze_panes = "A2"
    return ws, 2


def append_records(wb: Workbook, sheet_name: str, records: list[dict],
                   tx_type: str):
    if records:
        keys = list(records[0].keys())
        _HEADER_CACHE[tx_type] = keys
    else:
        keys = _HEADER_CACHE.get(tx_type) or FALLBACK_HEADERS.get(tx_type) or FALLBACK_HEADERS["outright"]

    ws, start_row = get_or_create_sheet(wb, sheet_name, tx_type, keys)
    for ri, rec in enumerate(records):
        r    = start_row + ri
        fill = PatternFill("solid", start_color=LT_BLUE if r % 2 == 0 else "FFFFFF")
        for ci, k in enumerate(keys, 1):
            cell = ws.cell(r, ci)
            val  = try_numeric(rec.get(k, ""))
            cell.value  = val
            cell.font   = Font(name="Arial", size=10)
            cell.fill   = fill
            cell.border = _border()
            if isinstance(val, (int, float)):
                cell.number_format = "#,##0.####"
                cell.alignment = Alignment(horizontal="right", vertical="center")
            else:
                cell.alignment = Alignment(horizontal="center", vertical="center")


def ensure_empty_sheets(wb: Workbook, types_to_fetch: list):
    for tx_type in types_to_fetch:
        info       = TRANSACTION_TYPES[tx_type]
        sheet_name = info["label"][:28]
        if sheet_name not in wb.sheetnames:
            keys = (_HEADER_CACHE.get(tx_type)
                    or FALLBACK_HEADERS.get(tx_type)
                    or FALLBACK_HEADERS["outright"])
            ws = wb.create_sheet(sheet_name)
            ws.row_dimensions[1].height = 28
            for ci, k in enumerate(keys, 1):
                _hdr(ws.cell(1, ci), k)
                ws.column_dimensions[get_column_letter(ci)].width = max(14, len(k) + 3)
            ws.freeze_panes = "A2"


def write_cover(wb: Workbook, date_range_label: str, summary: dict,
                types_to_fetch: list):
    if "Tổng quan" in wb.sheetnames:
        del wb["Tổng quan"]
    ws = wb.create_sheet("Tổng quan", 0)
    for col, w in zip("ABCD", [30, 28, 18, 14]):
        ws.column_dimensions[col].width = w

    ws.merge_cells("A1:D1")
    _hdr(ws["A1"], "KẾT QUẢ GIAO DỊCH TRÁI PHIẾU – HNX", bg=NAVY, size=13)
    ws.row_dimensions[1].height = 30

    ws.merge_cells("A2:D2")
    _hdr(ws["A2"],
         f"Kỳ: {date_range_label}  |  Tạo lúc: {datetime.now():%d/%m/%Y %H:%M}  |  Nguồn: hnx.vn",
         bg=BLUE, size=9, bold=False)
    ws.row_dimensions[2].height = 16

    ws.append([])
    for ci, h in enumerate(["Loại giao dịch", "Sheet", "Số ngày có GD", "Tổng bản ghi"], 1):
        _hdr(ws.cell(4, ci), h)
    ws.row_dimensions[4].height = 22

    for i, tx_type in enumerate(types_to_fetch):
        info = TRANSACTION_TYPES[tx_type]
        r    = 5 + i
        s    = summary.get(tx_type, {"days": 0, "records": 0})
        fill = PatternFill("solid", start_color=LT_BLUE if i % 2 == 0 else "FFFFFF")
        for ci, (val, align) in enumerate([
            (info["label"],      "left"),
            (info["label"][:28], "center"),
            (s["days"],          "right"),
            (s["records"],       "right"),
        ], 1):
            cell = ws.cell(r, ci)
            cell.value = val
            cell.font  = Font(name="Arial", size=10)
            cell.fill  = fill
            cell.border = _border()
            cell.alignment = Alignment(horizontal=align, vertical="center")
            if ci in (3, 4):
                cell.number_format = "#,##0"


def export_excel(all_fetched: list[tuple], output_path: str,
                 date_range_label: str, types_to_fetch: list):
    wb = Workbook()
    wb.remove(wb.active)

    summary = {tx: {"days": 0, "records": 0} for tx in types_to_fetch}

    for trade_date, fetched in all_fetched:
        for tx_type in types_to_fetch:
            records    = fetched.get(tx_type, [])
            sheet_name = TRANSACTION_TYPES[tx_type]["label"][:28]
            if records:
                append_records(wb, sheet_name, records, tx_type)
                summary[tx_type]["days"]    += 1
                summary[tx_type]["records"] += len(records)

    ensure_empty_sheets(wb, types_to_fetch)
    write_cover(wb, date_range_label, summary, types_to_fetch)

    # Reorder sheets: Tổng quan first
    order = ["Tổng quan"] + [TRANSACTION_TYPES[t]["label"][:28] for t in types_to_fetch]
    for i, name in enumerate(order):
        if name in wb.sheetnames:
            wb.move_sheet(name, offset=wb.sheetnames.index(name) - i)

    wb.save(output_path)
    print(f"\n✅ Đã lưu: {output_path}")

# ─────────────────────────── Date helpers ────────────────────────────────────

def parse_date(s: str) -> date:
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Không nhận ra định dạng ngày: {s!r}  (dùng dd/MM/yyyy)")


def date_range(start: date, end: date) -> list[date]:
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def dates_for_month(year: int, month: int) -> list[date]:
    import calendar
    _, last = calendar.monthrange(year, month)
    return date_range(date(year, month, 1), date(year, month, last))

# ─────────────────────────── CLI ─────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="HNX Bond Scraper — API edition (không cần browser)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    dg = p.add_mutually_exclusive_group()
    dg.add_argument("--date",      "-d", help="Một ngày: dd/MM/yyyy")
    dg.add_argument("--from-date", "-f", help="Từ ngày: dd/MM/yyyy (dùng với --to-date)")
    dg.add_argument("--month",     "-m", help="Cả tháng: MM/yyyy  (ví dụ: 03/2026)")

    p.add_argument("--to-date", "-e", help="Đến ngày: dd/MM/yyyy")
    p.add_argument("--type",    "-t",
                   nargs="+",
                   choices=["all"] + list(TRANSACTION_TYPES.keys()),
                   default=["all"],
                   metavar="TYPE",
                   help="Loại GD, chọn nhiều: -t outright repo_buy  |  mặc định: all")
    p.add_argument("--output",  "-o", default=None, help="Tên file Excel output")
    p.add_argument("--workers", "-w", type=int, default=MAX_WORKERS,
                   help=f"Số ngày chạy song song (mặc định: {MAX_WORKERS})")
    args = p.parse_args()

    # ── Resolve dates ──
    if args.month:
        try:
            m, y       = args.month.split("/")
            dates      = dates_for_month(int(y), int(m))
            label      = f"Tháng {args.month}"
            def_output = f"HNX_TraiPhieu_{args.month.replace('/', '')}.xlsx"
        except Exception:
            print("Lỗi: --month phải dạng MM/yyyy"); sys.exit(1)
    elif args.from_date:
        if not args.to_date:
            print("Lỗi: --from-date cần --to-date"); sys.exit(1)
        start = parse_date(args.from_date)
        end   = parse_date(args.to_date)
        if end < start:
            print("Lỗi: --to-date phải >= --from-date"); sys.exit(1)
        dates      = date_range(start, end)
        label      = f"{start:%d/%m/%Y} – {end:%d/%m/%Y}"
        def_output = f"HNX_TraiPhieu_{start:%d%m%Y}_{end:%d%m%Y}.xlsx"
    else:
        d          = parse_date(args.date) if args.date else date.today()
        dates      = [d]
        label      = f"{d:%d/%m/%Y}"
        def_output = f"HNX_TraiPhieu_{d:%d%m%Y}.xlsx"

    # ── Resolve types ──
    if "all" in args.type:
        types_to_fetch = list(TRANSACTION_TYPES.keys())
    else:
        valid = list(TRANSACTION_TYPES.keys())
        types_to_fetch = [t for t in valid if t in args.type]

    output = args.output or def_output

    print("=" * 65)
    print(f"  HNX Bond Scraper  (API — no browser)")
    print(f"  Kỳ      : {label}")
    print(f"  Số ngày : {len(dates)} ngày giao dịch")
    print(f"  Loại GD : {', '.join(types_to_fetch)}")
    print(f"  Workers : {args.workers}")
    print(f"  Output  : {output}")
    print("=" * 65)

    start_time = time.time()

    if len(dates) == 1:
        # Single day — no threading needed
        d          = dates[0]
        trade_date = d.strftime("%d/%m/%Y")
        print(f"\n[1/1] {trade_date}")
        sess     = make_session()
        fetched  = fetch_day(sess, trade_date, types_to_fetch)
        total    = sum(len(v) for v in fetched.values())
        if total == 0:
            print("\n⚠  Không có dữ liệu.")
            sys.exit(1)
        all_fetched  = [(trade_date, fetched)]
        skipped_days = 0
        grand_total  = total
    else:
        all_fetched, skipped_days, grand_total = fetch_multi(
            dates, types_to_fetch, max_workers=args.workers
        )

    if not all_fetched:
        print("\n⚠  Không tìm thấy dữ liệu cho toàn bộ kỳ.")
        sys.exit(1)

    export_excel(all_fetched, output, label, types_to_fetch)

    elapsed = time.time() - start_time
    print(f"\n📊 Tóm tắt:")
    print(f"   Kỳ dữ liệu  : {label}")
    print(f"   Ngày có GD  : {len(all_fetched)}/{len(dates)}")
    if skipped_days:
        print(f"   Ngày bỏ qua : {skipped_days}")
    print(f"   Tổng bản ghi: {grand_total:,}")
    print(f"   Thời gian   : {elapsed:.1f}s")
    print(f"   File        : {output}")


if __name__ == "__main__":
    main()