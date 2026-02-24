"""
Xử lý data kiểm date - Optimized for large datasets (Pure Polars, no DuckDB)
=============================================================================
Chiến lược:
  1. Polars LazyFrame + streaming=True  → lazy eval, không load thừa RAM
  2. Parquet staging                    → file trung gian nhỏ, nhanh
  3. ThreadPoolExecutor per-file        → I/O song song (phù hợp Streamlit Cloud)
  4. Filter trước khi collect           → chỉ giữ rows cần thiết trong RAM
  5. xlsxwriter constant_memory         → streaming write Excel, không OOM
"""

import streamlit as st
import polars as pl
import tempfile
import os
import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from openpyxl import load_workbook

# ============================= CONFIG =============================
st.set_page_config(page_title="Xử lý data kiểm date", layout="wide")
st.title("📊 Xử lý dữ liệu kiểm date")
st.write("Upload nhiều file Excel → xử lý → xuất file tổng hợp")

COLUMNS_KEEP = [
    'Mã siêu thị','Tên siêu thị','Mã sản phẩm','Tên sản phẩm',
    'SL chuyển kho','SL giảm giá','SL hủy tại siêu thị',
    'Số lượng trả NCC','SL đổi hàng NCC','Số lượng bình thường',
    'SL tặng KM','SL cận date (tặng quà)',
    'Ngày tạo','Lần kiểm cuối cùng',
    'Mã nhân viên','Họ và tên nhân viên',
    'Ngày duyệt','Người duyệt','Tên người duyệt',
    'Hình ảnh','Ghi chú trạng thái','Ghi chú',
    'Ngày hệ thống yêu cầu','Trạng thái','Nội dung',
    'Hạn sử dụng','Date gần nhất','Hình ảnh_1',
    'Phân loại','Thời gian bắt đầu','Thời gian kết thúc',
    'Giá trị phần trăm giảm giá'
]

FILTER_COLS = ['SL giảm giá', 'SL hủy tại siêu thị', 'SL tặng KM', 'SL cận date (tặng quà)']

STREAMING_THRESHOLD = 200_000   # dòng — trên mức này dùng streaming export


# ============================= CORE PROCESSING =============================

def process_sheet(file_bytes: bytes, sheet_name: str, file_name: str) -> pl.DataFrame | None:
    """
    Đọc 1 sheet → LazyFrame pipeline → collect 1 lần duy nhất.
    Không giữ data thừa trong RAM — filter xong mới collect.
    """
    try:
        # infer_schema_length=0 → đọc tất cả là Utf8, tránh lỗi mixed type
        lf = pl.read_excel(
            BytesIO(file_bytes),
            sheet_name=sheet_name,
            infer_schema_length=0,
        ).lazy()

        available = lf.columns

        # Cast các cột số (chỉ những cột tồn tại trong sheet)
        cast_exprs = [
            pl.col(c).cast(pl.Float64, strict=False)
            for c in FILTER_COLS if c in available
        ]

        # Tạo expr lọc: tổng các cột số > 0
        filter_cols_present = [c for c in FILTER_COLS if c in available]
        if not filter_cols_present:
            return None  # sheet không có cột lọc → bỏ qua

        filter_expr = sum(
            pl.col(c).fill_null(0)
            for c in filter_cols_present
        ) > 0

        # Select chỉ những cột cần giữ (có trong sheet)
        keep = [c for c in COLUMNS_KEEP if c in available]

        lf = (
            lf
            .with_columns(cast_exprs)
            .filter(filter_expr)        # lọc TRƯỚC khi collect → RAM nhỏ hơn nhiều
            .select(keep)
        )

        # Xử lý hình ảnh sau filter (ít rows hơn)
        if 'Hình ảnh_1' in keep:
            lf = lf.with_columns(
                pl.concat_str([
                    pl.lit('"'),
                    pl.col('Hình ảnh_1').fill_null(''),
                    pl.lit('"')
                ]).alias('Hình ảnh_1')
            )

        lf = lf.with_columns(pl.lit(file_name).alias("file_name"))

        # collect(streaming=True) → Polars tự chia chunk nội bộ, không bùng RAM
        df = lf.collect(streaming=True)
        return df if len(df) > 0 else None

    except Exception as e:
        return e   # trả về exception để xử lý bên ngoài


def process_file_to_parquet(file_bytes: bytes, file_name: str, staging_dir: str) -> tuple[list[str], list[str]]:
    """
    Xử lý toàn bộ sheets trong 1 file → lưu từng sheet ra Parquet staging.
    Trả về (parquet_paths, errors).
    """
    parquet_paths = []
    errors = []

    try:
        wb = load_workbook(BytesIO(file_bytes), read_only=True)
        sheet_names = wb.sheetnames
        wb.close()
    except Exception as e:
        return [], [f"Không mở được file '{file_name}': {e}"]

    for sheet_name in sheet_names:
        result = process_sheet(file_bytes, sheet_name, file_name)

        if result is None:
            continue  # sheet rỗng hoặc không có cột lọc
        elif isinstance(result, Exception):
            errors.append(f"Sheet '{sheet_name}' / '{file_name}': {result}")
            continue

        df: pl.DataFrame = result
        try:
            safe = "".join(c if c.isalnum() else "_" for c in f"{file_name}_{sheet_name}")
            path = os.path.join(staging_dir, f"{safe[:120]}.parquet")
            df.write_parquet(path, compression="snappy")
            parquet_paths.append(path)
        except Exception as e:
            errors.append(f"Ghi Parquet sheet '{sheet_name}': {e}")
        finally:
            del df
            gc.collect()

    return parquet_paths, errors


def read_all_files(uploaded_files, progress_bar, status_text) -> pl.DataFrame | None:
    """
    Pipeline chính:
      1. Lưu file bytes (giữ trong memory — Streamlit Cloud không cho ghi disk tự do)
      2. ThreadPool xử lý từng file song song
      3. Gộp kết quả bằng Polars scan_parquet (lazy)
    """
    staging_dir = tempfile.mkdtemp(prefix="kiem_date_")
    n = len(uploaded_files)
    all_parquet: list[str] = []
    all_errors: list[str] = []

    def handle_one(uf):
        file_bytes = uf.getvalue()   # bytes đọc 1 lần
        file_name  = uf.name
        paths, errs = process_file_to_parquet(file_bytes, file_name, staging_dir)
        del file_bytes               # giải phóng ngay
        gc.collect()
        return paths, errs, file_name

    # ThreadPool phù hợp Streamlit Cloud (single-process, I/O-bound)
    max_workers = min(4, n)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(handle_one, uf): uf.name for uf in uploaded_files}
        for future in as_completed(futures):
            completed += 1
            progress_bar.progress(completed / n * 0.80)
            try:
                paths, errs, fname = future.result()
                all_parquet.extend(paths)
                all_errors.extend(errs)
                status_text.text(f"✅ {completed}/{n}: {fname}")
            except Exception as e:
                all_errors.append(f"Worker lỗi: {e}")

    for err in all_errors:
        st.warning(f"⚠️ {err}")

    if not all_parquet:
        return None

    # Gộp tất cả Parquet bằng scan_parquet (lazy, columnar)
    status_text.text(f"🔀 Đang tổng hợp {len(all_parquet)} phần dữ liệu...")
    progress_bar.progress(0.88)

    try:
        df_final = (
            pl.scan_parquet(all_parquet)          # lazy scan — không load hết RAM
              .collect(streaming=True)             # streaming collect
        )
    except Exception as e:
        st.error(f"Lỗi khi gộp Parquet: {e}")
        return None

    progress_bar.progress(0.92)
    return df_final


def export_excel(df: pl.DataFrame, status_text) -> bytes:
    """
    Xuất Excel thông minh:
    - Nhỏ  (≤ threshold): Polars write_excel (nhanh, đẹp)
    - Lớn  (> threshold): xlsxwriter constant_memory (streaming, không OOM)
    """
    output = BytesIO()
    n_rows = len(df)

    if n_rows <= STREAMING_THRESHOLD:
        status_text.text(f"📤 Đang xuất {n_rows:,} dòng...")
        df.write_excel(output, autofit=True)
    else:
        import xlsxwriter
        status_text.text(f"📤 Streaming export {n_rows:,} dòng (constant_memory mode)...")

        workbook = xlsxwriter.Workbook(output, {
            'constant_memory': True,  # ghi từng row ra ngay, không buffer
            'in_memory': True,
        })
        worksheet = workbook.add_worksheet("Data")

        header_fmt = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
        })

        cols = df.columns
        for ci, col in enumerate(cols):
            worksheet.write(0, ci, col, header_fmt)

        # Chuyển từng batch 10k dòng — tránh OOM với pandas itertuples
        BATCH = 10_000
        for start in range(0, n_rows, BATCH):
            batch = df.slice(start, BATCH).to_pandas()
            for ri, row in enumerate(batch.itertuples(index=False), start=start + 1):
                for ci, val in enumerate(row):
                    if val is not None and val == val:   # bỏ NaN
                        worksheet.write(ri, ci, val)
            del batch
            gc.collect()
            if start % 50_000 == 0 and start > 0:
                status_text.text(f"📤 Đã ghi {start:,}/{n_rows:,} dòng...")

        workbook.close()

    output.seek(0)
    return output.getvalue()


# ============================= UI =============================

# Sidebar config
with st.sidebar:
    st.header("⚙️ Cấu hình")
    streaming_threshold = st.number_input(
        "Ngưỡng streaming export (dòng)",
        min_value=10_000, max_value=1_000_000,
        value=STREAMING_THRESHOLD, step=50_000,
        help="Trên ngưỡng này dùng streaming write để tiết kiệm RAM"
    )
    max_workers_cfg = st.slider(
        "Số luồng xử lý song song",
        min_value=1, max_value=8, value=4,
        help="Tăng nếu nhiều file nhỏ; giảm nếu ít RAM"
    )
    st.divider()
    st.markdown("""
**💡 Tips:**
- File lớn → giảm số luồng
- Nhiều sheet → Parquet staging tự động
- RAM thấp → giảm ngưỡng streaming
    """)

# File uploader
uploaded_files = st.file_uploader(
    "📂 Upload các file Excel",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if "output_excel" not in st.session_state:
    st.session_state.output_excel = None
if "row_count" not in st.session_state:
    st.session_state.row_count = 0

if uploaded_files:
    total_mb = sum(len(f.getvalue()) for f in uploaded_files) / 1024 / 1024
    st.info(f"📁 **{len(uploaded_files)} file** | Tổng: **{total_mb:.1f} MB**")
    if total_mb > 300:
        st.warning("⚠️ Data lớn — tự động dùng Parquet staging + streaming export")

    if st.button("🚀 Xử lý dữ liệu", type="primary", use_container_width=True):
        STREAMING_THRESHOLD = streaming_threshold

        start_time = time.perf_counter()
        progress_bar = st.progress(0)
        status_text  = st.empty()

        try:
            df_result = read_all_files(uploaded_files, progress_bar, status_text)

            if df_result is None:
                st.error("❌ Không có dữ liệu thỏa điều kiện lọc")
                st.stop()

            progress_bar.progress(0.94)
            output_bytes = export_excel(df_result, status_text)

            st.session_state.output_excel = output_bytes
            st.session_state.row_count    = len(df_result)

            elapsed = time.perf_counter() - start_time
            progress_bar.progress(1.0)
            status_text.text("✅ Hoàn thành!")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("⏱️ Thời gian",  f"{elapsed:.1f}s")
            c2.metric("📊 Tổng dòng",  f"{len(df_result):,}")
            c3.metric("📁 Files",       len(uploaded_files))
            c4.metric("💾 Output",      f"{len(output_bytes)/1024/1024:.1f} MB")

            del df_result
            gc.collect()

        except MemoryError:
            st.error("❌ Hết RAM! Thử giảm số luồng hoặc chia nhỏ file.")
        except Exception as e:
            st.error(f"❌ Lỗi: {e}")
            st.exception(e)

# Download button
if st.session_state.output_excel is not None:
    st.download_button(
        label=f"📥 Download Excel ({st.session_state.row_count:,} dòng)",
        data=st.session_state.output_excel,
        file_name="data_kiem_date.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )
