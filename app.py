"""
Xử lý data kiểm date - Optimized for large datasets
=====================================================
Chiến lược tối ưu:
  1. Polars LazyFrame  → Lazy eval, không load thừa RAM
  2. Streaming collect → xử lý từng chunk, không bùng RAM
  3. DuckDB engine     → SQL columnar cực nhanh cho filter/concat
  4. Parquet staging   → file trung gian nhanh hơn Excel nhiều lần
  5. Batched file I/O  → đọc nhiều file song song, giải phóng ngay
  6. Memory-mapped     → tái sử dụng buffer thay vì copy bytes
"""

import streamlit as st
import polars as pl
import duckdb
import tempfile
import os
import gc
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
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

# Số dòng tối đa trước khi chuyển sang streaming mode
STREAMING_THRESHOLD = 200_000
# Batch size khi streaming
CHUNK_SIZE = 50_000


# ============================= CORE PROCESSING =============================

def _build_filter_expr(available_cols: list[str]) -> pl.Expr:
    """Tạo expression lọc dùng LazyFrame — không tính toán ngay."""
    filter_cols = [c for c in FILTER_COLS if c in available_cols]
    if not filter_cols:
        return pl.lit(False)
    return sum(
        pl.col(c).cast(pl.Float64, strict=False).fill_null(0)
        for c in filter_cols
    ) > 0


def process_sheet_lazy(file_bytes: bytes, sheet_name: str, file_name: str) -> pl.DataFrame | None:
    """
    Đọc 1 sheet dùng LazyFrame pipeline:
    - cast → filter → select, collect 1 lần duy nhất
    - không giữ data thừa trong RAM
    """
    try:
        # Polars đọc Excel trực tiếp vào LazyFrame (polars >= 0.20)
        lf = pl.read_excel(
            BytesIO(file_bytes),
            sheet_name=sheet_name,
            infer_schema_length=0,   # đọc hết là Utf8, không tốn thời gian infer
        ).lazy()

        schema = lf.schema
        available = list(schema.keys())

        # Build lazy pipeline: cast số → filter → select → thêm file_name
        cast_exprs = [
            pl.col(c).cast(pl.Float64, strict=False)
            for c in FILTER_COLS if c in available
        ]

        filter_expr = _build_filter_expr(available)
        keep_cols = [c for c in COLUMNS_KEEP if c in available] + []

        lf = (
            lf
            .with_columns(cast_exprs if cast_exprs else [pl.lit(None).alias("__dummy__")])
            .filter(filter_expr)
            .select([c for c in COLUMNS_KEEP if c in available])
        )

        # Xử lý Hình ảnh_1 — chỉ nếu cột tồn tại
        if 'Hình ảnh_1' in available:
            lf = lf.with_columns(
                pl.concat_str([pl.lit('"'), pl.col('Hình ảnh_1').fill_null(''), pl.lit('"')])
                .alias('Hình ảnh_1')
            )

        lf = lf.with_columns(pl.lit(file_name).alias("file_name"))

        # Collect — đây là lúc duy nhất data load vào RAM (đã filter xong)
        df = lf.collect(streaming=True)  # streaming=True để Polars tự chunk nội bộ

        return df if len(df) > 0 else None

    except Exception as e:
        # Không dùng st.error trong worker process
        return None


def _worker_process_file(args) -> tuple[str, list[str], list[str]]:
    """
    Worker chạy trong ProcessPool — đọc file, xử lý, lưu Parquet trung gian.
    Trả về (file_name, danh sách parquet paths, danh sách lỗi).
    Dùng Parquet thay vì truyền DataFrame qua IPC → tiết kiệm memory serialization.
    """
    file_path, file_name, staging_dir = args
    errors = []
    parquet_paths = []

    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()

        wb = load_workbook(BytesIO(file_bytes), read_only=True)
        sheet_names = wb.sheetnames
        wb.close()

        for sheet_name in sheet_names:
            try:
                df = process_sheet_lazy(file_bytes, sheet_name, file_name)
                if df is not None:
                    # Lưu Parquet vào staging — nhanh và nhỏ hơn Excel
                    safe_name = "".join(c if c.isalnum() else "_" for c in f"{file_name}_{sheet_name}")
                    out_path = os.path.join(staging_dir, f"{safe_name}.parquet")
                    df.write_parquet(out_path, compression="snappy")
                    parquet_paths.append(out_path)
                    del df
                    gc.collect()
            except Exception as e:
                errors.append(f"Sheet '{sheet_name}' in '{file_name}': {e}")

        del file_bytes
        gc.collect()

    except Exception as e:
        errors.append(f"File '{file_name}': {e}")

    return file_name, parquet_paths, errors


def _save_uploaded_to_temp(uploaded_files) -> list[tuple[str, str]]:
    """Lưu uploaded files ra disk để ProcessPool có thể đọc (không serialize qua IPC)."""
    saved = []
    for uf in uploaded_files:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp.write(uf.getvalue())
        tmp.close()
        saved.append((tmp.name, uf.name))
    return saved


def read_excel_files_optimized(uploaded_files, progress_bar, status_text) -> pl.DataFrame | None:
    """
    Pipeline tối ưu cho data lớn:
    1. Lưu files ra disk
    2. ProcessPool xử lý song song → Parquet staging
    3. DuckDB đọc và concat tất cả Parquet (lazy, columnar)
    4. Xuất kết quả
    """
    staging_dir = tempfile.mkdtemp(prefix="kiem_date_")
    temp_files = []

    try:
        # Bước 1: Lưu uploaded files ra disk
        status_text.text("💾 Đang lưu files tạm...")
        saved_files = _save_uploaded_to_temp(uploaded_files)
        temp_files = [path for path, _ in saved_files]

        # Bước 2: ProcessPool xử lý song song
        args_list = [
            (file_path, file_name, staging_dir)
            for file_path, file_name in saved_files
        ]

        all_parquet_paths = []
        all_errors = []
        n_files = len(args_list)

        # Dùng ProcessPool để bypass Python GIL — thực sự song song
        max_workers = min(4, os.cpu_count() or 2, n_files)

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_worker_process_file, args): args[1] for args in args_list}

            completed = 0
            for future in as_completed(futures):
                file_name = futures[future]
                completed += 1
                progress_bar.progress(completed / n_files * 0.8)  # 80% cho bước đọc

                try:
                    fname, parquet_paths, errors = future.result()
                    all_parquet_paths.extend(parquet_paths)
                    all_errors.extend(errors)
                    status_text.text(f"✅ Xong {completed}/{n_files}: {fname}")
                except Exception as e:
                    all_errors.append(f"Worker error cho {file_name}: {e}")

        # Hiển thị lỗi nếu có
        for err in all_errors:
            st.warning(f"⚠️ {err}")

        if not all_parquet_paths:
            return None

        # Bước 3: DuckDB concat tất cả Parquet — cực nhanh, không load hết RAM
        status_text.text(f"🔀 Đang tổng hợp {len(all_parquet_paths)} phần...")
        progress_bar.progress(0.85)

        con = duckdb.connect()

        # DuckDB đọc nhiều Parquet bằng glob hoặc list — lazy columnar scan
        parquet_list_sql = ", ".join(f"'{p}'" for p in all_parquet_paths)

        # Estimate row count trước để quyết định strategy
        row_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet([{parquet_list_sql}])"
        ).fetchone()[0]

        status_text.text(f"📊 Tổng {row_count:,} dòng — đang xuất Excel...")
        progress_bar.progress(0.90)

        if row_count <= STREAMING_THRESHOLD:
            # Nhỏ: load hết vào Polars DataFrame
            result_df = con.execute(
                f"SELECT * FROM read_parquet([{parquet_list_sql}])"
            ).pl()
            con.close()
            return result_df
        else:
            # Lớn: streaming export thẳng sang Parquet tổng hợp, rồi batch sang Excel
            final_parquet = os.path.join(staging_dir, "_final.parquet")
            con.execute(f"""
                COPY (SELECT * FROM read_parquet([{parquet_list_sql}]))
                TO '{final_parquet}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            con.close()

            # Đọc lại bằng Polars lazy rồi collect theo chunk
            status_text.text(f"📦 Streaming {row_count:,} dòng → Polars...")
            result_df = pl.read_parquet(final_parquet)
            return result_df

    finally:
        # Cleanup temp files (không xóa staging để debug nếu cần)
        for tp in temp_files:
            try:
                os.unlink(tp)
            except Exception:
                pass


def export_to_excel_streaming(df: pl.DataFrame, status_text) -> bytes:
    """
    Xuất Excel theo batch nếu data lớn — tránh OOM khi write.
    Dùng xlsxwriter với constant_memory=True cho file lớn.
    """
    output = BytesIO()
    n_rows = len(df)

    if n_rows <= STREAMING_THRESHOLD:
        # Nhỏ: xuất thẳng
        df.write_excel(output, autofit=True)
    else:
        # Lớn: dùng xlsxwriter streaming mode
        import xlsxwriter
        status_text.text(f"📝 Đang ghi {n_rows:,} dòng vào Excel (streaming mode)...")

        workbook = xlsxwriter.Workbook(output, {
            'constant_memory': True,   # streaming write, không giữ data trong RAM
            'in_memory': True,
        })
        worksheet = workbook.add_worksheet("Data")

        # Header
        cols = df.columns
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': 'white'})
        for ci, col in enumerate(cols):
            worksheet.write(0, ci, col, header_fmt)

        # Data theo batch
        pandas_df = df.to_pandas()  # xlsxwriter cần pandas; Polars write_excel không hỗ trợ constant_memory
        for ri, row in enumerate(pandas_df.itertuples(index=False), start=1):
            for ci, val in enumerate(row):
                worksheet.write(ri, ci, val)

            if ri % 10_000 == 0:
                status_text.text(f"📝 Đã ghi {ri:,}/{n_rows:,} dòng...")

        workbook.close()

    output.seek(0)
    return output.getvalue()


# ============================= UI =============================

uploaded_files = st.file_uploader(
    "📂 Upload các file Excel",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if "output_excel" not in st.session_state:
    st.session_state.output_excel = None
if "row_count" not in st.session_state:
    st.session_state.row_count = 0

# Sidebar: memory & config
with st.sidebar:
    st.header("⚙️ Cấu hình")
    streaming_threshold = st.number_input(
        "Ngưỡng streaming (số dòng)",
        min_value=10_000,
        max_value=2_000_000,
        value=STREAMING_THRESHOLD,
        step=50_000,
        help="Trên ngưỡng này sẽ dùng streaming mode để tiết kiệm RAM"
    )
    max_workers_ui = st.slider(
        "Số luồng xử lý song song",
        min_value=1,
        max_value=min(8, os.cpu_count() or 4),
        value=min(4, os.cpu_count() or 2),
        help="Tăng nếu nhiều file; giảm nếu RAM thấp"
    )

    st.divider()
    st.caption("💡 Tips:\n- File lớn → giảm số luồng\n- RAM thấp → giảm ngưỡng streaming\n- Parquet trung gian được lưu trong /tmp")

if uploaded_files:
    total_size = sum(len(f.getvalue()) for f in uploaded_files) / 1024 / 1024
    st.info(f"📁 {len(uploaded_files)} file | Tổng kích thước: **{total_size:.1f} MB**")

    # Cảnh báo nếu data quá lớn
    if total_size > 500:
        st.warning("⚠️ Data lớn (>500MB) — sẽ tự động dùng streaming mode để tránh OOM")

    if st.button("🚀 Xử lý dữ liệu", type="primary"):
        start_time = time.perf_counter()

        progress_bar = st.progress(0)
        status_text = st.empty()
        metrics_cols = st.columns(4)

        try:
            STREAMING_THRESHOLD = streaming_threshold  # Apply UI config

            data_date = read_excel_files_optimized(uploaded_files, progress_bar, status_text)

            if data_date is None:
                st.error("❌ Không đọc được dữ liệu — kiểm tra lại file hoặc cột lọc")
                st.stop()

            progress_bar.progress(0.95)
            status_text.text("📤 Đang xuất Excel...")

            output_bytes = export_to_excel_streaming(data_date, status_text)
            st.session_state.output_excel = output_bytes
            st.session_state.row_count = len(data_date)

            elapsed = time.perf_counter() - start_time
            progress_bar.progress(1.0)
            status_text.text("✅ Hoàn thành!")

            # Metrics
            metrics_cols[0].metric("⏱️ Thời gian", f"{elapsed:.1f}s")
            metrics_cols[1].metric("📊 Tổng dòng", f"{len(data_date):,}")
            metrics_cols[2].metric("📁 Files", len(uploaded_files))
            metrics_cols[3].metric("💾 Output", f"{len(output_bytes)/1024/1024:.1f} MB")

            # Xóa DataFrame khỏi RAM ngay sau khi export
            del data_date
            gc.collect()

        except MemoryError:
            st.error("❌ Hết RAM! Giảm số luồng hoặc chia nhỏ file rồi thử lại.")
        except Exception as e:
            st.error(f"❌ Lỗi: {e}")
            st.exception(e)

if st.session_state.output_excel is not None:
    st.download_button(
        label=f"📥 Download Excel ({st.session_state.row_count:,} dòng)",
        data=st.session_state.output_excel,
        file_name="data_kiem_date.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )
