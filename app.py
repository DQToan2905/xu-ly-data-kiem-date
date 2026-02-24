import streamlit as st
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import time

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

NUMERIC_COLS = ['SL giảm giá', 'SL hủy tại siêu thị', 'SL tặng KM', 'SL cận date (tặng quà)']

def process_dataframe(df: pl.DataFrame, file_name: str) -> pl.DataFrame | None:
    # Ép kiểu string toàn bộ 1 lần
    df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in df.columns])

    # Ép kiểu số + tính điều kiện lọc trong 1 lần with_columns
    numeric_exprs = [
        pl.col(col).cast(pl.Float64)
        for col in NUMERIC_COLS if col in df.columns
    ]
    if numeric_exprs:
        df = df.with_columns(numeric_exprs)

    # Tính tổng điều kiện lọc
    sum_expr = sum(
        pl.col(col).fill_null(0)
        for col in NUMERIC_COLS if col in df.columns
    )
    df = df.filter(sum_expr > 0)

    if df.is_empty():
        return None

    # Xử lý hình ảnh sau khi lọc (ít row hơn)
    if 'Hình ảnh_1' in df.columns:
        df = df.with_columns(
            pl.concat_str([pl.lit('"'), pl.col('Hình ảnh_1'), pl.lit('"')]).alias('Hình ảnh_1')
        )

    # Giữ cột cần thiết
    keep_cols = [c for c in COLUMNS_KEEP if c in df.columns]
    df = df.select(keep_cols)
    df = df.with_columns(pl.lit(file_name).alias("file_name"))
    return df


def read_one_sheet(file_bytes: bytes, sheet_name: str, file_name: str) -> pl.DataFrame | None:
    try:
        df = pl.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)
        return process_dataframe(df, file_name)
    except Exception as e:
        st.error(f"Lỗi sheet '{sheet_name}' trong file {file_name}: {e}")
        return None


def read_one_file(file) -> list[pl.DataFrame]:
    file_bytes = file.read()  # đọc 1 lần duy nhất
    file_name = file.name

    # Lấy sheet names không cần load_workbook
    import openpyxl
    wb = openpyxl.open(BytesIO(file_bytes), read_only=True, data_only=True)
    sheet_names = wb.sheetnames
    wb.close()

    results = []
    # Chỉ dùng thread nếu nhiều sheet (overhead không đáng với 1-2 sheet)
    if len(sheet_names) <= 2:
        for sn in sheet_names:
            df = read_one_sheet(file_bytes, sn, file_name)
            if df is not None:
                results.append(df)
    else:
        with ThreadPoolExecutor(max_workers=min(4, len(sheet_names))) as executor:
            futures = {executor.submit(read_one_sheet, file_bytes, sn, file_name): sn for sn in sheet_names}
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    results.append(df)
    return results


def read_excel_files(uploaded_files) -> pl.DataFrame | None:
    all_dfs = []

    # Xử lý các file song song
    with ThreadPoolExecutor(max_workers=min(4, len(uploaded_files))) as executor:
        futures = {executor.submit(read_one_file, f): f.name for f in uploaded_files}
        for future in as_completed(futures):
            try:
                dfs = future.result()
                all_dfs.extend(dfs)
            except Exception as e:
                st.error(f"Lỗi: {e}")

    if not all_dfs:
        return None
    return pl.concat(all_dfs, how="diagonal_relaxed")


uploaded_files = st.file_uploader(
    "📂 Upload các file Excel",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if "output_excel" not in st.session_state:
    st.session_state.output_excel = None

if uploaded_files:
    st.success(f"Đã upload {len(uploaded_files)} file")

    if st.button("🚀 Xử lý dữ liệu"):
        start_time = time.perf_counter()

        with st.spinner("Đang xử lý..."):
            data_date = read_excel_files(uploaded_files)

        if data_date is None:
            st.error("Không đọc được dữ liệu")
            st.stop()

        # Xuất Excel bằng polars trực tiếp (không qua pandas)
        output = BytesIO()
        data_date.write_excel(output)
        output.seek(0)
        st.session_state.output_excel = output.getvalue()

        elapsed = time.perf_counter() - start_time
        st.info(f"⏱️ Thời gian xử lý: {elapsed:.2f} giây | {len(data_date):,} dòng")
        st.success("✅ Hoàn thành!")

if st.session_state.output_excel is not None:
    st.download_button(
        label="📥 Download file Excel",
        data=st.session_state.output_excel,
        file_name="data_kiem_date.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
