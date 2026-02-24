import streamlit as st
import polars as pl
from openpyxl import load_workbook
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

# =============================
# CONFIG UI
# =============================

st.set_page_config(page_title="Xử lý data kiểm date", layout="wide")

st.title("📊 Xử lý dữ liệu kiểm date")
st.write("Upload nhiều file Excel → xử lý → xuất file tổng hợp")

# =============================
# CONFIG COLUMNS
# =============================

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

# =============================
# PROCESS ONE DATAFRAME
# =============================

def process_dataframe(df: pl.DataFrame, file_name: str) -> pl.DataFrame:

    # ép kiểu string toàn bộ để tránh lỗi schema
    df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in df.columns])

    # xử lý hình ảnh
    if 'Hình ảnh_1' in df.columns:
        df = df.with_columns(
            pl.col('Hình ảnh_1').str.replace_all('^(.*)$', '"$1"')
        )

    # ép kiểu số
    numeric_cols = [
        'SL giảm giá',
        'SL hủy tại siêu thị',
        'SL tặng KM',
        'SL cận date (tặng quà)'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64))

    # tạo điều kiện lọc
    df = df.with_columns(
        (
            pl.col('SL giảm giá').fill_null(0) +
            pl.col('SL hủy tại siêu thị').fill_null(0) +
            pl.col('SL tặng KM').fill_null(0) +
            pl.col('SL cận date (tặng quà)').fill_null(0)
        ).alias('Điều kiện lọc')
    )

    # lọc dữ liệu
    df = df.filter(pl.col('Điều kiện lọc') > 0)

    # giữ các cột cần thiết (nếu tồn tại)
    keep_cols = [c for c in COLUMNS_KEEP if c in df.columns]
    df = df.select(keep_cols)

    # thêm tên file nguồn
    df = df.with_columns(pl.lit(file_name).alias("file_name"))

    return df


# =============================
# READ ONE FILE → RETURN LIST DF
# =============================

def read_one_file(file):

    file_bytes = file.getvalue()
    file_name = file.name

    excel_io = BytesIO(file_bytes)

    wb = load_workbook(excel_io, read_only=True)
    sheet_names = wb.sheetnames

    processed_dfs = []

    def handle_sheet(sheet_name):

        try:
            df = pl.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)
            df = process_dataframe(df, file_name)

            if df is not None and len(df) > 0:
                return df

        except Exception as e:
            st.error(f"Lỗi sheet {sheet_name} trong file {file_name}: {e}")

        return None

    max_workers = min(4, len(sheet_names))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(handle_sheet, sheet_names))

    for df in results:
        if df is not None:
            processed_dfs.append(df)

    return processed_dfs


# =============================
# READ MULTIPLE FILES
# =============================

def read_excel_files(uploaded_files):

    all_processed = []

    for file in uploaded_files:

        try:
            dfs = read_one_file(file)
            all_processed.extend(dfs)

        except Exception as e:
            st.error(f"Lỗi file {file.name}: {e}")

    if not all_processed:
        return None

    final = pl.concat(all_processed, how="diagonal_relaxed")

    return final


# =============================
# UPLOAD UI
# =============================

uploaded_files = st.file_uploader(
    "📂 Upload các file Excel",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

# session state lưu file output

if "output_excel" not in st.session_state:
    st.session_state.output_excel = None


# =============================
# PROCESS BUTTON
# =============================

if uploaded_files:

    st.success(f"Đã upload {len(uploaded_files)} file")

    if st.button("🚀 Xử lý dữ liệu"):

        with st.spinner("Đang xử lý..."):

            data_date = read_excel_files(uploaded_files)

            if data_date is None:
                st.error("Không đọc được dữ liệu")
                st.stop()

            output = BytesIO()

            # xuất excel
            data_date.to_pandas().to_excel(
                output,
                index=False,
                engine="xlsxwriter"
            )

            output.seek(0)

            st.session_state.output_excel = output.getvalue()

            st.success("✅ Hoàn thành!")


# =============================
# DOWNLOAD BUTTON
# =============================

if st.session_state.output_excel is not None:

    st.download_button(
        label="📥 Download file Excel",
        data=st.session_state.output_excel,
        file_name="data_kiem_date.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )