import traceback
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import polars as pl
import streamlit as st
from openpyxl import load_workbook

st.set_page_config(page_title="Xử lý data kiểm date", layout="wide")
st.title("📊 Xử lý dữ liệu kiểm date")
st.write("Upload folder chứa các file Excel → xuất file tổng hợp")

# =============================
# CONFIG
# =============================

COLUMNS_KEEP = [
    'Mã siêu thị', 'Tên siêu thị', 'Mã sản phẩm', 'Tên sản phẩm',
    'SL chuyển kho', 'SL giảm giá', 'SL hủy tại siêu thị',
    'Số lượng trả NCC', 'SL đổi hàng NCC', 'Số lượng bình thường',
    'SL tặng KM', 'SL cận date (tặng quà)',
    'Ngày tạo', 'Lần kiểm cuối cùng',
    'Mã nhân viên', 'Họ và tên nhân viên',
    'Ngày duyệt', 'Người duyệt', 'Tên người duyệt',
    'Hình ảnh', 'Ghi chú trạng thái', 'Ghi chú',
    'Ngày hệ thống yêu cầu', 'Trạng thái', 'Nội dung',
    'Hạn sử dụng', 'Date gần nhất', 'Hình ảnh_1',
    'Phân loại', 'Thời gian bắt đầu', 'Thời gian kết thúc',
    'Giá trị phần trăm giảm giá'
]

NUMERIC_COLS = [
    'SL giảm giá',
    'SL hủy tại siêu thị',
    'SL tặng KM',
    'SL cận date (tặng quà)'
]


# =============================
# PROCESS DATAFRAME
# =============================

def process_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    # Cast cột numeric (đã là Utf8) sang Float64
    cast_exprs = [
        pl.col(c).cast(pl.Float64, strict=False)
        for c in NUMERIC_COLS if c in df.columns
    ]
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    # Tính tổng các cột điều kiện
    sum_cols = [pl.col(c) for c in NUMERIC_COLS if c in df.columns]
    if sum_cols:
        df = df.with_columns(
            pl.sum_horizontal(sum_cols).alias('Điều kiện lọc')
        )
        df = df.filter(pl.col('Điều kiện lọc') > 0)
    else:
        return df.head(0)  # Không có cột nào thì trả về rỗng

    # Wrap Hình ảnh_1 sau filter để giảm số dòng xử lý
    if 'Hình ảnh_1' in df.columns:
        df = df.with_columns(
            pl.col('Hình ảnh_1').str.replace_all(r'^(.*)$', '"$1"')
        )

    # Chỉ lấy cột tồn tại
    cols = [c for c in COLUMNS_KEEP if c in df.columns]
    return df.select(cols)


# =============================
# ĐỌC 1 FILE (tất cả sheet)
# =============================

def read_one_file(file_bytes: bytes, file_name: str):
    try:
        # Lấy danh sách sheet bằng openpyxl (nhẹ, chỉ đọc metadata)
        wb = load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
        sheet_names = wb.sheetnames
        wb.close()
    except Exception:
        st.error(f"❌ Không mở được file: {file_name}\n{traceback.format_exc()}")
        return None

    processed = []

    for sheet_name in sheet_names:
        try:
            df = pl.read_excel(
                BytesIO(file_bytes),       # BytesIO mới cho mỗi lần đọc
                sheet_name=sheet_name,
                engine="openpyxl",
                infer_schema_length=0,     # Giữ tất cả cột dạng Utf8, không mất thời gian infer
                read_options={"header_row": 0},
            )

            df = process_dataframe(df)

            if df.height > 0:
                df = df.with_columns(pl.lit(file_name).alias("file_name"))
                processed.append(df)

        except Exception:
            st.warning(f"⚠️ Bỏ qua sheet '{sheet_name}' trong {file_name}:\n{traceback.format_exc()}")

    if not processed:
        return None

    return pl.concat(processed, how="diagonal_relaxed")


# =============================
# ĐỌC NHIỀU FILE (song song)
# =============================

def read_excel_files(uploaded_files):
    # Đọc bytes TRƯỚC khi vào thread để tránh race condition
    files_data = [(f.read(), f.name) for f in uploaded_files]

    def task(args):
        return read_one_file(*args)

    max_workers = min(4, len(files_data))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(task, files_data))

    all_dfs = [df for df in results if df is not None]

    if not all_dfs:
        return None

    return pl.concat(all_dfs, how="diagonal_relaxed")


# =============================
# UI
# =============================

uploaded_files = st.file_uploader(
    "📂 Upload các file Excel trong folder",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if "output_excel" not in st.session_state:
    st.session_state.output_excel = None

if uploaded_files:
    st.success(f"Đã upload {len(uploaded_files)} file")

    if st.button("🚀 Xử lý dữ liệu"):
        with st.spinner("Đang xử lý..."):
            try:
                data_date = read_excel_files(uploaded_files)

                if data_date is None:
                    st.error("Không đọc được dữ liệu từ các file đã upload.")
                    st.stop()

                output = BytesIO()

                # Dùng pandas + xlsxwriter (ổn định nhất)
                data_date.to_pandas().to_excel(
                    output,
                    index=False,
                    engine="xlsxwriter"
                )

                output.seek(0)
                st.session_state.output_excel = output.getvalue()
                st.success(f"✅ Hoàn thành! Tổng {data_date.height:,} dòng")

            except Exception:
                st.error(f"Lỗi trong quá trình xử lý:\n{traceback.format_exc()}")


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
