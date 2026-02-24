import streamlit as st
import polars as pl
from io import BytesIO

st.set_page_config(page_title="Xử lý data kiểm date", layout="wide")
st.title("📊 Xử lý dữ liệu kiểm date")
st.write("Upload folder chứa các file Excel → xuất file tổng hợp")

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

def process_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    # Chỉ cast các cột cần thiết, không cast toàn bộ
    df = df.with_columns([
        pl.col(c).cast(pl.Float64, strict=False) for c in NUMERIC_COLS if c in df.columns
    ])

    df = df.with_columns(
        pl.sum_horizontal([pl.col(c) for c in NUMERIC_COLS if c in df.columns])
          .alias('Điều kiện lọc')
    )

    df = df.filter(pl.col('Điều kiện lọc') > 0)

    # Wrap Hình ảnh_1 sau khi filter để giảm số row phải xử lý
    if 'Hình ảnh_1' in df.columns:
        df = df.with_columns(
            pl.col('Hình ảnh_1').str.replace_all('^(.*)$', '"$1"')
        )

    # Chỉ giữ cột tồn tại trong df
    cols = [c for c in COLUMNS_KEEP if c in df.columns]
    return df.select(cols)


def read_one_file(file_bytes: bytes, file_name: str) -> pl.DataFrame | None:
    """Đọc tất cả sheet trong 1 file, trả về DataFrame gộp."""
    try:
        # Dùng calamine — nhanh hơn openpyxl ~3-5x
        # sheet_id=None đọc tất cả sheet cùng lúc → trả về dict
        sheets: dict[str, pl.DataFrame] = pl.read_excel(
            BytesIO(file_bytes),
            sheet_id=None,          # đọc tất cả sheet
            engine="calamine",      # engine nhanh nhất
            infer_schema_length=0,  # không mất thời gian infer, giữ Utf8
        )
    except Exception as e:
        st.error(f"Lỗi đọc file {file_name}: {e}")
        return None

    processed = []
    for sheet_name, df in sheets.items():
        try:
            df = process_dataframe(df)
            df = df.with_columns(pl.lit(file_name).alias("file_name"))
            processed.append(df)
        except Exception as e:
            st.warning(f"Bỏ qua sheet '{sheet_name}' trong {file_name}: {e}")

    if not processed:
        return None
    return pl.concat(processed, how="diagonal_relaxed")


def read_excel_files(uploaded_files) -> pl.DataFrame | None:
    # Đọc bytes trước (ngoài thread) để tránh race condition với StreamlitUploadedFile
    files_data = [(f.read(), f.name) for f in uploaded_files]

    from concurrent.futures import ThreadPoolExecutor

    def task(args):
        return read_one_file(*args)

    with ThreadPoolExecutor(max_workers=min(4, len(files_data))) as ex:
        results = list(ex.map(task, files_data))

    all_dfs = [df for df in results if df is not None]
    if not all_dfs:
        return None
    return pl.concat(all_dfs, how="diagonal_relaxed")


# ── UI ──────────────────────────────────────────────────────────────
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
            data_date = read_excel_files(uploaded_files)

            if data_date is None:
                st.error("Không đọc được dữ liệu")
                st.stop()

            output = BytesIO()
            # Viết thẳng từ polars, không qua pandas
            data_date.write_excel(output, autofit=True)
            output.seek(0)

            st.session_state.output_excel = output.getvalue()
            st.success(f"✅ Hoàn thành! {data_date.shape[0]:,} dòng")

if st.session_state.output_excel is not None:
    st.download_button(
        label="📥 Download file Excel",
        data=st.session_state.output_excel,
        file_name="data_kiem_date.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
