import streamlit as st
import pandas as pd
import polars as pl
import os
from pathlib import Path
import tempfile

st.set_page_config(page_title="Excel Column Keeper", layout="wide")

st.title("📊 Excel Column Keeper System")

# ======================
# INPUT MODE
# ======================

mode = st.radio(
    "Chọn cách nhập file",
    ["Upload file (khuyến nghị)", "Nhập đường dẫn file"]
)

input_path = None
uploaded_file = None

# ======================
# MODE 1 — UPLOAD
# ======================

if mode == "Upload file (khuyến nghị)":

    uploaded_file = st.file_uploader(
        "📂 Chọn file Excel",
        type=["xlsx", "xls"]
    )

    if uploaded_file is not None:

        # lưu file tạm
        temp_dir = tempfile.gettempdir()
        input_path = os.path.join(temp_dir, uploaded_file.name)

        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        st.success(f"✅ File đã upload: {uploaded_file.name}")


# ======================
# MODE 2 — PATH
# ======================

if mode == "Nhập đường dẫn file":

    raw_path = st.text_input("📂 Input Excel Path")

    if raw_path:
        raw_path = raw_path.strip().strip('"')

        file_path = Path(raw_path)

        if file_path.is_file():
            input_path = str(file_path)
            st.success("✅ Tìm thấy file")
        else:
            st.error("❌ File input không tồn tại")
            st.stop()


# ======================
# OUTPUT PATH
# ======================

output_path = st.text_input("💾 Output Excel Path")

def validate_output_path(path):
    if not path:
        return False
    folder = os.path.dirname(path)
    if folder == "":
        return False
    return os.path.isdir(folder)


# ======================
# LOAD COLUMNS
# ======================

selected_columns = []
columns = []

if input_path:

    try:
        df_preview = pd.read_excel(input_path, nrows=5)
        columns = list(df_preview.columns)

        st.success(f"📑 Tổng số cột: {len(columns)}")

        option = st.radio(
            "Phương án chọn cột",
            ["Nhập danh sách tên cột", "Chọn cột trực tiếp"]
        )

        if option == "Nhập danh sách tên cột":

            col_text = st.text_area(
                "Nhập tên cột (cách nhau bằng dấu phẩy)",
                placeholder="col1, col2, col3"
            )

            if col_text:
                selected_columns = [
                    c.strip() for c in col_text.split(",") if c.strip()
                ]

        else:

            selected_columns = st.multiselect(
                "Chọn cột muốn giữ",
                columns
            )

        st.write("📌 Cột đã chọn:", selected_columns)

    except Exception as e:
        st.error(f"❌ Lỗi đọc file: {e}")
        st.stop()


# ======================
# EXECUTE BUTTON
# ======================

run_button = st.button(
    "🚀 Xử lý và lưu file",
    disabled=not (input_path and output_path and selected_columns)
)

if run_button:

    if not validate_output_path(output_path):
        st.error("❌ Thư mục output không tồn tại")
        st.stop()

    try:
        with st.spinner("⏳ Đang xử lý dữ liệu..."):

            df = pl.read_excel(input_path)

            missing_cols = [
                c for c in selected_columns if c not in df.columns
            ]

            if missing_cols:
                st.error(f"❌ Cột không tồn tại: {missing_cols}")
                st.stop()

            df_selected = df.select(selected_columns)

            df_selected.write_excel(output_path)

        st.success("✅ Hoàn thành!")
        st.write(f"📁 File đã lưu tại: {output_path}")

    except Exception as e:
        st.error(f"❌ Lỗi xử lý: {e}")
