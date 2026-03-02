import streamlit as st
import pandas as pd
import polars as pl
import os

st.set_page_config(page_title="Excel Column Keeper", layout="wide")

st.title("📊 Excel Column Keeper System")

# ======================
# INPUT PATH
# ======================

input_path = st.text_input("📂 Input Excel Path")
output_path = st.text_input("💾 Output Excel Path")

selected_columns = []

# ======================
# LOAD FILE + GET COLUMNS
# ======================

if input_path:

    if not os.path.exists(input_path):
        st.error("❌ File input không tồn tại")
        st.stop()

    try:
        # đọc preview để lấy header
        df_preview = pd.read_excel(input_path, nrows=5)
        columns = list(df_preview.columns)

        st.success(f"✅ Đã đọc file — Tổng số cột: {len(columns)}")

        # ======================
        # OPTION SELECT
        # ======================

        st.subheader("Chọn phương án")

        option = st.radio(
            "Phương án chọn cột",
            ["Nhập danh sách tên cột", "Chọn cột trực tiếp"]
        )

        # OPTION 1
        if option == "Nhập danh sách tên cột":

            col_text = st.text_area(
                "Nhập tên cột (cách nhau bằng dấu phẩy)",
                placeholder="col1, col2, col3"
            )

            if col_text:
                selected_columns = [c.strip() for c in col_text.split(",") if c.strip()]

        # OPTION 2
        if option == "Chọn cột trực tiếp":

            selected_columns = st.multiselect(
                "Chọn cột muốn giữ",
                columns
            )

        st.write("📌 Cột đã chọn:", selected_columns)

    except Exception as e:
        st.error(f"❌ Lỗi đọc file: {e}")
        st.stop()


# ======================
# VALIDATION OUTPUT PATH
# ======================

def validate_output_path(path):
    folder = os.path.dirname(path)
    if folder == "":
        return False
    return os.path.isdir(folder)


# ======================
# EXECUTE BUTTON
# ======================

run_button = st.button(
    "🚀 Xử lý và lưu file",
    disabled=not (input_path and output_path and selected_columns)
)

if run_button:

    # kiểm tra output folder
    if not validate_output_path(output_path):
        st.error("❌ Thư mục output không tồn tại")
        st.stop()

    try:
        with st.spinner("⏳ Đang xử lý dữ liệu..."):

            # đọc bằng polars (nhanh hơn pandas)
            df = pl.read_excel(input_path)

            # kiểm tra cột tồn tại
            missing_cols = [c for c in selected_columns if c not in df.columns]

            if missing_cols:
                st.error(f"❌ Các cột không tồn tại: {missing_cols}")
                st.stop()

            # select cột
            df_selected = df.select(selected_columns)

            # ghi file excel
            df_selected.write_excel(output_path)

        st.success("✅ Hoàn thành!")
        st.write(f"📁 File đã lưu tại: {output_path}")

    except Exception as e:
        st.error(f"❌ Lỗi xử lý: {e}")
