import streamlit as st
import pandas as pd
import polars as pl
import os

st.set_page_config(page_title="Column Selector", layout="wide")

st.title("📊 Excel Column Keeper System")

# ======================
# INPUT PATH
# ======================

input_path = st.text_input("📂 Input Excel Path")
output_path = st.text_input("💾 Output Excel Path")

if input_path and os.path.exists(input_path):

    try:
        # Đọc file bằng pandas (lấy header nhanh)
        df_preview = pd.read_excel(input_path, nrows=5)
        columns = list(df_preview.columns)

        st.success(f"Đã đọc file. Tổng số cột: {len(columns)}")

        st.subheader("Chọn phương án")

        option = st.radio(
            "Phương án chọn cột",
            ["Nhập danh sách tên cột", "Chọn cột trực tiếp"]
        )

        selected_columns = []

        # ======================
        # OPTION 1: TEXT INPUT
        # ======================
        if option == "Nhập danh sách tên cột":
            col_text = st.text_area(
                "Nhập tên cột (cách nhau bằng dấu phẩy)",
                placeholder="col1, col2, col3"
            )

            if col_text:
                selected_columns = [c.strip() for c in col_text.split(",")]

        # ======================
        # OPTION 2: MULTISELECT
        # ======================
        if option == "Chọn cột trực tiếp":
            selected_columns = st.multiselect(
                "Chọn cột muốn giữ",
                columns
            )

        st.write("Cột đã chọn:", selected_columns)

        # ======================
        # PROCESS BUTTON
        # ======================
        if st.button("🚀 Xử lý và lưu file"):

            if not selected_columns:
                st.warning("Vui lòng chọn ít nhất 1 cột")
            elif not output_path:
                st.warning("Vui lòng nhập output path")
            else:

                with st.spinner("Đang xử lý dữ liệu..."):

                    # Dùng polars đọc nhanh hơn pandas
                    df = pl.read_excel(input_path)

                    # Giữ cột cần thiết
                    df_selected = df.select(selected_columns)

                    # Ghi ra Excel
                    df_selected.write_excel(output_path)

                st.success("✅ Hoàn thành!")
                st.write(f"File đã lưu tại: {output_path}")

    except Exception as e:
        st.error(f"Lỗi: {e}")

else:
    st.info("Vui lòng nhập đúng đường dẫn file Excel.")
