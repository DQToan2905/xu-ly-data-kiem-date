import streamlit as st
import pandas as pd
import polars as pl
import io

st.set_page_config(page_title="Excel Column Keeper", layout="wide")

st.title("📊 Excel Column Keeper System")

# ======================
# UPLOAD FILE
# ======================

uploaded_file = st.file_uploader(
    "📂 Upload file Excel",
    type=["xlsx", "xls"]
)

output_name = st.text_input(
    "💾 Tên file kết quả",
    placeholder="ket_qua.xlsx"
)

selected_columns = []
columns = []

# ======================
# LOAD COLUMNS (ALL TEXT)
# ======================

if uploaded_file:

    try:
        uploaded_file.seek(0)

        # đọc preview — tất cả text
        df_preview = pd.read_excel(
            uploaded_file,
            nrows=5,
            dtype=str
        )

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
# EXECUTE
# ======================

run_button = st.button(
    "🚀 Xử lý",
    disabled=not (uploaded_file and output_name and selected_columns)
)

if run_button:

    try:
        with st.spinner("⏳ Đang xử lý dữ liệu..."):

            uploaded_file.seek(0)

            # đọc full dữ liệu
            df = pl.read_excel(uploaded_file)

            # ép tất cả thành TEXT
            df = df.with_columns(pl.all().cast(pl.Utf8))

            missing_cols = [
                c for c in selected_columns if c not in df.columns
            ]

            if missing_cols:
                st.error(f"❌ Cột không tồn tại: {missing_cols}")
                st.stop()

            df_selected = df.select(selected_columns)

            buffer = io.BytesIO()
            df_selected.write_excel(buffer)

            buffer.seek(0)

        st.success("✅ Hoàn thành!")

        st.download_button(
            label="📥 Tải file kết quả",
            data=buffer,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"❌ Lỗi xử lý: {e}")
