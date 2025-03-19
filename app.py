












# data_converter_app.py
import streamlit as st
import pandas as pd
import numpy as np
import io
from io import BytesIO

# Configure app settings
st.set_page_config(
    page_title="Data Converter & Cleaner Pro",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'raw_df' not in st.session_state:
    st.session_state.raw_df = None
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None

# Main app header
st.title("📁 Advanced Data Converter & Analytics Suite")
st.markdown("""
**Transform, Analyze, and Visualize Data with Ease**  
✅ Multi-format support | 🧩 Advanced Cleaning | 📊 Interactive Visualizations  
""")

# File upload section
uploaded_file = st.file_uploader(
    "Upload your file (CSV, Excel, JSON)",
    type=["csv", "xlsx", "xls", "json"],
    key="file_upload"
)

def load_data(file):
    """Load data from uploaded file"""
    try:
        if file.name.endswith('.csv'):
            return pd.read_csv(file)
        elif file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file)
        elif file.name.endswith('.json'):
            return pd.read_json(file)
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

# Process uploaded file
if uploaded_file:
    if st.session_state.raw_df is None:
        st.session_state.raw_df = load_data(uploaded_file)
    
    if st.session_state.raw_df is not None:
        st.subheader("🔍 Original Data Preview")
        with st.expander("View Raw Data", expanded=True):
            st.dataframe(st.session_state.raw_df.head(10), use_container_width=True)

        # ========== Data Processing Controls ==========
        st.sidebar.header("⚙️ Processing Pipeline")
        
        with st.sidebar.expander("🗂️ Column Operations", expanded=True):
            cols_to_keep = st.multiselect(
                "Select columns to keep:",
                options=st.session_state.raw_df.columns,
                default=list(st.session_state.raw_df.columns),
                help="Select columns to include in processed data"
            )
            
            rename_cols = {}
            for col in cols_to_keep:
                new_name = st.text_input(
                    f"Rename '{col}'",
                    value=col,
                    key=f"rename_{col}",
                    help="Leave blank to keep original name"
                )
                if new_name.strip() and new_name != col:
                    rename_cols[col] = new_name.strip()

        with st.sidebar.expander("🧼 Data Cleaning", expanded=True):
            na_action = st.radio(
                "Handle missing values:",
                ["Keep", "Drop rows", "Drop columns", "Fill values"],
                index=0,
                horizontal=True
            )
            
            if na_action == "Fill values":
                fill_strategy = st.selectbox(
                    "Filling strategy:",
                    ["Custom value", "Mean", "Median", "Mode", "Forward fill", "Backward fill"]
                )
                
                if fill_strategy == "Custom value":
                    fill_value = st.text_input("Enter fill value:")

            handle_duplicates = st.checkbox(
                "Remove duplicate rows",
                help="Remove rows with identical values in all columns"
            )

        with st.sidebar.expander("🔧 Transformations", expanded=True):
            st.subheader("Data Filters")
            filter_expr = st.text_input(
                "Filter rows (Pandas query syntax):",
                help="Example: 'Age > 30 & Income < 50000'"
            )
            
            st.subheader("Type Conversion")
            dtype_col = st.selectbox(
                "Select column for type conversion:",
                options=[""] + list(st.session_state.raw_df.columns)
            )
            if dtype_col:
                new_type = st.selectbox(
                    "Convert to:",
                    ["str", "int", "float", "datetime", "category"],
                    index=0
                )

            st.subheader("📅 Datetime Operations")
            dt_col = st.selectbox(
                "Select datetime column:",
                options=[""] + list(st.session_state.raw_df.columns)
            )
            if dt_col:
                dt_parts = st.multiselect(
                    "Extract date parts:",
                    ["Year", "Month", "Day", "Weekday", "Hour", "Minute"]
                )

            st.subheader("✂️ Text Operations")
            text_col = st.selectbox(
                "Select text column:",
                options=[""] + list(st.session_state.raw_df.columns)
            )
            if text_col:
                text_ops = st.multiselect(
                    "Text transformations:",
                    ["Lowercase", "Uppercase", "Title Case", "Trim Whitespace"]
                )

        # Process data when clicked
        if st.sidebar.button("🚀 Process Data", type="primary"):
            df = st.session_state.raw_df.copy()
            
            # Apply column operations
            df = df[cols_to_keep].rename(columns=rename_cols)
            
            # Handle missing values
            if na_action == "Drop rows":
                df = df.dropna()
            elif na_action == "Drop columns":
                df = df.dropna(axis=1)
            elif na_action == "Fill values":
                if fill_strategy == "Custom value":
                    df = df.fillna(fill_value)
                elif fill_strategy in ["Forward fill", "Backward fill"]:
                    df = df.ffill() if fill_strategy == "Forward fill" else df.bfill()
                else:
                    for col in df.select_dtypes(include=np.number):
                        if fill_strategy == "Mean":
                            df[col].fillna(df[col].mean(), inplace=True)
                        elif fill_strategy == "Median":
                            df[col].fillna(df[col].median(), inplace=True)
                        elif fill_strategy == "Mode":
                            df[col].fillna(df[col].mode()[0], inplace=True)
            
            # Remove duplicates
            if handle_duplicates:
                df = df.drop_duplicates()
            
            # Apply filtering
            if filter_expr:
                try:
                    df = df.query(filter_expr)
                except Exception as e:
                    st.error(f"Invalid filter expression: {str(e)}")
            
            # Data type conversion
            if dtype_col and new_type:
                try:
                    df[dtype_col] = df[dtype_col].astype(new_type)
                except Exception as e:
                    st.error(f"Conversion error: {str(e)}")

            # Datetime operations
            if dt_col and dt_parts:
                try:
                    df[dt_col] = pd.to_datetime(df[dt_col])
                    for part in dt_parts:
                        part_lower = part.lower()
                        df[f"{dt_col}_{part_lower}"] = getattr(df[dt_col].dt, part_lower)
                except Exception as e:
                    st.error(f"Datetime error: {str(e)}")

            # Text operations
            if text_col and text_ops:
                try:
                    if "Lowercase" in text_ops:
                        df[text_col] = df[text_col].str.lower()
                    if "Uppercase" in text_ops:
                        df[text_col] = df[text_col].str.upper()
                    if "Title Case" in text_ops:
                        df[text_col] = df[text_col].str.title()
                    if "Trim Whitespace" in text_ops:
                        df[text_col] = df[text_col].str.strip()
                except Exception as e:
                    st.error(f"Text processing error: {str(e)}")

            st.session_state.processed_df = df
            st.success("✅ Data processing completed!")

        # ========== Processed Data Section ==========
        if st.session_state.processed_df is not None:
            st.subheader("✨ Processed Data Preview")
            with st.expander("View Processed Data", expanded=True):
                st.dataframe(
                    st.session_state.processed_df.head(10),
                    use_container_width=True,
                    height=300
                )

            # Dataset statistics
            with st.expander("📈 Dataset Statistics"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Rows", len(st.session_state.processed_df))
                with col2:
                    st.metric("Total Columns", len(st.session_state.processed_df.columns))
                with col3:
                    st.metric("Missing Values", 
                             st.session_state.processed_df.isna().sum().sum())
                with col4:
                    st.metric("Duplicate Rows", 
                             st.session_state.processed_df.duplicated().sum())

            # ========== Advanced Analytics ==========
            st.subheader("📊 Interactive Analytics")
            
            # Visualization controls
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                x_axis = st.selectbox(
                    "X-axis column",
                    options=st.session_state.processed_df.columns
                )
            with col2:
                y_axis = st.selectbox(
                    "Y-axis column",
                    options=st.session_state.processed_df.columns
                )
            with col3:
                color_col = st.selectbox(
                    "Color by",
                    options=["None"] + list(st.session_state.processed_df.columns)
                )
            with col4:
                chart_type = st.selectbox(
                    "Chart type",
                    options=["Scatter", "Line", "Bar", "Histogram", "Box", "Pie", "Heatmap"]
                )
            
            # Additional controls
            col5, col6, col7 = st.columns(3)
            with col5:
                chart_title = st.text_input("Chart title", "Data Visualization")
            with col6:
                theme = st.selectbox(
                    "Theme",
                    options=["plotly", "plotly_white", "ggplot2", "seaborn", "none"]
                )
            with col7:
                hover_data = st.multiselect(
                    "Hover data",
                    options=st.session_state.processed_df.columns
                )

            # Generate visualization
            try:
                if chart_type == "Scatter":
                    fig = px.scatter(
                        st.session_state.processed_df,
                        x=x_axis,
                        y=y_axis,
                        color=None if color_col == "None" else color_col,
                        title=chart_title,
                        template=theme,
                        hover_data=hover_data
                    )
                elif chart_type == "Line":
                    fig = px.line(
                        st.session_state.processed_df,
                        x=x_axis,
                        y=y_axis,
                        color=None if color_col == "None" else color_col,
                        title=chart_title,
                        template=theme
                    )
                elif chart_type == "Bar":
                    fig = px.bar(
                        st.session_state.processed_df,
                        x=x_axis,
                        y=y_axis,
                        color=None if color_col == "None" else color_col,
                        title=chart_title,
                        template=theme
                    )
                elif chart_type == "Histogram":
                    fig = px.histogram(
                        st.session_state.processed_df,
                        x=x_axis,
                        color=None if color_col == "None" else color_col,
                        title=chart_title,
                        template=theme
                    )
                elif chart_type == "Box":
                    fig = px.box(
                        st.session_state.processed_df,
                        x=x_axis,
                        y=y_axis,
                        color=None if color_col == "None" else color_col,
                        title=chart_title,
                        template=theme
                    )
                elif chart_type == "Pie":
                    fig = px.pie(
                        st.session_state.processed_df,
                        names=x_axis,
                        values=y_axis,
                        title=chart_title,
                        template=theme
                    )
                elif chart_type == "Heatmap":
                    fig = px.density_heatmap(
                        st.session_state.processed_df,
                        x=x_axis,
                        y=y_axis,
                        title=chart_title,
                        template=theme
                    )
                
                st.plotly_chart(fig, use_container_width=True)

                # Download visualization
                if st.button("💾 Download Chart as PNG"):
                    img_bytes = fig.to_image(format="png")
                    st.download_button(
                        label="Download Image",
                        data=img_bytes,
                        file_name="data_visualization.png",
                        mime="image/png"
                    )

            except Exception as e:
                st.error(f"Visualization error: {str(e)}")

            # ========== Advanced Export ==========
            st.subheader("🚀 Export Data")
            
            convert_col1, convert_col2, convert_col3 = st.columns(3)
            with convert_col1:
                target_format = st.selectbox(
                    "Export format:",
                    ["CSV", "Excel", "JSON", "Parquet"]
                )
            with convert_col2:
                if target_format == "JSON":
                    json_orient = st.selectbox(
                        "JSON format",
                        ["columns", "records", "split", "index"]
                    )
            with convert_col3:
                st.markdown("<br>", unsafe_allow_html=True)
                download_btn = st.button("📤 Start Export", type="primary")

            if download_btn:
                try:
                    if target_format == "CSV":
                        csv = st.session_state.processed_df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name="processed_data.csv",
                            mime="text/csv"
                        )
                    elif target_format == "Excel":
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            st.session_state.processed_df.to_excel(writer, index=False)
                        st.download_button(
                            label="Download Excel",
                            data=output.getvalue(),
                            file_name="processed_data.xlsx",
                            mime="application/vnd.ms-excel"
                        )
                    elif target_format == "JSON":
                        json_str = st.session_state.processed_df.to_json(
                            orient=json_orient,
                            indent=2
                        )
                        st.download_button(
                            label="Download JSON",
                            data=json_str,
                            file_name="processed_data.json",
                            mime="application/json"
                        )
                    elif target_format == "Parquet":
                        output = BytesIO()
                        st.session_state.processed_df.to_parquet(
                            output,
                            engine='pyarrow'
                        )
                        st.download_button(
                            label="Download Parquet",
                            data=output.getvalue(),
                            file_name="processed_data.parquet",
                            mime="application/octet-stream"
                        )
                except Exception as e:
                    st.error(f"Export error: {str(e)}")

else:
    st.info("📤 Please upload a data file to begin processing")

# Add footer
st.divider()
st.markdown("""
**🔍 Features Highlight:**
- Multi-format support (CSV, Excel, JSON, Parquet)
- Advanced data cleaning & transformation
- Interactive visualizations with export
- Smart date/text processing
- Real-time data previews
- Dataset quality metrics

*Powered by Streamlit | Pandas | Plotly*
""")