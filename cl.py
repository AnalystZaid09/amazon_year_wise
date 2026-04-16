import streamlit as st
import pandas as pd
import zipfile
from pathlib import Path
import io
import calendar
import gc
import numpy as np

import base64
try:
    import psutil
except ImportError:
    psutil = None

st.set_page_config(page_title="Sales Data Analysis", layout="wide", initial_sidebar_state="expanded")

# Initialize session state for analysis control
if 'start_analysis' not in st.session_state:
    st.session_state.start_analysis = False

# Initialize data storage in session state to prevent redundant processing
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'unfiltered_combined_df' not in st.session_state:
    st.session_state.unfiltered_combined_df = None
if 'transaction_counts' not in st.session_state:
    st.session_state.transaction_counts = {}
if 'metrics' not in st.session_state:
    st.session_state.metrics = {'filtered_count': 0, 'unfiltered_count': 0}

# Helper function to generate Excel binary data (Memory efficient - only generates when needed)
@st.cache_data(show_spinner="Generating Excel file...")
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def get_ram_usage():
    if psutil:
        process = psutil.Process()
        mem_info = process.memory_info()
        return mem_info.rss / (1024 * 1024) # Return MB
    return 0

def show_mem_warning():
    ram = get_ram_usage()
    if ram > 800: # 800MB limit alert for Streamlit Cloud
        st.sidebar.warning(f"⚠️ RAM Usage High: {ram:.0f}MB / 1024MB. Consider using High Volume Mode.")
    elif ram > 0:
        st.sidebar.caption(f"📊 RAM Usage: {ram:.0f}MB")

def render_download_button(df, filename, button_text, key=None):
    """Use native Streamlit download_button.
    This is much more memory efficient than b64-encoded HTML links."""
    st.download_button(
        label=f"📥 {button_text}",
        data=convert_df_to_excel(df),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key
    )


# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .filter-box {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }
    .quarter-badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        background-color: #4CAF50;
        color: white;
        border-radius: 5px;
        font-size: 0.9rem;
        margin-left: 0.5rem;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">📊 Sales Data Analysis Dashboard <span style="font-size: 1rem; vertical-align: middle;">(v2.5 - Ultra-Stabilized)</span></div>', unsafe_allow_html=True)

# File uploaders
st.sidebar.header("Upload Files")

# Add a clear cache button to ensure fresh data processing
if st.sidebar.button("🔄 Clear Cache & Refresh"):
    st.cache_data.clear()
    # Explicitly clear session state data
    st.session_state.processed_df = None
    st.session_state.unfiltered_combined_df = None
    st.session_state.start_analysis = False
    st.rerun()

show_mem_warning()

b2c_files = st.sidebar.file_uploader(
    "1. B2C Zip Files (Consumer Reports)", 
    type=['zip'], 
    accept_multiple_files=True
)
b2b_files = st.sidebar.file_uploader(
    "2. B2B Zip Files (Business Reports)", 
    type=['zip'], 
    accept_multiple_files=True
)
pm_file = st.sidebar.file_uploader("Upload PM Excel File", type=['xlsx', 'xls'])
cat_file = st.sidebar.file_uploader("Upload ASIN & Category File", type=['xlsx', 'xls'])

# Reset analysis if files are changed or cleared
current_batch_id = [f.name for f in (b2c_files + b2b_files)] if (b2c_files or b2b_files) else []
if pm_file: current_batch_id.append(pm_file.name)
if cat_file: current_batch_id.append(cat_file.name)

if 'last_batch_id' not in st.session_state:
    st.session_state.last_batch_id = current_batch_id

if st.session_state.last_batch_id != current_batch_id:
    st.session_state.start_analysis = False
    st.session_state.last_batch_id = current_batch_id
    # Clear cached results when files are changed
    st.session_state.processed_df = None
    st.session_state.unfiltered_combined_df = None

high_volume_mode = st.sidebar.checkbox(
    "🚀 High Volume Mode (50+ files)", 
    value=False, 
    help="Disables memory-intensive unfiltered data view to prevent crashes on large datasets."
)

if (b2c_files or b2b_files) and pm_file:
    # Analysis Trigger Button
    if not st.session_state.start_analysis:
        if st.sidebar.button("🚀 Start Data Analysis", use_container_width=True, type="primary"):
            st.session_state.start_analysis = True
            st.rerun()
    
    if st.session_state.start_analysis:
        # Clear garbage from potential previous runs
        gc.collect()
        
        # Process ZIP files
        def process_zip_files(zip_file_list, h_volume=False, segment="B2C"):
            if not zip_file_list:
                return pd.DataFrame(), pd.DataFrame(), {}
                
            transaction_counts = {}
            
            # Use lists for storage - significantly more memory efficient than iterative pd.concat
            all_shipments = []
            all_unfiltered = []
            
            relevant_cols = ['Invoice Date', 'Asin', 'Quantity', 'Invoice Amount', 'Order Id', 'Shipment Id', 'Transaction Type']
            
            total_files = 0
            for uploaded_zip in zip_file_list:
                with zipfile.ZipFile(uploaded_zip, 'r') as z:
                    total_files += len([f for f in z.namelist() if f.lower().endswith(('.csv', '.xlsx', '.xls')) and not f.endswith('/')])
            
            # v2.5 Auto-High-Volume Safety Check (RAM + Count)
            total_upload_size = sum([f.size for f in zip_file_list]) if zip_file_list else 0
            if (total_files > 12 or total_upload_size > 40 * 1024 * 1024) and not h_volume:
                st.sidebar.warning(f"🚀 {segment} Cloud Safe Mode Active ({total_files} files, {total_upload_size/1024/1024:.1f} MB)")
                h_volume = True

            progress_bar = st.progress(0, text=f"Preparing {segment} analysis...")
            status_text = st.empty()
            processed_count = 0
            
            # Deep Diagnostic Log
            with st.sidebar.expander(f"📝 {segment} Logs", expanded=False):
                log_container = st.empty()
                logs = [f"Starting {segment} process..."]
                log_container.code("\n".join(logs))
            
            for uploaded_zip in zip_file_list:
                with zipfile.ZipFile(uploaded_zip, 'r') as z:
                    for file_name in z.namelist():
                        if file_name.endswith('/') or not file_name.lower().endswith(('.csv', '.xlsx', '.xls')):
                            continue
                        
                        processed_count += 1
                        pct = int((processed_count / total_files) * 100)
                        status_text.text(f"⏳ [{segment}] File {processed_count}/{total_files}: {file_name} ({pct}%)")
                        progress_bar.progress(processed_count / total_files)
                        
                        try:
                            with z.open(file_name) as f:
                                if file_name.lower().endswith('.csv'):
                                    try:
                                        # Explicit dtype for ASIN to save memory
                                        df = pd.read_csv(f, low_memory=False, usecols=lambda x: x in relevant_cols, dtype={'Asin': str, 'ASIN': str})
                                    except ValueError:
                                        f.seek(0)
                                        # Fallback: Load and immediately filter columns to minimize RAM spike
                                        df = pd.read_csv(f, low_memory=False)
                                        df = df[[c for c in df.columns if c in relevant_cols]]
                                elif file_name.lower().endswith(('.xlsx', '.xls')):
                                    df = pd.read_excel(f)
                                    df = df[[c for c in df.columns if c in relevant_cols]]
                                else: continue
                                
                                if 'Asin' not in df.columns and 'ASIN' in df.columns:
                                    df.rename(columns={'ASIN': 'Asin'}, inplace=True)
                                
                                if 'Transaction Type' in df.columns:
                                    # Update counts
                                    counts = df['Transaction Type'].str.strip().str.lower().value_counts().to_dict()
                                    for t_type, count in counts.items():
                                        transaction_counts[t_type] = transaction_counts.get(t_type, 0) + count
                                    
                                    # Downcast numbers immediately
                                    for col in ['Quantity', 'Invoice Amount']:
                                        if col in df.columns:
                                            if col == 'Quantity':
                                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
                                            else:
                                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')

                                    # Memory Optimization: Compress strings early
                                    for col in ['Asin', 'Transaction Type']:
                                        if col in df.columns:
                                            df[col] = df[col].astype(str).astype('category')

                                    # Tag Segment
                                    df['Segment'] = segment

                                    # Filter for shipments
                                    is_shipment = df['Transaction Type'].str.strip().str.lower() == 'shipment'
                                    ship_df = df[is_shipment].copy()
                                    
                                    if not ship_df.empty:
                                        all_shipments.append(ship_df)
                                    
                                    if not h_volume:
                                        all_unfiltered.append(df)
                                    
                                    del df, is_shipment
                                    
                                    # Throttled GC to keep the event loop responsive
                                    if processed_count % 20 == 0:
                                        gc.collect()
                                    
                                    # Update Diagnostic Log (Only every 5 files to reduce WebSocket traffic)
                                    if processed_count % 5 == 0 or processed_count == total_files:
                                        logs.append(f"✅ [{processed_count}] {file_name}")
                                        log_container.code("\n".join(logs[-10:]))

                        except Exception as e:
                            err_msg = f"❌ Error in {file_name}: {str(e)}"
                            st.sidebar.error(err_msg)
                            logs.append(err_msg)
                            log_container.code("\n".join(logs[-10:]))
                            continue
            
            # Final consolidation phase using "Chunked Concat" to prevent RAM spikes
            status_text.text(f"📊 Consolidation Phase: {segment}")
            
            def safe_concat(df_list):
                if not df_list: return pd.DataFrame()
                # Process in batches of 10 to keep Peak RAM low
                chunks = []
                while df_list:
                    batch = []
                    for _ in range(min(10, len(df_list))):
                        batch.append(df_list.pop(0))
                    
                    if batch:
                        chunks.append(pd.concat(batch, ignore_index=True))
                        del batch
                        gc.collect()
                
                final_df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
                del chunks
                gc.collect()
                return final_df

            filtered_combined = safe_concat(all_shipments)
            unfiltered_combined = safe_concat(all_unfiltered)
            
            progress_bar.empty()
            status_text.empty()
            
            return filtered_combined, unfiltered_combined, transaction_counts

        gc.collect()

        def process_data(filtered_df, unfiltered_df, pm_df, cat_df=None):
            def add_date_columns(df):
                if df.empty: return df
                # Speed up datetime parsing and save RAM spikes with explicit format if possible
                df['Invoice Date'] = pd.to_datetime(df['Invoice Date'], errors='coerce')
                df['Date'] = df['Invoice Date'].dt.date
                df['Month'] = pd.to_datetime(df['Date']).dt.month
                df['Year'] = pd.to_datetime(df['Date']).dt.year
                df['Month_Year'] = pd.to_datetime(df['Date']).dt.strftime('%b-%y')
                df['Quarter'] = pd.cut(df['Month'], bins=[0, 3, 6, 9, 12], labels=['Q1', 'Q2', 'Q3', 'Q4']).astype(str)
                df['Quarter_Year'] = df['Quarter'] + '-' + df['Year'].astype(str)
                return df
            
            # Select only essential PM columns to minimize merge memory footprint
            pm_cols = pm_df[['ASIN', 'Brand', 'Brand Manager', 'Vendor SKU Codes', 'Product Name']].drop_duplicates(subset=['ASIN'], keep='first').copy()
            pm_cols['ASIN'] = pm_cols['ASIN'].astype(str)
            
            # Helper to clean numeric columns
            def clean_numeric(df, col):
                if df.empty or col not in df.columns: return df
                # Memory-safe cleaning: avoid creating massive temporary string copies
                df[col] = df[col].astype(str).str.replace('₹', '', regex=False).str.replace(',', '', regex=False).str.replace(' ', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                # Downcast to save 50% RAM for numeric columns
                df[col] = df[col].astype('int32') if col == 'Quantity' else df[col].astype('float32')
                return df

            # Prepare category mapping if available
            cat_mapping = None
            if cat_df is not None and not cat_df.empty:
                asin_col = next((c for c in cat_df.columns if c.lower() == 'asin'), None)
                category_col = next((c for c in cat_df.columns if c.lower() == 'category'), None)
                if asin_col and category_col:
                    cat_mapping = cat_df[[asin_col, category_col]].drop_duplicates(subset=[asin_col]).copy()
                    cat_mapping.rename(columns={asin_col: 'ASIN_CAT', category_col: 'Category'}, inplace=True)
                    cat_mapping['ASIN_CAT'] = cat_mapping['ASIN_CAT'].astype(str)

            # Process filtered_df
            if not filtered_df.empty:
                filtered_df = add_date_columns(filtered_df)
                filtered_df = clean_numeric(filtered_df, 'Quantity')
                filtered_df = clean_numeric(filtered_df, 'Invoice Amount')
                filtered_df['Asin'] = filtered_df['Asin'].astype(str)
                filtered_df = filtered_df.merge(pm_cols, left_on='Asin', right_on='ASIN', how='left')
                if 'ASIN' in filtered_df.columns: del filtered_df['ASIN']
                
                if cat_mapping is not None:
                    filtered_df = filtered_df.merge(cat_mapping, left_on='Asin', right_on='ASIN_CAT', how='left')
                    if 'ASIN_CAT' in filtered_df.columns: del filtered_df['ASIN_CAT']
                
                # Categorize descriptive columns for massive RAM savings - FIX: cast to str first
                cat_cols = ['Brand', 'Brand Manager', 'Product Name', 'Category']
                for col in cat_cols:
                    if col in filtered_df.columns:
                        filtered_df[col] = filtered_df[col].astype(str).replace(['nan', 'None', '<NA>', ''], f'Unknown {col}').astype('category')
                gc.collect()
            
            # Process unfiltered_df (only if not in high volume mode)
            if not unfiltered_df.empty:
                unfiltered_df = add_date_columns(unfiltered_df)
                unfiltered_df = clean_numeric(unfiltered_df, 'Quantity')
                unfiltered_df = clean_numeric(unfiltered_df, 'Invoice Amount')
                unfiltered_df['Asin'] = unfiltered_df['Asin'].astype(str)
                unfiltered_df = unfiltered_df.merge(pm_cols, left_on='Asin', right_on='ASIN', how='left')
                if 'ASIN' in unfiltered_df.columns: del unfiltered_df['ASIN']
                
                if cat_mapping is not None:
                    unfiltered_df = unfiltered_df.merge(cat_mapping, left_on='Asin', right_on='ASIN_CAT', how='left')
                    if 'ASIN_CAT' in unfiltered_df.columns: del unfiltered_df['ASIN_CAT']
                
                cat_cols_u = ['Brand', 'Brand Manager', 'Product Name', 'Transaction Type', 'Category']
                for col in cat_cols_u:
                    if col in unfiltered_df.columns:
                        unfiltered_df[col] = unfiltered_df[col].astype(str).replace(['nan', 'None', '<NA>', ''], f'Unknown {col}').astype('category')
                gc.collect()
            
            del pm_cols, cat_mapping
            gc.collect()
            return filtered_df, unfiltered_df, len(filtered_df), len(unfiltered_df)

        if st.session_state.processed_df is None:
            with st.spinner("Processing files..."):
                # Run sequential processing to save RAM
                f_b2c, u_b2c, t_b2c = process_zip_files(b2c_files, high_volume_mode, "B2C")
                
                # B2B Processing
                f_b2b, u_b2b, t_b2b = process_zip_files(b2b_files, high_volume_mode, "B2B")
                
                # Combine results incrementally to clear buffers immediately
                f_combined = pd.concat([f_b2c, f_b2b], ignore_index=True)
                del f_b2c, f_b2b
                gc.collect()
                
                u_combined = pd.concat([u_b2c, u_b2b], ignore_index=True)
                del u_b2c, u_b2b
                gc.collect()
                
                # Combine transaction counts
                t_counts = t_b2c.copy()
                del t_b2c
                for k, v in t_b2b.items():
                    t_counts[k] = t_counts.get(k, 0) + v
                del t_b2b
                st.session_state.transaction_counts = t_counts
                gc.collect()

                pm_relevant_cols = ['ASIN', 'Brand', 'Brand Manager', 'Vendor SKU Codes', 'Product Name']
                pm_df = pd.read_excel(pm_file, usecols=lambda x: x in pm_relevant_cols)
                cat_df = pd.read_excel(cat_file) if cat_file else None
                
                # Store results in session state
                st.session_state.processed_df, st.session_state.unfiltered_combined_df, f_count, u_count = process_data(f_combined, u_combined, pm_df, cat_df)
                st.session_state.metrics = {'filtered_count': f_count, 'unfiltered_count': u_count}
                
                del f_combined, u_combined, pm_df, cat_df
                gc.collect()
        
        # Reference data from session state
        processed_df = st.session_state.processed_df
        unfiltered_combined_df = st.session_state.unfiltered_combined_df
        transaction_counts = st.session_state.transaction_counts
        filtered_count = st.session_state.metrics['filtered_count']
        unfiltered_count = st.session_state.metrics['unfiltered_count']
        
        # Guard against zero records found
        if filtered_count == 0 and unfiltered_count == 0:
            st.warning("⚠️ No valid records found in the uploaded files. Check if you uploaded the correct report types.")
            st.stop()
        
        # Show detailed record counts
        col1, col2 = st.columns(2)
        with col1:
            st.success(f"✅ Filtered (Shipment only): **{filtered_count:,}** records")
        with col2:
            st.info(f"📊 Total Records: **{unfiltered_count if not high_volume_mode else filtered_count:,}**")
            if high_volume_mode:
                st.warning("🚀 High Volume Mode is ACTIVE. Unfiltered data view is disabled to prioritize memory.")
        
        # Show transaction type breakdown
        with st.expander("🔍 Transaction Type Breakdown"):
            st.write("Records by Transaction Type:")
            for trans_type, count in sorted(transaction_counts.items(), key=lambda x: -x[1]):
                st.write(f"  - **{trans_type}**: {count:,}")
    
        # Enhanced Sidebar filters
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🎯 Time Period Filters")
        
        # Create filter box styling
        with st.sidebar.container():
            time_period = st.radio(
                "Select View Type",
                ["📅 All Data", "📆 Quarter View", "🗓️ Month View"],
                help="Choose how you want to view your data"
            )
            
        # Segment Filter
        segments = ["B2C", "B2B"] if not processed_df.empty else []
        selected_segments = st.sidebar.multiselect("Select Segment", options=segments, default=segments)
        
        # Filtering logic
        filtered_df = processed_df.copy()
        
        if selected_segments:
            filtered_df = filtered_df[filtered_df['Segment'].isin(selected_segments)]
        
        filter_info = ""
        
        if time_period == "📆 Quarter View":
            st.sidebar.markdown("---")
            
            # Get available years
            years = sorted(filtered_df['Year'].dropna().unique(), reverse=True)
            selected_year = st.sidebar.selectbox(
                "📅 Select Year",
                years,
                help="Select the year for quarter analysis"
            )
            
            # Quarter selection with descriptions
            quarter_options = {
                'Q1': 'Q1 (Jan - Mar)',
                'Q2': 'Q2 (Apr - Jun)',
                'Q3': 'Q3 (Jul - Sep)',
                'Q4': 'Q4 (Oct - Dec)'
            }
            
            # Filter available quarters for selected year
            available_quarters = processed_df[processed_df['Year'] == selected_year]['Quarter'].unique()
            available_quarter_options = {k: v for k, v in quarter_options.items() if k in available_quarters}
            
            if available_quarter_options:
                selected_quarter_display = st.sidebar.selectbox(
                    "📊 Select Quarter",
                    list(available_quarter_options.values()),
                    help="Q1: Jan-Mar | Q2: Apr-Jun | Q3: Jul-Sep | Q4: Oct-Dec"
                )
                
                # Get the quarter code (Q1, Q2, Q3, Q4)
                selected_quarter = [k for k, v in quarter_options.items() if v == selected_quarter_display][0]
                
                filtered_df = processed_df[
                    (processed_df['Quarter'] == selected_quarter) & 
                    (processed_df['Year'] == selected_year)
                ]
                
                # Define month ranges
                quarter_months = {
                    'Q1': ['January', 'February', 'March'],
                    'Q2': ['April', 'May', 'June'],
                    'Q3': ['July', 'August', 'September'],
                    'Q4': ['October', 'November', 'December']
                }
                
                filter_info = f"**{selected_quarter} {selected_year}** ({', '.join(quarter_months[selected_quarter])})"
                
                # Show summary for quarter
                st.sidebar.markdown("---")
                st.sidebar.markdown("#### Quarter Summary")
                st.sidebar.metric("Total Records", f"{len(filtered_df):,}")
                st.sidebar.metric("Date Range", f"{filtered_df['Date'].min()} to {filtered_df['Date'].max()}")
            else:
                st.sidebar.warning(f"No data available for {selected_year}")
    
        elif time_period == "🗓️ Month View":
            st.sidebar.markdown("---")
            
            # Get available years
            years = sorted(processed_df['Year'].dropna().unique(), reverse=True)
            selected_year = st.sidebar.selectbox(
                "📅 Select Year",
                years,
                help="Select the year for month analysis"
            )
            
            # Get available months for selected year
            year_data = processed_df[processed_df['Year'] == selected_year]
            available_months = sorted(year_data['Month'].dropna().unique())
            month_names = [calendar.month_name[m] for m in available_months]
            
            if month_names:
                selected_month_name = st.sidebar.selectbox(
                    "📊 Select Month",
                    month_names,
                    help="Choose a specific month to analyze"
                )
                
                # Get month number
                selected_month = list(calendar.month_name).index(selected_month_name)
                
                filtered_df = processed_df[
                    (processed_df['Month'] == selected_month) & 
                    (processed_df['Year'] == selected_year)
                ]
                
                filter_info = f"**{selected_month_name} {selected_year}**"
                
                # Show summary for month
                st.sidebar.markdown("---")
                st.sidebar.markdown("#### Month Summary")
                st.sidebar.metric("Total Records", f"{len(filtered_df):,}")
                st.sidebar.metric("Date Range", f"{filtered_df['Date'].min()} to {filtered_df['Date'].max()}")
            else:
                st.sidebar.warning(f"No data available for {selected_year}")
        else:
            filter_info = "**All Available Data**"
    
        # Additional filters
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🔍 Additional Filters")
        
        # Brand filter with count
        brands = sorted([b for b in filtered_df['Brand'].dropna().unique() if b])
        brand_counts = filtered_df['Brand'].value_counts()
        
        brand_options = ['All Brands'] + [f"{brand} ({brand_counts[brand]:,})" for brand in brands]
        selected_brand_display = st.sidebar.selectbox(
            "🏢 Filter by Brand",
            brand_options,
            help="Select a specific brand or view all brands"
        )
        
        if selected_brand_display != 'All Brands':
            selected_brand = selected_brand_display.split(' (')[0]
            filtered_df = filtered_df[filtered_df['Brand'] == selected_brand]
        
        # Brand Manager filter
        managers = sorted([m for m in filtered_df['Brand Manager'].dropna().unique() if m])
        manager_options = ['All Managers'] + managers
        selected_manager = st.sidebar.selectbox(
            "👤 Filter by Brand Manager",
            manager_options,
            help="Select a specific brand manager"
        )
        
        if selected_manager != 'All Managers':
            filtered_df = filtered_df[filtered_df['Brand Manager'] == selected_manager]
        
        # Display active filters
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📌 Active Filters")
        st.sidebar.info(f"""
        **Period:** {filter_info}
        **Brand:** {selected_brand_display.split(' (')[0]}
        **Manager:** {selected_manager}
        **Records:** {len(filtered_df):,}
        """)
        
        st.markdown(f"### Current View: {filter_info}")
        
        # Segment summary metrics - NEW
        if not filtered_df.empty:
            seg_col1, seg_col2, seg_col3 = st.columns([1, 1, 2])
            b2c_total = filtered_df[filtered_df['Segment'] == 'B2C']['Invoice Amount'].sum()
            b2b_total = filtered_df[filtered_df['Segment'] == 'B2B']['Invoice Amount'].sum()
            total_sum = b2c_total + b2b_total
            
            with seg_col1:
                st.metric("🛒 B2C Sales", f"₹ {b2c_total:,.0f}", f"{b2c_total/total_sum*100:.1f}% of total" if total_sum > 0 else "")
            with seg_col2:
                st.metric("🏢 B2B Sales", f"₹ {b2b_total:,.0f}", f"{b2b_total/total_sum*100:.1f}% of total" if total_sum > 0 else "")
            with seg_col3:
                st.info(f"**Total Consolidated Segment Sales:** ₹ {total_sum:,.0f}")
        
        st.markdown("---")
        
        # Main content tabs - Tab 4 is conditional based on high_volume_mode
        tabs = ["🏢 Brand Analysis", "📦 ASIN Analysis", "📋 Raw Data"]
        if not high_volume_mode:
            tabs.append("📊 Combined Data (Unfiltered)")
        tabs.extend(["📊 Brand Comparison (YoY)", "📦 ASIN Comparison (YoY)"])
        
        tab_list = st.tabs(tabs)
        
        # Map tabs correctly based on whether Tab 4 exists
        tab1 = tab_list[0] # Brand Analysis
        tab2 = tab_list[1] # ASIN Analysis
        tab3 = tab_list[2] # Raw Data
        if not high_volume_mode:
            tab4 = tab_list[3] # Combined Data (Unfiltered)
            tab5 = tab_list[4] # Brand Comparison (YoY)
            tab6 = tab_list[5] # ASIN Comparison (YoY)
        else:
            # When high_volume_mode is True, "Combined Data (Unfiltered)" is skipped.
            # The original tab_list would have 3 items + 2 items = 5 items.
            # So, tab_list[3] becomes "Brand Comparison (YoY)" and tab_list[4] becomes "ASIN Comparison (YoY)".
            tab4 = None # No "Combined Data (Unfiltered)" tab
            tab5 = tab_list[3] # Brand Comparison (YoY)
            tab6 = tab_list[4] # ASIN Comparison (YoY)
        
        with tab1:
            st.header("Brand Analysis")
            
            brand_pivot = pd.pivot_table(
                filtered_df,
                index='Brand',
                values=['Quantity', 'Invoice Amount'],
                aggfunc='sum',
                observed=True,
                margins=False
            ).reset_index()
            
            # Ensure Brand is string for Arrow compatibility
            brand_pivot['Brand'] = brand_pivot['Brand'].astype(str)
            brand_pivot = brand_pivot.sort_values(by='Quantity', ascending=False)
            
            # Add Grand Total row properly for Arrow compatibility
            grand_total_row = pd.DataFrame({
                'Brand': ['Grand Total'],
                'Invoice Amount': [brand_pivot['Invoice Amount'].sum() if 'Invoice Amount' in brand_pivot.columns else 0],
                'Quantity': [brand_pivot['Quantity'].sum() if 'Quantity' in brand_pivot.columns else 0]
            })
            brand_pivot = pd.concat([brand_pivot, grand_total_row], ignore_index=True)
            
            # Format display dataframe (defensive formatting)
            display_brand_pivot = brand_pivot.copy()
            if 'Invoice Amount' in display_brand_pivot.columns:
                display_brand_pivot['Invoice Amount'] = display_brand_pivot['Invoice Amount'].apply(
                    lambda x: f"₹{float(x):,.2f}" if pd.notnull(x) and str(x).replace('.','',1).replace('-','',1).isdigit() else "₹0.00"
                )
            if 'Quantity' in display_brand_pivot.columns:
                display_brand_pivot['Quantity'] = display_brand_pivot['Quantity'].apply(
                    lambda x: f"{int(float(x)):,.0f}" if pd.notnull(x) and str(x).replace('.','',1).replace('-','',1).isdigit() else "0"
                )
            
            st.dataframe(display_brand_pivot, use_container_width=True, height=600)
            
            # Download link (Excel format)
            render_download_button(brand_pivot, f"brand_analysis_{time_period}.xlsx", "Download Brand Analysis Excel", key="brand_dl")
        
        with tab2:
            st.header("ASIN Analysis")
            
            asin_index = ['Asin']

            if 'Category' in filtered_df.columns:
                asin_index.append('Category')
            
            asin_index += ['Product Name', 'Brand']
            
            asin_pivot = pd.pivot_table(
                filtered_df,
                index=asin_index,
                values=['Quantity', 'Invoice Amount'],
                aggfunc='sum',
                observed=True,
                margins=False
            ).reset_index()
            
            # Ensure index columns are strings for Arrow compatibility (Safe cast for Categorical)
            for col in asin_index:
                if col in asin_pivot.columns:
                    asin_pivot[col] = asin_pivot[col].astype(str).replace(['nan', 'None', '<NA>'], '')
            
            asin_pivot = asin_pivot.sort_values(by='Quantity', ascending=False)
            
            # Add Grand Total row properly for Arrow compatibility
            grand_total_row_asin_dict = {
                'Asin': ['Grand Total'],
                'Product Name': [''],
                'Brand': [''],
                'Invoice Amount': [asin_pivot['Invoice Amount'].sum() if 'Invoice Amount' in asin_pivot.columns else 0],
                'Quantity': [asin_pivot['Quantity'].sum() if 'Quantity' in asin_pivot.columns else 0]
            }
            if 'Category' in asin_pivot.columns:
                grand_total_row_asin_dict['Category'] = ['']
                
            grand_total_row_asin = pd.DataFrame(grand_total_row_asin_dict)
            asin_pivot = pd.concat([asin_pivot, grand_total_row_asin], ignore_index=True)
            
            # Format display dataframe (defensive formatting)
            display_asin_pivot = asin_pivot.copy()
            if 'Invoice Amount' in display_asin_pivot.columns:
                display_asin_pivot['Invoice Amount'] = display_asin_pivot['Invoice Amount'].apply(
                    lambda x: f"₹{float(x):,.2f}" if pd.notnull(x) and str(x).replace('.','',1).replace('-','',1).isdigit() else "₹0.00"
                )
            if 'Quantity' in display_asin_pivot.columns:
                display_asin_pivot['Quantity'] = display_asin_pivot['Quantity'].apply(
                    lambda x: f"{int(float(x)):,.0f}" if pd.notnull(x) and str(x).replace('.','',1).replace('-','',1).isdigit() else "0"
                )
            
            st.dataframe(display_asin_pivot, use_container_width=True, height=600)
            
            # Download link (Excel format)
            render_download_button(asin_pivot, f"asin_analysis_{time_period}.xlsx", "Download ASIN Analysis Excel", key="asin_dl")
        
        with tab3:
            st.header("Raw/Processed Data")
            
            # Select columns to display
            all_columns = filtered_df.columns.tolist()
            default_columns = ['Invoice Date', 'Asin', 'Brand', 'Category', 'Product Name', 'Quantity', 
                              'Invoice Amount', 'Month_Year', 'Quarter', 'Year', 'Order Id', 'Shipment Id']
            
            selected_columns = st.multiselect(
                "Select columns to display",
                all_columns,
                default=[col for col in default_columns if col in all_columns]
            )
            
            if selected_columns:
                # Stricter row limit for Cloud stability
                row_limit = 5000
                if len(filtered_df) > row_limit:
                    st.warning(f"⚠️ Showing only first {row_limit:,} rows for RAM stability. Full data is available in Excel below.")
                    display_df = filtered_df[selected_columns].head(row_limit).copy()
                else:
                    display_df = filtered_df[selected_columns].copy()
                
                st.dataframe(display_df, use_container_width=True, height=600)
                
                # Download link - Excel format
                render_download_button(filtered_df[selected_columns], f"filtered_data_{time_period}.xlsx", "Download ALL Filtered Data Excel", key="raw_dl")
            else:
                st.warning("Please select at least one column to display")
        
        if not high_volume_mode and tab5:
            with tab5:
                st.header("Combined Data (Unfiltered)")
                st.info(f"📊 This tab shows ALL {unfiltered_count:,} records without the 'Shipment' transaction type filter.")
            
                # Show transaction type breakdown
                st.subheader("Transaction Type Distribution")
                if not unfiltered_combined_df.empty and 'Transaction Type' in unfiltered_combined_df.columns:
                    trans_type_counts = unfiltered_combined_df['Transaction Type'].value_counts().reset_index()
                    trans_type_counts.columns = ['Transaction Type', 'Count']
                    trans_type_counts['Percentage'] = (trans_type_counts['Count'] / trans_type_counts['Count'].sum() * 100).round(2).astype(str) + '%'
                    st.dataframe(trans_type_counts, use_container_width=True)
                
                st.subheader("All Data")
                
                # Select columns to display
                all_columns_unfiltered = unfiltered_combined_df.columns.tolist()
                default_columns_unfiltered = ['Invoice Date', 'Transaction Type', 'Asin', 'Brand', 'Product Name', 'Quantity', 
                                  'Invoice Amount', 'Month_Year', 'Quarter', 'Year', 'Order Id', 'Shipment Id']
                
                selected_columns_unfiltered = st.multiselect(
                    "Select columns to display",
                    all_columns_unfiltered,
                    default=[col for col in default_columns_unfiltered if col in all_columns_unfiltered],
                    key="unfiltered_columns"
                )
                
                if selected_columns_unfiltered:
                    # Apply row limit for unfiltered data as well
                    row_limit_u = 5000
                    if len(unfiltered_combined_df) > row_limit_u:
                        st.warning(f"⚠️ Showing only first {row_limit_u:,} rows for RAM stability.")
                        display_unfiltered_df = unfiltered_combined_df[selected_columns_unfiltered].head(row_limit_u).copy()
                    else:
                        display_unfiltered_df = unfiltered_combined_df[selected_columns_unfiltered].copy()
                    
                    st.dataframe(display_unfiltered_df, use_container_width=True, height=600)
                    
                    # Download link - Excel format (base64 approach for Streamlit Cloud)
                    render_download_button(display_unfiltered_df, f"combined_unfiltered_data_{time_period}.xlsx", "Download Combined (Unfiltered) Data Excel", key="unfiltered_dl")
                else:
                    st.warning("Please select at least one column to display")
        
        # Year-over-Year Comparison Tabs
        with tab5:
            st.header("📊 Brand Comparison (Year-over-Year)")
            
            # Get available years
            available_years = sorted(processed_df['Year'].dropna().unique(), reverse=True)
            
            if len(available_years) >= 2:
                st.markdown("### Select Years to Compare")
                col1, col2 = st.columns(2)
                
                with col1:
                    current_year = st.selectbox(
                        "📅 Current Year (to be analyzed)",
                        available_years,
                        index=0,
                        key="brand_current_year"
                    )
                
                with col2:
                    # Filter out the current year from previous year options
                    prev_year_options = [y for y in available_years if y != current_year]
                    if prev_year_options:
                        previous_year = st.selectbox(
                            "📅 Previous Year (to compare against)",
                            prev_year_options,
                            index=0,
                            key="brand_previous_year"
                        )
                    else:
                        previous_year = None
                        st.warning("No other year available for comparison")
                
                if previous_year:
                    # Filter data by years
                    current_year_data = processed_df[processed_df['Year'] == current_year]
                    previous_year_data = processed_df[processed_df['Year'] == previous_year]
                    
                    # Create brand pivots for each year
                    current_brand_pivot = pd.pivot_table(
                        current_year_data,
                        index='Brand',
                        values=['Quantity', 'Invoice Amount'],
                        aggfunc='sum',
                        observed=True
                    ).reset_index()
                    current_brand_pivot.columns = ['Brand', f'Invoice Amount ({current_year})', f'Quantity ({current_year})']
                    
                    previous_brand_pivot = pd.pivot_table(
                        previous_year_data,
                        index='Brand',
                        values=['Quantity', 'Invoice Amount'],
                        aggfunc='sum',
                        observed=True
                    ).reset_index()
                    previous_brand_pivot.columns = ['Brand', f'Invoice Amount ({previous_year})', f'Quantity ({previous_year})']
                
                    # Merge the two pivots
                    brand_comparison = pd.merge(
                        previous_brand_pivot,
                        current_brand_pivot,
                        on='Brand',
                        how='outer'
                    )
                    
                    # Safe fill: only fill numeric columns with 0, cast categorical to str
                    brand_comparison['Brand'] = brand_comparison['Brand'].astype(str).replace(['nan', 'None', '<NA>'], 'Unknown Brand')
                    numeric_cols = brand_comparison.select_dtypes(include=[np.number]).columns
                    brand_comparison[numeric_cols] = brand_comparison[numeric_cols].fillna(0)
                    
                    # Calculate differences and percentage changes
                    brand_comparison['Qty Difference'] = brand_comparison[f'Quantity ({current_year})'] - brand_comparison[f'Quantity ({previous_year})']
                    brand_comparison['Qty % Change'] = brand_comparison.apply(
                        lambda row: ((row[f'Quantity ({current_year})'] - row[f'Quantity ({previous_year})']) / row[f'Quantity ({previous_year})'] * 100) 
                        if row[f'Quantity ({previous_year})'] != 0 else (100 if row[f'Quantity ({current_year})'] > 0 else 0), axis=1
                    )
                    
                    brand_comparison['Amount Difference'] = brand_comparison[f'Invoice Amount ({current_year})'] - brand_comparison[f'Invoice Amount ({previous_year})']
                    brand_comparison['Amount % Change'] = brand_comparison.apply(
                        lambda row: ((row[f'Invoice Amount ({current_year})'] - row[f'Invoice Amount ({previous_year})']) / row[f'Invoice Amount ({previous_year})'] * 100) 
                        if row[f'Invoice Amount ({previous_year})'] != 0 else (100 if row[f'Invoice Amount ({current_year})'] > 0 else 0), axis=1
                    )
                    
                    # Reorder columns
                    brand_comparison = brand_comparison[[
                        'Brand',
                        f'Quantity ({previous_year})', f'Quantity ({current_year})', 'Qty Difference', 'Qty % Change',
                        f'Invoice Amount ({previous_year})', f'Invoice Amount ({current_year})', 'Amount Difference', 'Amount % Change'
                    ]]
                    
                    # Sort by current year quantity descending
                    brand_comparison = brand_comparison.sort_values(by=f'Quantity ({current_year})', ascending=False)
                    
                    # Add Grand Total row
                    grand_total = pd.DataFrame({
                        'Brand': ['Grand Total'],
                        f'Quantity ({previous_year})': [brand_comparison[f'Quantity ({previous_year})'].sum()],
                        f'Quantity ({current_year})': [brand_comparison[f'Quantity ({current_year})'].sum()],
                        'Qty Difference': [brand_comparison['Qty Difference'].sum()],
                        'Qty % Change': [
                            (brand_comparison[f'Quantity ({current_year})'].sum() - brand_comparison[f'Quantity ({previous_year})'].sum()) / 
                            brand_comparison[f'Quantity ({previous_year})'].sum() * 100 if brand_comparison[f'Quantity ({previous_year})'].sum() != 0 else 0
                        ],
                        f'Invoice Amount ({previous_year})': [brand_comparison[f'Invoice Amount ({previous_year})'].sum()],
                        f'Invoice Amount ({current_year})': [brand_comparison[f'Invoice Amount ({current_year})'].sum()],
                        'Amount Difference': [brand_comparison['Amount Difference'].sum()],
                        'Amount % Change': [
                            (brand_comparison[f'Invoice Amount ({current_year})'].sum() - brand_comparison[f'Invoice Amount ({previous_year})'].sum()) / 
                            brand_comparison[f'Invoice Amount ({previous_year})'].sum() * 100 if brand_comparison[f'Invoice Amount ({previous_year})'].sum() != 0 else 0
                        ]
                    })
                    brand_comparison = pd.concat([brand_comparison, grand_total], ignore_index=True)
                    
                    # Format display dataframe
                    display_brand_comparison = brand_comparison.copy()
                    display_brand_comparison[f'Quantity ({previous_year})'] = display_brand_comparison[f'Quantity ({previous_year})'].apply(lambda x: f"{x:,.0f}")
                    display_brand_comparison[f'Quantity ({current_year})'] = display_brand_comparison[f'Quantity ({current_year})'].apply(lambda x: f"{x:,.0f}")
                    display_brand_comparison['Qty Difference'] = display_brand_comparison['Qty Difference'].apply(lambda x: f"{x:+,.0f}")
                    display_brand_comparison['Qty % Change'] = display_brand_comparison['Qty % Change'].apply(lambda x: f"{x:+.2f}%")
                    display_brand_comparison[f'Invoice Amount ({previous_year})'] = display_brand_comparison[f'Invoice Amount ({previous_year})'].apply(lambda x: f"₹{x:,.2f}")
                    display_brand_comparison[f'Invoice Amount ({current_year})'] = display_brand_comparison[f'Invoice Amount ({current_year})'].apply(lambda x: f"₹{x:,.2f}")
                    display_brand_comparison['Amount Difference'] = display_brand_comparison['Amount Difference'].apply(lambda x: f"₹{x:+,.2f}")
                    display_brand_comparison['Amount % Change'] = display_brand_comparison['Amount % Change'].apply(lambda x: f"{x:+.2f}%")
                    
                    # Show summary metrics
                    st.markdown(f"### Comparison: {current_year} vs {previous_year}")
                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                    
                    total_qty_change = brand_comparison.iloc[-1]['Qty Difference']
                    total_qty_pct = brand_comparison.iloc[-1]['Qty % Change']
                    total_amt_change = brand_comparison.iloc[-1]['Amount Difference']
                    total_amt_pct = brand_comparison.iloc[-1]['Amount % Change']
                    
                    with metric_col1:
                        st.metric("Total Qty Change", f"{total_qty_change:+,.0f}", f"{total_qty_pct:+.2f}%")
                    with metric_col2:
                        st.metric("Total Amount Change", f"₹{total_amt_change:+,.0f}", f"{total_amt_pct:+.2f}%")
                    with metric_col3:
                        st.metric(f"Brands in {current_year}", f"{len(current_year_data['Brand'].dropna().unique()):,}")
                    with metric_col4:
                        st.metric(f"Brands in {previous_year}", f"{len(previous_year_data['Brand'].dropna().unique()):,}")
                    
                    st.dataframe(display_brand_comparison, use_container_width=True, height=600)
                    
                    # Download link (native download button)
                    render_download_button(brand_comparison, f"brand_comparison_{current_year}_vs_{previous_year}.xlsx", "Download Brand Comparison Excel", key="brand_comp_dl")
                else:
                    st.warning("⚠️ Need at least 2 years of data for comparison. Please upload data from multiple years.")
            else:
                st.warning("⚠️ Need at least 2 years of data for comparison. Please upload data from multiple years.")
    
        with tab6:
            st.header("📦 ASIN Comparison (Year-over-Year)")
            
            # Get available years
            available_years_asin = sorted(processed_df['Year'].dropna().unique(), reverse=True)
            
            if len(available_years_asin) >= 2:
                st.markdown("### Select Years to Compare")
                col1, col2 = st.columns(2)
                
                with col1:
                    current_year_asin = st.selectbox(
                        "📅 Current Year (to be analyzed)",
                        available_years_asin,
                        index=0,
                        key="asin_current_year"
                    )
                
                with col2:
                    # Filter out the current year from previous year options
                    prev_year_options_asin = [y for y in available_years_asin if y != current_year_asin]
                    if prev_year_options_asin:
                        previous_year_asin = st.selectbox(
                            "📅 Previous Year (to compare against)",
                            prev_year_options_asin,
                            index=0,
                            key="asin_previous_year"
                        )
                    else:
                        previous_year_asin = None
                        st.warning("No other year available for comparison")
                
                if previous_year_asin:
                    # Filter data by years
                    current_year_data_asin = processed_df[processed_df['Year'] == current_year_asin]
                    previous_year_data_asin = processed_df[processed_df['Year'] == previous_year_asin]
                    
                    yoy_index = ['Asin']

                    if 'Category' in processed_df.columns:
                        yoy_index.append('Category')

                    yoy_index += ['Brand']
                    
                    # Create ASIN pivots for each year
                    current_asin_pivot = pd.pivot_table(
                        current_year_data_asin,
                        index=yoy_index,
                        values=['Quantity', 'Invoice Amount'],
                        aggfunc='sum',
                        observed=True
                    ).reset_index()
                    current_asin_pivot.columns = yoy_index + [f'Invoice Amount ({current_year_asin})', f'Quantity ({current_year_asin})']
                    
                    previous_asin_pivot = pd.pivot_table(
                        previous_year_data_asin,
                        index=yoy_index,
                        values=['Quantity', 'Invoice Amount'],
                        aggfunc='sum',
                        observed=True
                    ).reset_index()
                    previous_asin_pivot.columns = yoy_index + [f'Invoice Amount ({previous_year_asin})', f'Quantity ({previous_year_asin})']
                    
                    # Merge the two pivots
                    asin_comparison = pd.merge(
                        previous_asin_pivot,
                        current_asin_pivot,
                        on=yoy_index,
                        how='outer'
                    )
                    
                    # Safe fill: only fill numeric columns with 0, cast identifiers to str
                    for col in yoy_index:
                        if col in asin_comparison.columns:
                            asin_comparison[col] = asin_comparison[col].astype(str).replace(['nan', 'None', '<NA>'], '')
                    
                    numeric_cols_asin = asin_comparison.select_dtypes(include=[np.number]).columns
                    asin_comparison[numeric_cols_asin] = asin_comparison[numeric_cols_asin].fillna(0)
                    
                    # Calculate differences
                    asin_comparison['Qty Difference'] = asin_comparison[f'Quantity ({current_year_asin})'] - asin_comparison[f'Quantity ({previous_year_asin})']
                    asin_comparison['Qty % Change'] = asin_comparison.apply(
                        lambda row: ((row[f'Quantity ({current_year_asin})'] - row[f'Quantity ({previous_year_asin})']) / row[f'Quantity ({previous_year_asin})'] * 100) 
                        if row[f'Quantity ({previous_year_asin})'] != 0 else (100 if row[f'Quantity ({current_year_asin})'] > 0 else 0), axis=1
                    )
                    
                    asin_comparison['Amount Difference'] = asin_comparison[f'Invoice Amount ({current_year_asin})'] - asin_comparison[f'Invoice Amount ({previous_year_asin})']
                    asin_comparison['Amount % Change'] = asin_comparison.apply(
                        lambda row: ((row[f'Invoice Amount ({current_year_asin})'] - row[f'Invoice Amount ({previous_year_asin})']) / row[f'Invoice Amount ({previous_year_asin})'] * 100) 
                        if row[f'Invoice Amount ({previous_year_asin})'] != 0 else (100 if row[f'Invoice Amount ({current_year_asin})'] > 0 else 0), axis=1
                    )
                    
                    # Reorder columns
                    cols = ['Asin']

                    if 'Category' in asin_comparison.columns:
                        cols.append('Category')
                    
                    cols += ['Brand',
                             f'Quantity ({previous_year_asin})', f'Quantity ({current_year_asin})', 'Qty Difference', 'Qty % Change',
                             f'Invoice Amount ({previous_year_asin})', f'Invoice Amount ({current_year_asin})', 'Amount Difference', 'Amount % Change'
                    ]
                    
                    asin_comparison = asin_comparison[cols]
                    
                    # Sort by current year quantity descending
                    asin_comparison = asin_comparison.sort_values(by=f'Quantity ({current_year_asin})', ascending=False)
                    
                    # Add Grand Total row
                    grand_total_asin_dict = {
                        'Asin': ['Grand Total'],
                        'Brand': [''],
                        f'Quantity ({previous_year_asin})': [asin_comparison[f'Quantity ({previous_year_asin})'].sum()],
                        f'Quantity ({current_year_asin})': [asin_comparison[f'Quantity ({current_year_asin})'].sum()],
                        'Qty Difference': [asin_comparison['Qty Difference'].sum()],
                        'Qty % Change': [
                            (asin_comparison[f'Quantity ({current_year_asin})'].sum() - asin_comparison[f'Quantity ({previous_year_asin})'].sum()) / 
                            asin_comparison[f'Quantity ({previous_year_asin})'].sum() * 100 if asin_comparison[f'Quantity ({previous_year_asin})'].sum() != 0 else 0
                        ],
                        f'Invoice Amount ({previous_year_asin})': [asin_comparison[f'Invoice Amount ({previous_year_asin})'].sum()],
                        f'Invoice Amount ({current_year_asin})': [asin_comparison[f'Invoice Amount ({current_year_asin})'].sum()],
                        'Amount Difference': [asin_comparison['Amount Difference'].sum()],
                        'Amount % Change': [
                            (asin_comparison[f'Invoice Amount ({current_year_asin})'].sum() - asin_comparison[f'Invoice Amount ({previous_year_asin})'].sum()) / 
                            asin_comparison[f'Invoice Amount ({previous_year_asin})'].sum() * 100 if asin_comparison[f'Invoice Amount ({previous_year_asin})'].sum() != 0 else 0
                        ]
                    }
                    if 'Category' in asin_comparison.columns:
                        grand_total_asin_dict['Category'] = ['']
                    
                    grand_total_asin = pd.DataFrame(grand_total_asin_dict)
                    asin_comparison = pd.concat([asin_comparison, grand_total_asin], ignore_index=True)
                    
                    # Format display dataframe
                    display_asin_comparison = asin_comparison.copy()
                    display_asin_comparison[f'Quantity ({previous_year_asin})'] = display_asin_comparison[f'Quantity ({previous_year_asin})'].apply(lambda x: f"{x:,.0f}")
                    display_asin_comparison[f'Quantity ({current_year_asin})'] = display_asin_comparison[f'Quantity ({current_year_asin})'].apply(lambda x: f"{x:,.0f}")
                    display_asin_comparison['Qty Difference'] = display_asin_comparison['Qty Difference'].apply(lambda x: f"{x:+,.0f}")
                    display_asin_comparison['Qty % Change'] = display_asin_comparison['Qty % Change'].apply(lambda x: f"{x:+.2f}%")
                    display_asin_comparison[f'Invoice Amount ({previous_year_asin})'] = display_asin_comparison[f'Invoice Amount ({previous_year_asin})'].apply(lambda x: f"₹{x:,.2f}")
                    display_asin_comparison[f'Invoice Amount ({current_year_asin})'] = display_asin_comparison[f'Invoice Amount ({current_year_asin})'].apply(lambda x: f"₹{x:,.2f}")
                    display_asin_comparison['Amount Difference'] = display_asin_comparison['Amount Difference'].apply(lambda x: f"₹{x:+,.2f}")
                    display_asin_comparison['Amount % Change'] = display_asin_comparison['Amount % Change'].apply(lambda x: f"{x:+.2f}%")
                    
                    # Show summary metrics
                    st.markdown(f"### Comparison: {current_year_asin} vs {previous_year_asin}")
                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                    
                    total_qty_change_asin = asin_comparison.iloc[-1]['Qty Difference']
                    total_qty_pct_asin = asin_comparison.iloc[-1]['Qty % Change']
                    total_amt_change_asin = asin_comparison.iloc[-1]['Amount Difference']
                    total_amt_pct_asin = asin_comparison.iloc[-1]['Amount % Change']
                    
                    with metric_col1:
                        st.metric("Total Qty Change", f"{total_qty_change_asin:+,.0f}", f"{total_qty_pct_asin:+.2f}%")
                    with metric_col2:
                        st.metric("Total Amount Change", f"₹{total_amt_change_asin:+,.0f}", f"{total_amt_pct_asin:+.2f}%")
                    with metric_col3:
                        st.metric(f"Unique ASINs in {current_year_asin}", f"{len(current_year_data_asin['Asin'].dropna().unique()):,}")
                    with metric_col4:
                        st.metric(f"Unique ASINs in {previous_year_asin}", f"{len(previous_year_data_asin['Asin'].dropna().unique()):,}")
                    
                    st.dataframe(display_asin_comparison, use_container_width=True, height=600)
                    
                    # Download link (native download button)
                    render_download_button(asin_comparison, f"asin_comparison_{current_year_asin}_vs_{previous_year_asin}.xlsx", "Download ASIN Comparison Excel", key="asin_comp_dl")
                else:
                    st.warning("⚠️ Need at least 2 years of data for comparison. Please upload data from multiple years.")
            else:
                st.warning("⚠️ Need at least 2 years of data for comparison. Please upload data from multiple years.")
        st.info("👈 **Ready!** Adjust filters in the sidebar or export results.")
else:
    # Landing page with instructions
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style='text-align: center; padding: 2rem;'>
            <h2>👋 Welcome to Sales Data Analysis Dashboard</h2>
            <p style='font-size: 1.1rem; color: #666;'>
                Upload your files to get started with comprehensive sales analysis
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("""
        ### 🚀 Getting Started
        
        Welcome to the **Snaphire Amazon Analysis Dashboard**. This tool allows you to process 
        multiple B2B/B2C transaction reports and generate comprehensive sales insights.
        
        **Step 1:** Upload Data
        - Upload your **ZIP files** containing Amazon transaction reports.
        - Upload your **Product Master (PM)** Excel file for brand/manager mapping.
        
        **Step 2:** Trigger Analysis
        - Click the **🚀 Start Data Analysis** button in the sidebar.
        - Wait for the progress bar to complete (for 50+ files, this may take a few minutes).
        
        **Step 3:** Explore Insights
        - Use the **📊 Dashboard Tabs** to navigate through summary statistics, brand analysis, 
          ASIN breakdowns, and Year-over-Year comparisons.
          
        ### ✨ Key Features
        
        | Feature | Description |
        |---------|-------------|
        | 📈 **YOY Comparison** | Compare sales metrics between any two years |
        | 🏢 **Brand Analysis** | Performance breakdowns by brand and manager |
        | 📦 **ASIN Analysis** | Detailed product-level shipment data |
        | 🚀 **High Volume Mode** | Optimized processing for 50+ file uploads |
        
        """)
        
        st.info("👈 **Ready to begin?** Upload your files using the sidebar on the left!")
