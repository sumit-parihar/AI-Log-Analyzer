import streamlit as st
import pandas as pd
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# modules
from src.data_preprocessing import parse_logs
from src.feature_engineering import feature_engineering
from src.ml_anomaly_detection import detect_anomalies

# Google Sheets imports
import gspread
from google.oauth2.service_account import Credentials

# Initialize Sheets credentials
SERVICE_ACCOUNT_FILE = os.path.join("data", "valued-clarity-489113-n4-ce823f03059b.json")  # service account JSON
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)
gc = gspread.authorize(credentials)

# Your Google Sheet ID containing historic logs
SHEET_ID = "170qMeVXvZlbCHvEUIGDCwr-jxtZWMYr1r-balW4cc5Y"  # replace with actual Sheet ID
sheet = gc.open_by_key(SHEET_ID).sheet1  # first worksheet

# Load historic logs from the Sheet
try:
    data = sheet.get_all_records()
    historic_df = pd.DataFrame(data)
    st.write(f"Loaded {len(historic_df)} historic logs from Google Sheets.")
except Exception as e:
    st.warning(f"Failed to load historic logs: {e}")
    historic_df = pd.DataFrame()

# Ensure timestamp column is datetime
if not historic_df.empty and 'timestamp' in historic_df.columns:
    historic_df['timestamp'] = pd.to_datetime(historic_df['timestamp'], errors='coerce')

# Upload files from user
uploaded_file = st.file_uploader("Upload your raw Apache log file", type=['log', 'txt'])
if uploaded_file is not None:
    st.success(f"Uploaded file: {uploaded_file.name}")

    # Preprocessing of data
    df_uploaded, df_combined = parse_logs(uploaded_file, historic_df = historic_df)
    st.write(f"Uploaded logs: {len(df_uploaded)} rows")
    st.write(f"Total combined logs for model: {len(df_combined)} rows")

    # Ensure uploaded timestamps are datetime
    if not df_uploaded.empty and 'timestamp' in df_uploaded.columns:
        df_uploaded['timestamp'] = pd.to_datetime(df_uploaded['timestamp'], errors='coerce')

    if not df_combined.empty and 'timestamp' in df_combined.columns:
        df_combined['timestamp'] = pd.to_datetime(df_combined['timestamp'], errors='coerce')

    # Feature Engineering
    df_combined_features, df_uploaded_features = feature_engineering(
        df_combined, uploaded_len = len(df_uploaded)
    )
    st.write("Feature Engineering Completed")

    # Anomaly Detection
    contamination = st.slider("Anomaly Contamination(relative to your file)",
                              0.01,
                              0.2,
                              0.05,
                              0.01
                              )
    df_anomalies = detect_anomalies(df_combined_features, df_uploaded_features, contamination = contamination)
    st.write(f"Detected {df_anomalies['anomaly'].sum()} anomalies in your {uploaded_file.name}")
    st.dataframe(df_anomalies[df_anomalies['anomaly'] == 1])

    # Option to download records
    # Create mask for uploaded file rows
    uploaded_mask = df_anomalies.index >= (len(df_combined_features) - len(df_uploaded_features))

    # Select only uploaded file anomalies
    df_uploaded_anomalies = df_anomalies[uploaded_mask & (df_anomalies['anomaly'] == 1)]

    # Then download button
    csv = df_uploaded_anomalies.to_csv(index=False).encode('utf-8')
    st.download_button("Download Anomalies CSV", csv, file_name='anomalies.csv')

    st.write("---")  # optional separator

    if not df_uploaded.empty:
        if st.button("Add uploaded logs to historical data"):
            # Prepare DataFrame for Sheets
            df_to_append = df_uploaded.copy()
            if 'timestamp' in df_to_append.columns:
                df_to_append['timestamp'] = df_to_append['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            try:
                sheet.append_rows(df_to_append.values.tolist(), value_input_option='RAW')
                st.success("Uploaded logs appended to Google Sheet for future use.")
            except Exception as e:
                st.error(f"Failed to upload logs to Google Sheet: {e}")