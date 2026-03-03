# imports
import pandas as pd

def feature_engineering(df, uploaded_len = None):

    # Use this file for training only
    """
    {
    df = pd.read_csv('data/processed/cleaned_sample_logs.csv', parse_dates=['timestamp'])   # sample logs
    df = pd.read_csv('data/processed/cleaned_access_logs.csv', parse_dates=['timestamp'])   # actual logs
    }
    """

    # 1️ Time-based features
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['minute'] = df['timestamp'].dt.minute

    # 2️ URL path depth
    df['url_path_depth'] = df['url'].apply(lambda x: x.count('/'))

    # 3 Protocol category
    protocol_dummies = pd.get_dummies(df['protocol'], prefix='protocol')
    df = pd.concat([df, protocol_dummies], axis=1)

    # 4️ Status category
    df['status_type'] = df['status'].apply(lambda x: 'success' if 200 <= x < 300
                                           else ('client_error' if 400 <= x < 500 else 'server_error'))

    status_dummies = pd.get_dummies(df['status_type'], prefix='status')
    df = pd.concat([df, status_dummies], axis=1)

    # 5 One-hot encoding for HTTP method
    df = pd.get_dummies(df, columns=['method'], prefix='method')

    # 6 Only include a simple numeric feature for referrer
    df['has_referrer'] = (df['referrer'] != '-').astype(int)

    # 7 Detect bots from user agent
    df['is_bot'] = df['user_agent'].str.contains('bot|crawl|spider', case=False, regex=True).astype(int)

    # 8 Browser extraction (simplified)
    df['browser'] = df['user_agent'].str.extract(r'(chrome|firefox|safari|edge|opera|msie|wordpress)', expand=False).fillna('other')

    # 9 Encode browser
    df = pd.get_dummies(df, columns=['browser'], prefix='browser')

    """
    {
        # Check processed features
        print(df.head())
        print(df.info())
    
        # featured_csv_path = "data/processed/featured_sample_logs.csv"   # sample logs
        featured_csv_path = "data/processed/featured_access_logs.csv"   # actual logs
        df.to_csv(featured_csv_path, index=False)
        print(f"Feature Engineering Complete. Saved to {featured_csv_path}"
    }
    """
    # =========================
    # Slice uploaded features if length provided
    # =========================

    if uploaded_len:
        df_uploaded_features = df.iloc[-uploaded_len:]
    else:
        df_uploaded_features = df.copy()

    return df_uploaded_features, df