import pandas as pd

# Load cleaned CSV
df = pd.read_csv('data/processed/cleaned_logs.csv', parse_dates=['timestamp'])

# 1️ Time-based features
df['hour'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.dayofweek
df['minute'] = df['timestamp'].dt.minute

# 2️ URL path depth
df['url_path_depth'] = df['url'].apply(lambda x: x.count('/'))

# 3️ Status category
df['status_type'] = df['status'].apply(lambda x: 'success' if 200 <= x < 300
                                       else ('client_error' if 400 <= x < 500 else 'server_error'))

# 4️ One-hot encoding for HTTP method
df = pd.get_dummies(df, columns=['method'], prefix='method')

# 5️ Detect bots from user agent
df['is_bot'] = df['user_agent'].str.contains('bot|crawl|spider', case=False, regex=True).astype(int)

# 6️ Browser extraction (simplified)
df['browser'] = df['user_agent'].str.extract(r'(chrome|firefox|safari|edge|opera|msie)', expand=False).fillna('other')

# 7️ Encode browser
df = pd.get_dummies(df, columns=['browser'], prefix='browser')

# Check processed features
print(df.head())
print(df.info())