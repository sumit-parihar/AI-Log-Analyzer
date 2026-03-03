# pandas for DataFrame handling
import pandas as pd

# re for regex parsing of logs
import re

# datetime for timestamp conversion
from datetime import datetime

# tqdm for progress bar (optional, useful for large logs)
from tqdm import tqdm

log_pattern = re.compile(
        r'^'
        r'(?P<ip>(?:\d{1,3}(?:\.\d{1,3}){3}|[A-Fa-f0-9:]+)) '
        r'- - '
        r'\[(?P<date>\d{2}/[A-Za-z]{3}/\d{4}:'
        r'\d{2}:\d{2}:\d{2} [+-]\d{4})\] '
        r'"(?P<method>[A-Z]+) '
        r'(?P<path>\S+) '
        r'(?P<protocol>HTTP/\d(?:\.\d)?)" '
        r'(?P<status>\d{3}) '
        r'(?P<size>\d+|-) '
        r'"(?P<referer>[^"]*)" '
        r'"(?P<agent>[^"]*)"'
        r'$'
    )

def parse_apache_log_line(line):

    match = re.match(log_pattern, line)
    if match:
        data = match.groupdict()
        try:
            data['timestamp'] = datetime.strptime(
                data['date'].split()[0], "%d/%b/%Y:%H:%M:%S"
            )
        except Exception:
            return None  # Skip lines with invalid timestamp
        data['bytes'] = 0 if data['size'] == '-' else int(data['size'])
        data['status'] = int(data['status'])

        return {
            'ip': data['ip'],
            'timestamp': data['timestamp'],
            'method': data['method'],
            'url': data['path'],
            'protocol': data['protocol'],
            'status': data['status'],
            'bytes': data['bytes'],
            'referrer': data['referer'],
            'user_agent': data['agent']
        }
    else:
        return None

# =========================
# Read Sample Log File for Testing
# Change to complete raw data after testing
# =========================

# raw_log_file = 'data/sample/wordpress_sample.log'  # SMALL test logs
raw_log_file = 'data/raw/wordpress_access.log'       # Real raw data
with open(raw_log_file, 'r') as f:
    lines = f.readlines()

print(f"Total lines in Logs: {len(lines)}")

# =========================
# Parse All Lines
# =========================

log_entries = []
for line in tqdm(lines, desc='Parsing log files'):
    parsed = parse_apache_log_line(line)
    if parsed:
        log_entries.append(parsed)

print(f"Successfully parsed lines: {len(log_entries)}")

# =========================
# Convert to DataFrame
# =========================

df = pd.DataFrame(log_entries)

# Basic Cleaning
df.drop_duplicates(keep='last', inplace=True)
df['user_agent'] = df['user_agent'].str.lower()

print('Sample Dataframes Columns:')
# print(df.columns.tolist())
print(df.head())
print(df.info())

# =========================
# Save Processed CSV for Sample
# =========================

processed_csv_path = "data/processed/cleaned_logs.csv"
df.to_csv(processed_csv_path, index=False)

print(f"Processed sample CSV saved at: {processed_csv_path}")