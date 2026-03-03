# =========================
# Imports
# =========================
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import numpy as np
def detect_anomalies(df_combined, df_uploaded, contamination=0.05):

    # =========================
    # Load Featured Data
    # =========================

    # Use this path for training only
    """
    {
        # input_csv = 'data/processed/featured_sample_logs.csv'     # sample logs
        input_csv = 'data/processed/featured_access_logs.csv'       # actual logs
        # input_csv = 'data/processed/featured_access_sample_logs.csv'
        df = pd.read_csv(input_csv, parse_dates=['timestamp'])
        
        print(f"Loaded {len(df)} rows from {input_csv}")
    }
    """

    # =========================
    # Prepare Data for ML
    # =========================

    # Drop non-numeric / textual columns that ML cannot use
    numeric_cols = df_combined.select_dtypes(include=['int64','float64','bool']).columns
    X_combined = df_combined[numeric_cols]

    # Optional: scale numeric features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_combined)

    print(f"Feature matrix shape: {X_scaled.shape}")

    # Initialize Isolation Forest
    clf = IsolationForest(
        n_estimators=100,       # Number of trees
        max_samples='auto',     # Use all samples
        contamination=contamination,     # Expected fraction of anomalies
        random_state=42         # Ensure reproducibility
    )

    # Fit model on scaled features
    clf.fit(X_scaled)

    # Get anomaly scores (decision function)
    scores = clf.decision_function(X_scaled)  # higher = normal, lower = anomalous

    # Extract uploaded rows scores
    uploaded_len = len(df_uploaded)
    scores_uploaded = scores[-uploaded_len:]

    # Apply contamination relative to uploaded rows only
    threshold = np.percentile(scores_uploaded, 100 * contamination)
    df_uploaded['anomaly'] = (scores_uploaded < threshold).astype(int)

    return df_uploaded