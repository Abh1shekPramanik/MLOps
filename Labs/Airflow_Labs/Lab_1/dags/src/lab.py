import pandas as pd
from sklearn.preprocessing import MinMaxScaler
# from sklearn.cluster import KMeans
# from kneed import KneeLocator
from sklearn.mixture import GaussianMixture
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.model_selection import train_test_split
import pickle
import os
import base64

# Columns used for clustering
FEATURES = [
    "sepal length (cm)",
    "sepal width (cm)",
    "petal length (cm)",
    "petal width (cm)",
]

K_RANGE = range(1,11)

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "model")
SCALER_FILENAME = "scaler.sav"

# Original Iris data from the UCI Machine Learning Repository
IRIS_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
SPECIES = ["setosa", "versicolor", "virginica"]

def download_data():
    """
    Downloads the Iris dataset, splits it 80/20 (stratified by species),
    and saves data/file.csv (train) and data/test.csv (test).
    Falls back to scikit-learn's bundled copy if the download fails.
    """
    try:
        print(f"Downloading Iris dataset from {IRIS_URL}")
        df = pd.read_csv(IRIS_URL, header=None, names=FEATURES + ["species"]).dropna()
        df["species"] = df["species"].str.replace("Iris-", "", regex=False)
        source = "UCI repository"
    except Exception as e:
        print(f"Download failed ({e}); using scikit-learn's bundled copy instead")
        from sklearn.datasets import load_iris
        iris = load_iris(as_frame=True)
        df = iris.frame[FEATURES].copy()
        df["species"] = iris.target.map(dict(enumerate(SPECIES)))
        source = "scikit-learn"

    if len(df) != 150:
        raise ValueError(f"Expected 150 rows, got {len(df)}")

    train_df, test_df = train_test_split(df, test_size=0.2, stratify=df["species"], random_state=42)

    data_dir = os.path.join(os.path.dirname(__file__), "../data")
    os.makedirs(data_dir, exist_ok=True)
    train_df.to_csv(os.path.join(data_dir, "file.csv"), index=False)
    test_df.to_csv(os.path.join(data_dir, "test.csv"), index=False)
    print(f"Saved {len(train_df)} training rows to file.csv and {len(test_df)} test rows to test.csv (source: {source})")
    return {"train_rows": len(train_df), "test_rows": len(test_df)}

def load_data():
    """
    Loads the Iris flower dataset from a CSV file, serializes it,
    and returns the serialized data.
    Returns:
        str: Base64-encoded serialized data (JSON-safe).
    """
    print("Loading Iris dataset")
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/file.csv"))
    serialized_data = pickle.dumps(df)                    # bytes
    return base64.b64encode(serialized_data).decode("ascii")  # JSON-safe string

def data_preprocessing(data_b64: str):
    """
    Deserializes base64-encoded pickled data, performs preprocessing,
    and returns base64-encoded pickled clustered data.
    """
    # decode -> bytes -> DataFrame
    data_bytes = base64.b64decode(data_b64)
    df = pickle.loads(data_bytes)

    df = df.dropna()
    clustering_data = df[FEATURES]

    min_max_scaler = MinMaxScaler()
    clustering_data_minmax = min_max_scaler.fit_transform(clustering_data)

    # Save the scaler so load_model_elbow can apply the same transform to test data
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(os.path.join(MODEL_DIR, SCALER_FILENAME), "wb") as f:
        pickle.dump(min_max_scaler, f)

    # bytes -> base64 string for XCom
    clustering_serialized_data = pickle.dumps(clustering_data_minmax)
    return base64.b64encode(clustering_serialized_data).decode("ascii")


def build_save_model(data_b64: str, filename: str):
    """
    Fits a Gaussian Mixture Model for each number of clusters in K_RANGE,
    picks the one with the lowest BIC (Bayesian Information Criterion),
    and saves it. Returns the list of BIC scores (JSON-serializable).
    """
    # decode -> bytes -> numpy array
    data_bytes = base64.b64decode(data_b64)
    data = pickle.loads(data_bytes)

    gmm_kwargs = {"covariance_type": "full", "n_init": 5, "max_iter": 300, "random_state": 42}
    bic_scores = []
    models = {}
    for k in K_RANGE:
        gmm = GaussianMixture(n_components=k, **gmm_kwargs)
        gmm.fit(data)
        bic_scores.append(float(gmm.bic(data)))
        models[k] = gmm
 
    # Lower BIC is better: it rewards good fit but penalizes extra components
    best_k = list(K_RANGE)[bic_scores.index(min(bic_scores))]
    print(f"Saving Gaussian Mixture model with {best_k} components")
 
    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(os.path.join(MODEL_DIR, filename), "wb") as f:
        pickle.dump(models[best_k], f)
 
    return bic_scores  # list of floats is JSON-safe


def load_model_elbow(filename: str, bic_scores: list):
    """
    Loads the saved Gaussian Mixture model and scaler, reports the best number
    of clusters (lowest BIC), and returns the predicted cluster (as a plain int)
    for the first row of test.csv.
    """
    with open(os.path.join(MODEL_DIR, filename), "rb") as f:
        loaded_model = pickle.load(f)
    with open(os.path.join(MODEL_DIR, SCALER_FILENAME), "rb") as f:
        scaler = pickle.load(f)
 
    best_k = list(K_RANGE)[bic_scores.index(min(bic_scores))]
    print(f"Optimal no. of clusters (lowest BIC): {best_k}")
 
    
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/test.csv"))
    df_scaled = scaler.transform(df[FEATURES])
    pred = loaded_model.predict(df_scaled)[0]
    probs = loaded_model.predict_proba(df_scaled)[0]
    print(f"Cluster membership probabilities: {[round(float(p), 3) for p in probs]}")
 
    return int(pred)


def evaluate_model(filename: str):
    """
    Scores the saved model on data/test.csv, which it never saw during training.
    - Adjusted Rand Index: match with the true species (1.0 = perfect, ~0 = random).
    - Silhouette score: how well separated the clusters are (closer to 1 = better).
    """
    with open(os.path.join(MODEL_DIR, filename), "rb") as f:
        model = pickle.load(f)
    with open(os.path.join(MODEL_DIR, SCALER_FILENAME), "rb") as f:
        scaler = pickle.load(f)

    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/test.csv"))
    X = scaler.transform(df[FEATURES])
    cluster_labels = model.predict(X)

    n_clusters = len(set(cluster_labels))
    ari = adjusted_rand_score(df["species"], cluster_labels)
    silhouette = silhouette_score(X, cluster_labels) if n_clusters > 1 else None
    print(f"Clusters found: {n_clusters}, ARI: {ari:.3f}, silhouette: {silhouette}")

    return {
        "n_clusters": int(n_clusters),
        "adjusted_rand_index": round(float(ari), 3),
        "silhouette": round(float(silhouette), 3) if silhouette is not None else None,
    }