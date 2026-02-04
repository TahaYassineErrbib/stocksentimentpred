from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, confusion_matrix

import tensorflow as tf
from tensorflow.keras import layers, models, callbacks


@dataclass
class LSTMResult:
    model: tf.keras.Model
    scaler: StandardScaler
    metrics: Dict[str, float]
    feature_names: List[str]


def make_sequences(
    X: np.ndarray, y: np.ndarray, lookback: int
) -> Tuple[np.ndarray, np.ndarray]:
    Xs, ys = [], []
    for i in range(lookback, len(X)):
        Xs.append(X[i - lookback : i])
        ys.append(y[i])
    return np.array(Xs, dtype=np.float32), np.array(ys, dtype=np.int32)


def train_lstm_classifier(
    df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str = "direction_next",
    lookback: int = 30,
    test_ratio: float = 0.2,
    random_state: int = 42,
) -> LSTMResult:
    df = df.sort_values("timestamp").reset_index(drop=True)

    X = df[feature_cols].astype(float).values
    y = df[target_col].astype(int).values

    split = int(len(df) * (1 - test_ratio))
    X_train_raw, X_test_raw = X[:split], X[split:]
    y_train_raw, y_test_raw = y[:split], y[split:]

    # Scale features (fit only on train)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_raw)
    X_test_scaled = scaler.transform(X_test_raw)

    # Build sequences (note: sequences lose first `lookback` labels)
    X_train, y_train = make_sequences(X_train_scaled, y_train_raw, lookback)
    X_test, y_test = make_sequences(X_test_scaled, y_test_raw, lookback)

    # LSTM model
    tf.keras.utils.set_random_seed(random_state)

    model = models.Sequential(
        [
            layers.Input(shape=(lookback, X_train.shape[-1])),
            layers.LSTM(64, return_sequences=False),
            layers.Dropout(0.2),
            layers.Dense(32, activation="relu"),
            layers.Dense(1, activation="sigmoid"),
        ]
    )
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss="binary_crossentropy",
        metrics=[tf.keras.metrics.AUC(name="auc")],
    )

    es = callbacks.EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True)

    history = model.fit(
        X_train,
        y_train,
        validation_split=0.2,
        epochs=30,
        batch_size=32,
        callbacks=[es],
        verbose=0,
    )

    proba = model.predict(X_test, verbose=0).reshape(-1)
    pred = (proba >= 0.5).astype(int)

    metrics: Dict[str, float] = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "f1": float(f1_score(y_test, pred)),
    }
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_test, proba))
    except Exception:
        metrics["roc_auc"] = float("nan")

    cm = confusion_matrix(y_test, pred)
    metrics["tn"] = float(cm[0, 0])
    metrics["fp"] = float(cm[0, 1])
    metrics["fn"] = float(cm[1, 0])
    metrics["tp"] = float(cm[1, 1])

    metrics["train_samples"] = float(len(X_train))
    metrics["test_samples"] = float(len(X_test))

    return LSTMResult(model=model, scaler=scaler, metrics=metrics, feature_names=feature_cols)
