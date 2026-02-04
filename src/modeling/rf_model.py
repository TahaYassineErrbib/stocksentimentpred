from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Tuple, List

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, confusion_matrix


@dataclass
class RFResult:
    model: RandomForestClassifier
    metrics: Dict[str, float]
    feature_names: List[str]

def best_threshold_for_f1(y_true, proba):
    best_t, best_f1 = 0.5, -1.0
    for t in [i/100 for i in range(20, 81)]:  # 0.20 .. 0.80
        pred = (proba >= t).astype(int)
        f1 = f1_score(y_true, pred)
        if f1 > best_f1:
            best_f1, best_t = f1, t
    return best_t, best_f1

def train_rf_classifier(
    df: pd.DataFrame,
    target_col: str = "direction_next",
    test_ratio: float = 0.2,
    random_state: int = 42,
) -> RFResult:
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Define feature columns (keep it explicit)
    feature_cols = [
        "ma_7","ma_14","vol_7","vol_14","rsi_14","macd","macd_signal","macd_hist",
        "post_count","mean_sentiment","mean_influence","sum_influence","mean_score",
        "Volume",  # optional but useful
    ]
    # Some columns might be missing depending on your merge; filter safely
    feature_cols = [c for c in feature_cols if c in df.columns]

    X = df[feature_cols].astype(float).values
    y = df[target_col].astype(int).values

    split = int(len(df) * (1 - test_ratio))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=None,
        min_samples_leaf=2,
        random_state=random_state,
        n_jobs=-1,
        class_weight="balanced_subsample",
    )
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)

    metrics: Dict[str, float] = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "f1": float(f1_score(y_test, pred)),
    }
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_test, proba))
    except Exception:
        metrics["roc_auc"] = float("nan")

    # --- Threshold tuning for better F1 ---
    t_best, f1_best = best_threshold_for_f1(y_test, proba)
    pred_best = (proba >= t_best).astype(int)
    metrics["best_threshold"] = float(t_best)
    metrics["f1_best_threshold"] = float(f1_best)

    cm = confusion_matrix(y_test, pred)
    metrics["tn"] = float(cm[0, 0])
    metrics["fp"] = float(cm[0, 1])
    metrics["fn"] = float(cm[1, 0])
    metrics["tp"] = float(cm[1, 1])

    cm_best = confusion_matrix(y_test, pred_best)
    metrics["tn_best"] = float(cm_best[0, 0])
    metrics["fp_best"] = float(cm_best[0, 1])
    metrics["fn_best"] = float(cm_best[1, 0])
    metrics["tp_best"] = float(cm_best[1, 1])


    return RFResult(model=model, metrics=metrics, feature_names=feature_cols)
