import pandas as pd
import streamlit as st

st.set_page_config(page_title="TSLA Sentiment + Prediction", layout="wide")

st.markdown(
    """
<style>
div.block-container { padding-top: 1.2rem; padding-bottom: 1.2rem; }
h1 { margin-bottom: 0.2rem; }
div[data-testid="stMetric"] { padding: 0.6rem 0.8rem; border-radius: 10px; background: #f6f7f9; }
div[data-testid="stMetric"] * { color: #111 !important; }
div[data-testid="stMetric"] > div { gap: 0.1rem; }
</style>
""",
    unsafe_allow_html=True,
)

st.title("TSLA: Reddit Sentiment + Price Indicators + Predictions")
st.caption("Live dashboard from streaming outputs (Mongo + CSV refresh loop).")

df = pd.read_csv("data/processed/predictions_daily.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.sort_values("timestamp")

latest = df.iloc[-1]
prev = df.iloc[-2] if len(df) > 1 else latest

delta_close = latest["Close"] - prev["Close"]
delta_posts = int(latest.get("post_count", 0) - prev.get("post_count", 0))

sent_val = float(latest.get("mean_sentiment", 0.0))
if sent_val >= 0.15:
    sentiment_label = "Positive"
elif sent_val <= -0.15:
    sentiment_label = "Negative"
else:
    sentiment_label = "Neutral"

metrics = st.columns([1, 1, 1, 1, 2])
metrics[0].metric("Close (latest)", f"{latest['Close']:.2f}", f"{delta_close:+.2f}")
metrics[1].metric("Reddit posts / day", int(latest.get("post_count", 0)), f"{delta_posts:+d}")
metrics[2].metric("RF P(up)", f"{latest['rf_proba_up']:.3f}")
metrics[3].metric("LSTM P(up)", f"{latest.get('lstm_proba_up', float('nan')):.3f}")
metrics[4].metric("Last update (UTC)", latest["timestamp"].strftime("%Y-%m-%d %H:%M"))

with st.expander("KPI definitions"):
    st.markdown(
        """
- **Close (latest):** last TSLA closing price. Delta is change vs previous day.
- **Reddit posts / day:** number of Reddit posts captured for that day. Delta is change vs previous day.
- **RF P(up):** Random Forest probability that next day’s price goes up.
- **LSTM P(up):** LSTM probability that next day’s price goes up.
- **Last update (UTC):** timestamp of the most recent row in the dataset.
"""
    )

st.markdown(f"**Sentiment mood:** {sentiment_label} (mean_sentiment={sent_val:+.3f})")

col_left, col_right = st.columns([2, 1], gap="large")

with col_left:
    st.subheader("Price")
    st.line_chart(df.set_index("timestamp")[["Close"]])

    st.subheader("Model probabilities")
    prob_cols = [c for c in ["rf_proba_up", "lstm_proba_up"] if c in df.columns]
    st.line_chart(df.set_index("timestamp")[prob_cols])

with col_right:
    st.subheader("Reddit signals")
    cols = [c for c in ["mean_sentiment", "mean_influence", "sum_influence", "post_count"] if c in df.columns]
    if cols:
        st.line_chart(df.set_index("timestamp")[cols])

    with st.expander("Latest rows"):
        st.dataframe(df.tail(25), use_container_width=True)
