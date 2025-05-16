import pandas as pd
import numpy as np
import os
import datetime
import time

# ▼ 設定値（自由に変更可能）
RSI_PERIOD = 14
RSI_BUY_THRESHOLD = 45
RSI_SELL_THRESHOLD = 55
RSI_TREND_BUY_THRESHOLD = 40
RSI_TREND_SELL_THRESHOLD = 60
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
BOARD_BALANCE_BUY_THRESHOLD = 1.0
BOARD_BALANCE_SELL_THRESHOLD = 1.0
TREND_LOOKBACK = 5
PRICE_MAX_THRESHOLD = 20000
PRICE_MIN_THRESHOLD = 500
SUPPORT_THRESHOLD = 1.05
RESISTANCE_THRESHOLD = 0.95
VOLATILITY_LOOKBACK = 26

# ▼ 設定する日付（テスト用）
TEST_DATE = ""  # 例: "20250517"（空欄の場合はリアルタイム）

# ▼ 設定する時刻（テスト用）
TEST_TIMES = []  # 例: ["1000", "1010", "1020"]（空欄の場合はリアルタイム）

# ▼ 改善版 RSI計算関数
def calculate_rsi(prices, period=14):
    deltas = prices.diff()
    gains = deltas.where(deltas > 0, 0)
    losses = -deltas.where(deltas < 0, 0)

    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else np.nan

# ▼ シグナル判定関数
def analyze_and_display_filtered_signals(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace("　", "").str.replace(" ", "")

        price_columns = df.columns[31:57]

        df_filtered = df[(df[price_columns].astype(float).max(axis=1) <= PRICE_MAX_THRESHOLD) &
                         (df[price_columns].astype(float).min(axis=1) >= PRICE_MIN_THRESHOLD)]

        output_data = []

        for _, row in df_filtered.iterrows():
            try:
                code = row["銘柄コード"]
                name = row["銘柄名称"]
                prices = pd.Series(row[price_columns].values.astype(float))

                current_price = float(row["現在値"])
                high_price = float(row["高値"])
                low_price = float(row["安値"])

                rsi = calculate_rsi(prices, period=RSI_PERIOD)

                ema_short = prices.ewm(span=MACD_SHORT, adjust=False).mean()
                ema_long = prices.ewm(span=MACD_LONG, adjust=False).mean()
                macd = ema_short - ema_long
                macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
                macd_hist = macd.iloc[-1] - macd_signal.iloc[-1]

                short_trend = prices[-TREND_LOOKBACK:].mean()
                long_trend = prices.mean()

                signal = "中立"
                if rsi > RSI_TREND_BUY_THRESHOLD and macd_hist > 0 and current_price > short_trend and short_trend > long_trend:
                    signal = "順張り買い目"
                elif rsi <= RSI_BUY_THRESHOLD and macd_hist > 0:
                    signal = "逆張り買い目"
                elif rsi < RSI_TREND_SELL_THRESHOLD and macd_hist < 0 and current_price < short_trend and short_trend < long_trend:
                    signal = "順張り売り目"
                elif rsi >= RSI_SELL_THRESHOLD and macd_hist < 0:
                    signal = "逆張り売り目"

                if signal == "中立":
                    continue

                score = 0
                if rsi <= RSI_BUY_THRESHOLD:
                    score += 2
                elif rsi > RSI_TREND_BUY_THRESHOLD:
                    score += 1
                if macd_hist > 0:
                    score += 1
                if current_price > short_trend and short_trend > long_trend:
                    score += 1
                if current_price < low_price * SUPPORT_THRESHOLD:
                    score += 1

                overall_rating = max(1, min(5, score))

                output_data.append({
                    "銘柄コード": code,
                    "銘柄名称": name,
                    "シグナル": signal,
                    "株価": current_price,
                    "総合評価": overall_rating
                })

            except Exception as e:
                print(f"データ処理エラー（{code}）: {e}")

        output_df = pd.DataFrame(output_data)
        signal_order = ["順張り買い目", "逆張り買い目", "順張り売り目", "逆張り売り目"]
        output_df["シグナル"] = pd.Categorical(output_df["シグナル"], categories=signal_order, ordered=True)
        output_df = output_df.sort_values(by=["シグナル", "総合評価"], ascending=[True, False])

        log_output = ""
        for signal in signal_order:
            log_output += f"■{signal}\n"
            filtered_df = output_df[output_df["シグナル"] == signal]
            if not filtered_df.empty:
                for _, row in filtered_df.iterrows():
                    log_output += f"{row['銘柄コード']} {row['銘柄名称']} 株価:{row['株価']}円 評価:{row['総合評価']}\n"
            log_output += "\n"
        
        print(log_output)

    except Exception as e:
        print(f"データ読み込みエラー: {e}")

# ▼ 24時間監視ループ
while True:
    today_date = TEST_DATE if TEST_DATE else datetime.datetime.now().strftime("%Y%m%d")
    current_time = datetime.datetime.now().strftime("%H%M")
    time_slots = TEST_TIMES if TEST_TIMES else [current_time]
    
    for time_slot in time_slots:
        file_name = f"kabuteku{today_date}_{time_slot}.csv"
        file_path = f"/mnt/data/{file_name}"
        
        if os.path.exists(file_path):
            print(f"📂 処理中ファイル: {file_name}")
            analyze_and_display_filtered_signals(file_path)
        else:
            print(f"🚫 ファイルが見つかりません: {file_name}")
        
        time.sleep(60)
    time.sleep(10)

