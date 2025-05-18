import os
import dropbox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import requests

import sys


# ▼ バッファリングの無効化（リアルタイム出力）
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print(f"📝 Pythonバージョン: {sys.version}")

# ▼ JST（日本標準時）のタイムゾーン設定
JST = timezone(timedelta(hours=9))

# ▼ 現在の日本時間を取得する関数
def get_japan_time():
    return datetime.now(JST)

# ▼ 設定する日付（テスト用）
TEST_DATE = "2"  # 例: "20250517"（空欄の場合はリアルタイム）

# ▼ 設定する時刻（テスト用）
TEST_TIMES = []  # 例: ["1000", "1010", "1020"]（空欄の場合はリアルタイム）

# ▼ 設定値
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

# ▼ 環境変数から認証情報を取得
CLIENT_ID = os.environ.get('DROPBOX_CLIENT_ID')
CLIENT_SECRET = os.environ.get('DROPBOX_CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN')
ACCESS_TOKEN_FILE = '/tmp/access_token.txt'

# ▼ アクセストークンをリフレッシュする関数
def refresh_access_token():
    url = 'https://api.dropbox.com/oauth2/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': REFRESH_TOKEN
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        with open(ACCESS_TOKEN_FILE, 'w') as f:
            f.write(access_token)
        print('✅ アクセストークンをリフレッシュしました。')
        return access_token
    except Exception as e:
        print(f'🚫 アクセストークンのリフレッシュに失敗しました: {e}')
        exit(1)

# ▼ アクセストークンを取得またはリフレッシュ
if os.path.exists(ACCESS_TOKEN_FILE):
    with open(ACCESS_TOKEN_FILE, 'r') as f:
        ACCESS_TOKEN = f.read().strip()
else:
    ACCESS_TOKEN = refresh_access_token()

# ▼ Dropboxクライアントの初期化
try:
    dbx = dropbox.Dropbox(ACCESS_TOKEN)
    dbx.users_get_current_account()
    print('✅ Dropboxに接続しました。')
except dropbox.exceptions.AuthError:
    print('⚠️ アクセストークンが無効です。リフレッシュを試みます...')
    ACCESS_TOKEN = refresh_access_token()
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

# ▼ ファイルダウンロード関数
def download_csv_from_dropbox(file_name):
    try:
        print(f"🔍 ファイルダウンロードを試みます: {file_name}")
        dropbox_path = f'/デイトレファイル/{file_name}'
        local_path = f'/tmp/{file_name}'
        os.makedirs('/tmp', exist_ok=True)
        with open(local_path, 'wb') as f:
            metadata, res = dbx.files_download(path=dropbox_path)
            f.write(res.content)
        print(f"✅ ダウンロード完了: {dropbox_path} -> {local_path}")
        return local_path
    except Exception as e:
        print(f"🚫 ファイルのダウンロードエラー: {e}")
        return None

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

        print(output_df)

    except Exception as e:
        print(f"データ読み込みエラー: {e}")

# ▼ 24時間監視ループ
while True:
    try:
        # 日本時間で日付と時刻を取得
        today_date = TEST_DATE if TEST_DATE else get_japan_time().strftime("%Y%m%d")
        current_time = f"{TEST_TIMES[0]:04d}" if TEST_TIMES else get_japan_time().strftime("%H%M")
        
        # ファイル名を日本時間で生成
        file_name = f"kabuteku{today_date}_{current_time}.csv"
        print(f"📂 処理対象ファイル: {file_name}")
        
        # ファイルをダウンロードして分析
        file_path = download_csv_from_dropbox(file_name)
        if file_path:
            print(f"🔎 分析を開始します: {file_path}")
            analyze_and_display_filtered_signals(file_path)
        else:
            print(f"🚫 ファイルが見つかりません: {file_name}")
        
        # 同じ時刻に複数回処理しないように1分待機
        print("⏲️ 1分間待機中...")
        time.sleep(60)

    except Exception as e:
        print(f"🚫 メインループエラー: {e}")
