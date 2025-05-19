import os
import dropbox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import requests
import sys


print(f"📝 Pythonバージョン: {sys.version}")

# ▼ JST（日本標準時）のタイムゾーン設定
JST = timezone(timedelta(hours=9))

# ▼ 現在の日本時間を取得する関数
def get_japan_time():
    return datetime.now(JST)

# ▼ 設定する日付（テスト用）
TEST_DATE = ""  # 例: "20250517"（空欄の場合はリアルタイム）

# ▼ 設定する時刻（テスト用）
TEST_TIMES = []  # 例: ["1000", "1010", "1020"]（空欄の場合はリアルタイム）

# ▼ 設定値
RSI_PERIOD = 14  # RSIの計算に使用する期間（14期間）
RSI_BUY_THRESHOLD = 45  # 逆張り買いシグナルのRSI閾値（45以下で買い）
RSI_SELL_THRESHOLD = 55  # 逆張り売りシグナルのRSI閾値（55以上で売り）
RSI_TREND_BUY_THRESHOLD = 40  # 順張り買いシグナルのRSI閾値（40以上で買い）
RSI_TREND_SELL_THRESHOLD = 60  # 順張り売りシグナルのRSI閾値（60以下で売り）
MACD_SHORT = 12  # MACDの短期EMA期間（12期間）
MACD_LONG = 26  # MACDの長期EMA期間（26期間）
MACD_SIGNAL = 9  # MACDシグナルラインの期間（9期間）
BOARD_BALANCE_BUY_THRESHOLD = 1.2  # 板のバランス閾値（1.2以上で買い優勢）
BOARD_BALANCE_SELL_THRESHOLD = 0.8  # 板のバランス閾値（0.8以下で売り優勢）
TREND_LOOKBACK = 5  # トレンド判定に使用する直近期間（5本）
PRICE_MAX_THRESHOLD = 20000  # フィルタリングに使用する最大株価（20,000円）
PRICE_MIN_THRESHOLD = 500  # フィルタリングに使用する最小株価（500円）
SUPPORT_THRESHOLD = 1.05  # サポートライン閾値（安値の1.05倍以下で支持線割れと判断）
RESISTANCE_THRESHOLD = 0.95  # レジスタンスライン閾値（高値の0.95倍以上で抵抗線突破と判断）
VOLATILITY_LOOKBACK = 26  # ボラティリティ計算に使用する期間（26期間）
GAP_THRESHOLD = 0.015  # ギャップ判定に使用する閾値（2%以上のギャップ）

def calculate_rsi(prices, period=14):
    deltas = prices.diff()
    gains = deltas.where(deltas > 0, 0)
    losses = -deltas.where(deltas < 0, 0)

    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else np.nan

def analyze_and_display_filtered_signals(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace("　", "").str.replace(" ", "")

        price_columns = df.columns[31:57]

        output_data = []

        for _, row in df.iterrows():
            try:
                code = row["銘柄コード"]
                name = row["銘柄名称"]
                prices = pd.Series(row[price_columns].values.astype(float))

                current_price = float(row["現在値"])
                high_price = float(row["高値"])
                low_price = float(row["安値"])
                prev_close = float(row["前日終値"])
                open_price = float(row["始値"])
                best_bid_qty = float(row["最良買気配数量"])
                best_ask_qty = float(row["最良売気配数量"])
                margin_buy = float(row["信用買残"])
                margin_sell = float(row["信用売残"])

                # RSI計算
                rsi = calculate_rsi(prices, period=RSI_PERIOD)

                # MACD計算
                ema_short = prices.ewm(span=MACD_SHORT, adjust=False).mean()
                ema_long = prices.ewm(span=MACD_LONG, adjust=False).mean()
                macd = ema_short - ema_long
                macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
                macd_hist = macd.iloc[-1] - macd_signal.iloc[-1]

                # ギャップ計算
                gap_ratio = (open_price - prev_close) / prev_close

                # 信用残高比
                credit_ratio = margin_buy / margin_sell if margin_sell > 0 else np.nan

                # シグナル判定
                signal = "中立"
                if rsi > RSI_TREND_BUY_THRESHOLD and macd_hist > 0 and current_price > low_price * SUPPORT_THRESHOLD and best_bid_qty > best_ask_qty * BOARD_BALANCE_BUY_THRESHOLD:
                    signal = "順張り買い目"
                elif rsi <= RSI_BUY_THRESHOLD and macd_hist > 0 and gap_ratio < -GAP_THRESHOLD:
                    signal = "逆張り買い目"
                elif rsi < RSI_TREND_SELL_THRESHOLD and macd_hist < 0 and current_price < high_price * RESISTANCE_THRESHOLD and best_ask_qty > best_bid_qty * BOARD_BALANCE_SELL_THRESHOLD:
                    signal = "順張り売り目"
                elif rsi >= RSI_SELL_THRESHOLD and macd_hist < 0 and gap_ratio > GAP_THRESHOLD:
                    signal = "逆張り売り目"

                # 出力データに追加
                output_data.append({
                    "銘柄コード": code,
                    "銘柄名称": name,
                    "シグナル": signal,
                    "株価": current_price,
                    "ギャップ": gap_ratio,
                    "信用残比": credit_ratio
                })

            except Exception as e:
                print(f"データ処理エラー（{code}）: {e}")

        # 結果の表示
        output_df = pd.DataFrame(output_data)
        signal_order = ["順張り買い目", "逆張り買い目", "順張り売り目", "逆張り売り目"]
        output_df["シグナル"] = pd.Categorical(output_df["シグナル"], categories=signal_order, ordered=True)
        output_df = output_df.sort_values(by=["シグナル", "ギャップ"], ascending=[True, False])

        log_output = ""
        for signal in signal_order:
            log_output += f"■{signal}"
            filtered_df = output_df[output_df["シグナル"] == signal]
            if not filtered_df.empty:
                for _, row in filtered_df.iterrows():
                    log_output += f"{row['銘柄コード']} {row['銘柄名称']} 株価:{row['株価']}円" 
            log_output += ""
        print(log_output)
        return output_df


    except Exception as e:
        print(f"データ読み込みエラー: {e}")

        
        
        

# ▼ 環境変数から認証情報を取得
CLIENT_ID = os.environ.get('DROPBOX_CLIENT_ID')
CLIENT_SECRET = os.environ.get('DROPBOX_CLIENT_SECRET')
REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN')
ACCESS_TOKEN_FILE = '/tmp/access_token.txt'

# ▼ バッファリングの無効化（リアルタイム出力）
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

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
