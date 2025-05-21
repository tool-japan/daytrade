import os
import dropbox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import requests
import sys
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Bcc

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
RSI_PERIOD = 14
RSI_BUY_THRESHOLD = 45
RSI_SELL_THRESHOLD = 55
RSI_TREND_BUY_THRESHOLD = 40
RSI_TREND_SELL_THRESHOLD = 60
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
BOARD_BALANCE_BUY_THRESHOLD = 1.0  # 最良買気配と最良売気配の比率（買い優勢）
BOARD_BALANCE_SELL_THRESHOLD = 1.0  # 最良買気配と最良売気配の比率（売り優勢）
TREND_LOOKBACK = 5
PRICE_MAX_THRESHOLD = 20000
PRICE_MIN_THRESHOLD = 500
SUPPORT_THRESHOLD = 1.05
RESISTANCE_THRESHOLD = 0.95
VOLATILITY_LOOKBACK = 26

# ▼ 出来高関連の設定
VOLUME_SPIKE_MULTIPLIER = 1.0  # IQRスパイクの倍率
VOLUME_CONFIRMATION_BARS = 3  # 出来高増加の確認に使用するバー数
VOLUME_SPIKE_THRESHOLD = 0.05  # 最低出来高増加率（5%）

# ▼ ブレイクアウトの設定
BREAKOUT_THRESHOLD = 0.005  # 前日終値からの突破率（1%）
BREAKOUT_LOOKBACK = 26  # ブレイクアウトの確認に使用する期間（15秒足26本）
BREAKOUT_CONFIRMATION_BARS = 3  # 突破後に価格を維持する最低バー数


# ▼ 整形テキストを作る関数
def format_output_text(df):
    grouped = df.groupby("シグナル", observed=False)
    lines = []
    for signal, group in grouped:
        lines.append(f"■ {signal}")
        for _, row in group.iterrows():
            lines.append(f"{row['銘柄コード']} {row['銘柄名称']} 株価: {int(row['株価'])}円")
        lines.append("")  # 空行で区切り

    # ▼ 末尾に注意書きを追加
    lines.append("【ご注意】")
    lines.append("本分析は、特定の銘柄の売買を推奨するものではありません。")
    lines.append("出力内容はあくまでテクニカル分析に基づく参考情報であり、最終的な投資判断はご自身の責任で慎重に行ってください。")
    lines.append("市場動向は常に変動するため、本分析の結果に過信せず、複数の情報を組み合わせた冷静な判断を心がけてください。")

    return "\n".join(lines)


# ▼ メール送信関数（BCC対応）
def send_output_dataframe_via_email(output_data):
    try:
        # DataFrameを作成・整形
        output_df = pd.DataFrame(output_data)
        signal_order = ["順張り買い目", "逆張り買い目", "順張り売り目", "逆張り売り目", "ロングブレイクアウト", "ショートブレイクアウト"]
        output_df["シグナル"] = pd.Categorical(output_df["シグナル"], categories=signal_order, ordered=True)
        output_df = output_df.sort_values(by=["シグナル"], ascending=[True])

        # 🔧 ← ここが重要！
        message_text = format_output_text(output_df)

        # 環境変数を取得
        sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
        sender_email = os.environ.get("SENDER_EMAIL")
        email_list_path = "email_list.txt"
        email_subject = "【株式テクニカル分析検出通知】"

        # メール送信先を読み込み（BCC）
        with open(email_list_path, "r", encoding="utf-8") as f:
            recipient_emails = [email.strip() for email in f if email.strip()]

        # メール作成（Toは自分、BCCに全体）
        message = Mail(
            from_email=Email(sender_email),
            to_emails=To(sender_email),
            subject=email_subject,
            plain_text_content=message_text
        )
        message.bcc = [Bcc(email) for email in recipient_emails]

        # メール送信
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"✅ メール送信完了（BCCモード）: ステータスコード = {response.status_code}")

    except Exception as e:
        print(f"🚫 メール送信エラー: {e}")

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


# ▼ 板バランス計算関数
def calculate_board_balance(row):
    buy_quantities = [row[f"最良買気配数量{i}"] for i in range(1, 6)]
    sell_quantities = [row[f"最良売気配数量{i}"] for i in range(1, 6)]
    total_buy = sum(buy_quantities)
    total_sell = sum(sell_quantities)
    return total_buy / total_sell if total_sell > 0 else float('inf')

# ▼ 出来高スパイク計算関数
def calculate_volume_spike(df):
    volume_cols = [f"D{i:02d}" for i in range(1, 27)]
    df["出来高増加率"] = (df["D01"] - df["D26"]) / df["D26"]
    
    # IQRの計算
    Q1 = df["出来高増加率"].quantile(0.25)
    Q3 = df["出来高増加率"].quantile(0.75)
    IQR = Q3 - Q1
    threshold = Q3 + VOLUME_SPIKE_MULTIPLIER * IQR
    
    df["急増フラグ"] = df["出来高増加率"] > threshold

    return df

# ▼ ブレイクアウト計算関数（15秒足26本）
def detect_breakout(df):
    breakout_signals = []
    for _, row in df.iterrows():
        code = row["銘柄コード"]
        name = row["銘柄名称"]
        prices = pd.Series([row[f"G{i:02d}"] for i in range(1, BREAKOUT_LOOKBACK + 1)], dtype=float)
        volumes = pd.Series([row[f"D{i:02d}"] for i in range(1, BREAKOUT_LOOKBACK + 1)], dtype=float)

        # 前日終値を基準に変更
        prev_close = float(row["前日終値"])
        high_price = prices.max()
        low_price = prices.min()
        current_price = float(row["現在値"])

        # 板バランスの計算
        board_balance = calculate_board_balance(row)

        # ロングブレイクアウト
        if current_price > prev_close * (1 + BREAKOUT_THRESHOLD) and low_price < prev_close:
            # 突破後の価格維持確認
            if all(prices[-BREAKOUT_CONFIRMATION_BARS:] > prev_close * (1 + BREAKOUT_THRESHOLD)):
                # 出来高確認
                recent_volumes = volumes[-VOLUME_CONFIRMATION_BARS:]
                if (recent_volumes.pct_change().dropna() > VOLUME_SPIKE_THRESHOLD).all() and board_balance > BOARD_BALANCE_BUY_THRESHOLD:
                    breakout_signals.append({
                        "銘柄コード": code,
                        "銘柄名称": name,
                        "シグナル": "ロングブレイクアウト",
                        "株価": current_price
                    })

        # ショートブレイクアウト
        if current_price < prev_close * (1 - BREAKOUT_THRESHOLD) and high_price > prev_close:
            # 突破後の価格維持確認
            if all(prices[-BREAKOUT_CONFIRMATION_BARS:] < prev_close * (1 - BREAKOUT_THRESHOLD)):
                # 出来高確認
                recent_volumes = volumes[-VOLUME_CONFIRMATION_BARS:]
                if (recent_volumes.pct_change().dropna() > VOLUME_SPIKE_THRESHOLD).all() and board_balance < BOARD_BALANCE_SELL_THRESHOLD:
                    breakout_signals.append({
                        "銘柄コード": code,
                        "銘柄名称": name,
                        "シグナル": "ショートブレイクアウト",
                        "株価": current_price
                    })

    return breakout_signals

# ▼ シグナル判定関数（score削除、ログ出力分離）
def analyze_and_display_filtered_signals(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace("　", "").str.replace(" ", "")

        # 出来高スパイクを計算
        df = calculate_volume_spike(df)

        # ブレイクアウトを計算
        breakout_signals = detect_breakout(df)

        price_columns = df.columns[31:57]

        df_filtered = df[(df[price_columns].astype(float).max(axis=1) <= PRICE_MAX_THRESHOLD) &
                         (df[price_columns].astype(float).min(axis=1) >= PRICE_MIN_THRESHOLD)]

        output_data = breakout_signals

        for _, row in df_filtered.iterrows():
            try:
                code = row["銘柄コード"]
                name = row["銘柄名称"]
                prices = pd.Series(row[price_columns].values.astype(float))

                current_price = float(row["現在値"])
                high_price = float(row["高値"])
                low_price = float(row["安値"])
                volume_spike = row["急増フラグ"]

                rsi = calculate_rsi(prices, period=RSI_PERIOD)

                ema_short = prices.ewm(span=MACD_SHORT, adjust=False).mean()
                ema_long = prices.ewm(span=MACD_LONG, adjust=False).mean()
                macd = ema_short - ema_long
                macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
                macd_hist = macd.iloc[-1] - macd_signal.iloc[-1]

                short_trend = prices[-TREND_LOOKBACK:].mean()
                long_trend = prices.mean()

                signal = "中立"
                if rsi > RSI_TREND_BUY_THRESHOLD and macd_hist > 0 and current_price > short_trend and short_trend > long_trend and volume_spike:
                    signal = "順張り買い目"
                elif rsi <= RSI_BUY_THRESHOLD and macd_hist > 0 and volume_spike:
                    signal = "逆張り買い目"
                elif rsi < RSI_TREND_SELL_THRESHOLD and macd_hist < 0 and current_price < short_trend and short_trend < long_trend and volume_spike:
                    signal = "順張り売り目"
                elif rsi >= RSI_SELL_THRESHOLD and macd_hist < 0 and volume_spike:
                    signal = "逆張り売り目"

                if signal == "中立":
                    continue

                output_data.append({
                    "銘柄コード": code,
                    "銘柄名称": name,
                    "シグナル": signal,
                    "株価": current_price
                })

            except Exception as e:
                print(f"データ処理エラー（{code}）: {e}")

        # 分離したログ出力関数で表示
        send_output_dataframe_via_email(output_data)

    except Exception as e:
        print(f"データ読み込みエラー: {e}")



# ▼ バッファリングの無効化（リアルタイム出力）
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)      

# ▼ リフレッシュ間隔（3時間）
REFRESH_INTERVAL = timedelta(hours=0.1)

# ▼ グローバル状態を保持
dbx = None
last_refresh_time = None

# ▼ アクセストークンをリフレッシュする関数
def refresh_access_token():
    client_id = os.environ.get('DROPBOX_CLIENT_ID')
    client_secret = os.environ.get('DROPBOX_CLIENT_SECRET')
    refresh_token = os.environ.get('DROPBOX_REFRESH_TOKEN')

    if not all([client_id, client_secret, refresh_token]):
        print("🚫 認証情報が不足しています。環境変数を確認してください。")
        exit(1)

    url = 'https://api.dropbox.com/oauth2/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token
    }

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        access_token = response.json().get('access_token')
        print('✅ アクセストークンをリフレッシュしました。')
        return access_token
    except requests.exceptions.RequestException as e:
        print(f'🚫 アクセストークンのリフレッシュに失敗しました: {e}')
        exit(1)

# ▼ Dropboxクライアントの取得（3時間に1回リフレッシュ）
def get_dropbox_client():
    global dbx, last_refresh_time

    now = datetime.utcnow()
    time_since_refresh = (now - last_refresh_time) if last_refresh_time else None

    if dbx is None or last_refresh_time is None or time_since_refresh > REFRESH_INTERVAL:
        print(f"🔁 Dropboxクライアントを初期化します（前回更新から: {time_since_refresh}）")
        access_token = refresh_access_token()
        try:
            dbx = dropbox.Dropbox(access_token)
            dbx.users_get_current_account()
            last_refresh_time = now
            print('✅ Dropboxに接続しました。')
        except Exception as e:
            print(f'🚫 Dropbox接続に失敗しました: {e}')
            exit(1)

    return dbx

# ▼ ファイルダウンロード関数
def download_csv_from_dropbox(file_name):
    try:
        dbx = get_dropbox_client()
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
