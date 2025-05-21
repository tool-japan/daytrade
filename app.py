import os
import dropbox # type: ignore
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import requests
import sys
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Bcc

# ▼ テスト実行用に固定日付や時刻を指定できる（空欄ならリアルタイム）
TEST_DATE = "20250521"  # 例: "20250517"
TEST_TIMES = ["0915"]  # 例: ["1000", "1010"]


# ▼ -----RSIの計算-----
RSI_PERIOD = 26  # RSIの計算期間（例：14本）
TREND_LOOKBACK = 5  # トレンド判定で使う短期平均の参照期間

# ▼ RSI（相対力指数）を計算する関数
# - 過去の価格から価格変動の平均を使って買われすぎ／売られすぎを評価
def calculate_rsi(prices, period=RSI_PERIOD):
    deltas = prices.diff()
    gains = deltas.where(deltas > 0, 0)
    losses = -deltas.where(deltas < 0, 0)
    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not rsi.empty else np.nan

# ▼ スコア重み（順張り・逆張り用）
TREND_SCORE = {
    "rsi": 1,  # RSIによる評価スコア
    "macd_hist": 1,  # MACDヒストグラムによる評価スコア
    "trend_alignment": 1,  # 短期・長期トレンド整合性による評価
    "volume_spike": 1 , # 出来高急増による評価
    "board_balance": 1 #板バランスによる評価
}
REVERSAL_SCORE = {
    "rsi": 1,  # RSIによる評価スコア
    "macd_hist": 1,  # MACDヒストグラムによる評価スコア
    "volume_spike": 1 , # 出来高急増による評価
    "board_balance": 1
}

# ▼ 順張りの設定値
TREND_SCORE_THRESHOLD = 5  # 順張りシグナルとして採用するための最小スコア
RSI_TREND_BUY_THRESHOLD = 40  # RSIがこの値を超えたら順張り買いシグナル
RSI_TREND_SELL_THRESHOLD = 60  # RSIがこの値を下回ったら順張り売りシグナル

# ▼ 順張りシグナルのスコアを評価する関数（買い・売りを明確に分岐）
def analyze_trend_signals(row, prices, current_price, volume_spike, rsi, macd_hist, board_balance):
    buy_score = 0
    sell_score = 0
    short_trend = prices[-TREND_LOOKBACK:].mean()
    long_trend = prices.mean()

    # ▼ 順張り買いの条件（RSIが高い、MACD上向き、トレンド上昇）
    if rsi >= RSI_TREND_BUY_THRESHOLD:
        buy_score += TREND_SCORE["rsi"]
    if macd_hist > 0:
        buy_score += TREND_SCORE["macd_hist"]
    if current_price > short_trend and short_trend > long_trend:
        buy_score += TREND_SCORE["trend_alignment"]
    if volume_spike:
        buy_score += TREND_SCORE["volume_spike"]
    if board_balance > BOARD_BALANCE_BUY_THRESHOLD:
        buy_score += TREND_SCORE["board_balance"]   

    # ▼ 順張り売りの条件（RSIが低い、MACD下向き、トレンド下降）
    if rsi <= RSI_TREND_SELL_THRESHOLD:
        sell_score += TREND_SCORE["rsi"]
    if macd_hist < 0:
        sell_score += TREND_SCORE["macd_hist"]
    if current_price < short_trend and short_trend < long_trend:
        sell_score += TREND_SCORE["trend_alignment"]
    if volume_spike:
        sell_score += TREND_SCORE["volume_spike"]
    if board_balance < BOARD_BALANCE_SELL_THRESHOLD:
        sell_score += TREND_SCORE["board_balance"]

    # ▼ シグナル判定（スコア条件を満たす場合に出力）
    if buy_score >= TREND_SCORE_THRESHOLD:
        return "順張り買い目"
    elif sell_score >= TREND_SCORE_THRESHOLD:
        return "順張り売り目"

    return None

# ▼ 逆張りの設定値
REVERSAL_SCORE_THRESHOLD = 4  # 逆張りシグナルとして採用するための最小スコア
RSI_BUY_THRESHOLD = 45  # RSIがこの値以下なら逆張り買いシグナル
RSI_SELL_THRESHOLD = 55  # RSIがこの値以上なら逆張り売りシグナル

# ▼ 逆張りシグナルのスコアを評価する関数（買い／売りを明示）
def analyze_reversal_signals(volume_spike, rsi, macd_hist, board_balance):
    buy_score = 0
    sell_score = 0

    # ▼ 逆張り買いの評価条件
    if rsi <= RSI_BUY_THRESHOLD:
        buy_score += REVERSAL_SCORE["rsi"]
    if macd_hist > 0:
        buy_score += REVERSAL_SCORE["macd_hist"]
    if volume_spike:
        buy_score += REVERSAL_SCORE["volume_spike"]
    if board_balance > BOARD_BALANCE_BUY_THRESHOLD:
        buy_score += REVERSAL_SCORE["board_balance"]    

    # ▼ 逆張り売りの評価条件
    if rsi >= RSI_SELL_THRESHOLD:
        sell_score += REVERSAL_SCORE["rsi"]
    if macd_hist < 0:
        sell_score += REVERSAL_SCORE["macd_hist"]
    if volume_spike:
        sell_score += REVERSAL_SCORE["volume_spike"]
    if board_balance < BOARD_BALANCE_SELL_THRESHOLD:
        sell_score += REVERSAL_SCORE["board_balance"]

    # ▼ スコア判定と出力
    if buy_score >= REVERSAL_SCORE_THRESHOLD:
        return "逆張り買い目"
    elif sell_score >= REVERSAL_SCORE_THRESHOLD:
        return "逆張り売り目"
    
    return None


# ▼ -----MACDの計算-----
MACD_SHORT = 12  # MACD短期EMAの期間
MACD_LONG = 26  # MACD長期EMAの期間
MACD_SIGNAL = 9  # MACDシグナル（MACDのEMA）の期間

# ▼ MACDを計算する関数（短期EMA、長期EMA、シグナルを使ってMACDヒストグラムを返す）
def calculate_macd(prices):
    ema_short = prices.ewm(span=MACD_SHORT, adjust=False).mean()
    ema_long = prices.ewm(span=MACD_LONG, adjust=False).mean()
    macd = ema_short - ema_long
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_hist = macd.iloc[-1] - macd_signal.iloc[-1]
    return macd_hist


# ▼ -----出来高急増の計算-----
VOLUME_SPIKE_MULTIPLIER = 1.0  # IQRスパイク判定の乗数（大きいほど厳しく）

# ▼ 出来高急増を検出する関数（IQRを使ってスパイクを判定）
def calculate_volume_spike(df):
    df["出来高増加率"] = (df["D01"] - df["D26"]) / df["D26"]
    Q1 = df["出来高増加率"].quantile(0.25)
    Q3 = df["出来高増加率"].quantile(0.75)
    IQR = Q3 - Q1
    threshold = Q3 + VOLUME_SPIKE_MULTIPLIER * IQR
    df["急増フラグ"] = df["出来高増加率"] > threshold
    return df

# ▼ -----板バランスの計算-----
BOARD_BALANCE_BUY_THRESHOLD = 1.0  # 板バランス（買い優勢と判定する閾値）
BOARD_BALANCE_SELL_THRESHOLD = 1.0  # 板バランス（売り優勢と判定する閾値）

# ▼ 板の買い注文／売り注文の比率を計算（板バランス）
def calculate_board_balance(row):
    buy_quantities = [row[f"最良買気配数量{i}"] for i in range(1, 6)]
    sell_quantities = [row[f"最良売気配数量{i}"] for i in range(1, 6)]
    total_buy = sum(buy_quantities)
    total_sell = sum(sell_quantities)
    return total_buy / total_sell if total_sell > 0 else float('inf')


# ▼ -----ブレイクアウトの計算-----
BREAKOUT_THRESHOLD = 0.005  # ブレイクと判断する前日終値からの変動率（例：0.5%）
BREAKOUT_LOOKBACK = 26  # ブレイクアウトを評価する期間（例：30秒足26本）
BREAKOUT_CONFIRMATION_BARS = 3  # ブレイク後に価格が維持されているか確認するバー数
VOLUME_CONFIRMATION_BARS = 3  # 出来高が連続して増加しているかの確認バー数
VOLUME_SPIKE_THRESHOLD = 0.05  # 出来高変化率がこの値以上でスパイクと判定

# ▼ ブレイクアウト（前日終値の上下突破）を検出する関数
def detect_breakout(df):
    breakout_signals = []
    for _, row in df.iterrows():
        code = row["銘柄コード"]
        name = row["銘柄名称"]
        prices = pd.Series([row[f"G{i:02d}"] for i in range(1, BREAKOUT_LOOKBACK + 1)], dtype=float)
        volumes = pd.Series([row[f"D{i:02d}"] for i in range(1, BREAKOUT_LOOKBACK + 1)], dtype=float)
        prev_close = float(row["前日終値"])
        high_price = prices.max()
        low_price = prices.min()
        current_price = float(row["現在値"])
        board_balance = calculate_board_balance(row)

        if current_price > prev_close * (1 + BREAKOUT_THRESHOLD) and low_price < prev_close:
            if all(prices[-BREAKOUT_CONFIRMATION_BARS:] > prev_close * (1 + BREAKOUT_THRESHOLD)):
                if (volumes[-VOLUME_CONFIRMATION_BARS:].pct_change().dropna() > VOLUME_SPIKE_THRESHOLD).all() and board_balance > BOARD_BALANCE_BUY_THRESHOLD:
                    breakout_signals.append({"銘柄コード": code, "銘柄名称": name, "シグナル": "ロングブレイクアウト", "株価": current_price})

        if current_price < prev_close * (1 - BREAKOUT_THRESHOLD) and high_price > prev_close:
            if all(prices[-BREAKOUT_CONFIRMATION_BARS:] < prev_close * (1 - BREAKOUT_THRESHOLD)):
                if (volumes[-VOLUME_CONFIRMATION_BARS:].pct_change().dropna() > VOLUME_SPIKE_THRESHOLD).all() and board_balance < BOARD_BALANCE_SELL_THRESHOLD:
                    breakout_signals.append({"銘柄コード": code, "銘柄名称": name, "シグナル": "ショートブレイクアウト", "株価": current_price})
    return breakout_signals


# ▼ CSVファイルを分析し、テクニカルシグナルを判定してメール送信
def analyze_and_display_filtered_signals(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace("　", "").str.replace(" ", "")

        # ▼ 出来高スパイク検出とブレイクアウトシグナル取得
        df = calculate_volume_spike(df)
        breakout_signals = detect_breakout(df)

        # ▼ 価格列の抽出（例：G01〜G26）
        price_columns = df.columns[31:57]

        # ▼ 全銘柄を対象に分析（価格フィルタは削除）
        output_data = breakout_signals
        for _, row in df.iterrows():
            try:
                code = row["銘柄コード"]
                name = row["銘柄名称"]
                prices = pd.Series(row[price_columns].values.astype(float))
                current_price = float(row["現在値"])
                volume_spike = row["急増フラグ"]

                # ▼ RSI & MACDヒストグラムを計算
                rsi = calculate_rsi(prices, period=RSI_PERIOD)
                macd_hist = calculate_macd(prices)

                # ▼ シグナルを判定（順張り→逆張りの順で評価）
                board_balance = calculate_board_balance(row)  # ← 必須

                signal = analyze_trend_signals(row, prices, current_price, volume_spike, rsi, macd_hist, board_balance)
                if not signal:
                    signal = analyze_reversal_signals(volume_spike, rsi, macd_hist, board_balance)

                if not signal:
                    continue

                # ▼ 出力データに追加
                output_data.append({
                    "銘柄コード": code,
                    "銘柄名称": name,
                    "シグナル": signal,
                    "株価": current_price
                })

            except Exception as e:
                print(f"データ処理エラー（{code}）: {e}")

        # ▼ 結果をメール送信
        send_output_dataframe_via_email(output_data)

    except Exception as e:
        print(f"データ読み込みエラー: {e}")


# ▼ 出力データをテキスト形式に整形（メール本文用）
def format_output_text(df):
    grouped = df.groupby("シグナル", observed=False)
    lines = []
    for signal, group in grouped:
        lines.append(f"■ {signal}")
        for _, row in group.iterrows():
            lines.append(f"{row['銘柄コード']} {row['銘柄名称']} 株価: {int(row['株価'])}円")
        lines.append("")
    # ▼ 投資判断の注意文を末尾に追加
    lines.append("【ご注意】")
    lines.append("本分析は、特定の銘柄の売買を推奨するものではありません。")
    lines.append("出力内容はあくまでテクニカル分析に基づく参考情報であり、最終的な投資判断はご自身の責任で慎重に行ってください。")
    lines.append("市場動向は常に変動するため、本分析の結果に過信せず、複数の情報を組み合わせた冷静な判断を心がけてください。")
    return "\n".join(lines)

# ▼ SendGridを使って分析結果をメール送信（BCCモード）
def send_output_dataframe_via_email(output_data):
    try:
        output_df = pd.DataFrame(output_data)
        signal_order = ["順張り買い目", "逆張り買い目", "順張り売り目", "逆張り売り目", "ロングブレイクアウト", "ショートブレイクアウト"]
        output_df["シグナル"] = pd.Categorical(output_df["シグナル"], categories=signal_order, ordered=True)
        output_df = output_df.sort_values(by=["シグナル"], ascending=[True])

        message_text = format_output_text(output_df)

        sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
        sender_email = os.environ.get("SENDER_EMAIL")
        email_list_path = "email_list.txt"
        email_subject = "【株式テクニカル分析検出通知】"

        with open(email_list_path, "r", encoding="utf-8") as f:
            recipient_emails = [email.strip() for email in f if email.strip()]

        message = Mail(
            from_email=Email(sender_email),
            to_emails=To(sender_email),
            subject=email_subject,
            plain_text_content=message_text
        )
        message.bcc = [Bcc(email) for email in recipient_emails]
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"✅ メール送信完了（BCCモード）: ステータスコード = {response.status_code}")
    except Exception as e:
        print(f"🚫 メール送信エラー: {e}")


        
# ▼ ボラティリティ（価格変動幅）を計算する関数（将来拡張用）
VOLATILITY_LOOKBACK = 26  # ボラティリティ評価の期間（将来用途）
def calculate_volatility(prices):
    return prices[-VOLATILITY_LOOKBACK:].pct_change().std()


# ▼ タイムゾーンを日本時間（JST）に設定
JST = timezone(timedelta(hours=9))

# ▼ 現在の日本時間を取得
def get_japan_time():
    return datetime.now(JST)

# ▼ アクセストークンを定期的にリフレッシュするための設定（3時間）
REFRESH_INTERVAL = timedelta(hours=3)

# ▼ バッファリングの無効化（ログを即時に出力）
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ▼ グローバル変数の初期化（Dropbox接続状態・最終更新時刻）
dbx = None
last_refresh_time = None

# ▼ Dropboxのアクセストークンをリフレッシュする関数
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

# ▼ Dropboxクライアントの初期化＆リフレッシュ管理
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

# ▼ DropboxからCSVファイルをダウンロードする関数
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
