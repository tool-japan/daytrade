import os
import re
import dropbox # type: ignore
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import time
import requests
import sys
import jpholiday  # type: ignore # ← 追加：日本の祝日判定
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Bcc

# ▼ テスト実行用に固定日付や時刻を指定できる（空欄ならリアルタイム）
TEST_DATE = ""  # 例: "20250517"
TEST_TIME = ""  # 例: "1000"（空欄ならリアルタイム）

# ▼ バッファリングの無効化（ログを即時に出力）
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ▼ タイムゾーンを日本時間（JST）に設定
JST = timezone(timedelta(hours=9))

# ▼ 現在の日本時間を取得
def get_japan_time():
    return datetime.now(JST)

# ▼ アクセストークンを定期的にリフレッシュするための設定（3時間）
REFRESH_INTERVAL = timedelta(hours=3)

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


# ▼ 🔹修正済：CSVファイル一覧（hhmm順）を取得し、最新90件だけに絞る
def list_today_csv_files(target_date=None, limit=90):
    dbx = get_dropbox_client()
    today = target_date if target_date else get_japan_time().strftime("%Y%m%d")
    files = []

    try:
        all_entries = []
        res = dbx.files_list_folder("/デイトレファイル")
        all_entries.extend(res.entries)
        while res.has_more:
            res = dbx.files_list_folder_continue(res.cursor)
            all_entries.extend(res.entries)

        for entry in all_entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                fname = entry.name
                match = re.match(rf"kabuteku{today}_(\d{{4}})\.csv", fname)
                if match:
                    hhmm = match.group(1)
                    files.append((hhmm, fname))

    except Exception as e:
        print(f"🚫 Dropboxファイル一覧取得エラー: {e}")
        return []

    # hhmm順に並べて、最新 limit 件だけを取得
    files_sorted = sorted(files, key=lambda x: x[0])
    return files_sorted[-limit:]  # 最新limit件



def build_intraday_dataframe(target_date=None):
    dbx = get_dropbox_client()
    files = list_today_csv_files(target_date=target_date, limit=90)  # ← ここに制限を明示
    combined_df = []

    for hhmm, fname in files:
        dropbox_path = f"/デイトレファイル/{fname}"
        try:
            metadata, res = dbx.files_download(dropbox_path)
            df = pd.read_csv(res.raw)
            df["ファイル時刻"] = hhmm
            combined_df.append(df)
        except Exception as e:
            print(f"⚠️ {fname} の読み込みに失敗しました: {e}")
            continue

    if not combined_df:
        print("📭 有効なCSVファイルが見つかりませんでした。")
        return pd.DataFrame()

    df_all = pd.concat(combined_df, ignore_index=True)
    df_all["ファイル時刻"] = pd.to_datetime(df_all["ファイル時刻"], format="%H%M").dt.time
    df_all = df_all.sort_values(by=["銘柄コード", "ファイル時刻"]).reset_index(drop=True)

    return df_all



# ▼ ----- 上昇／【売り目】下降トレンド判定に必要な設定値（コメント付き） -----

UPTREND_LOOKBACK = 60  
# ✅ 過去何本を使ってトレンドを評価するか（最低必要本数）。  
# 適正値：60～300　本数が多いほど精度↑だが反応は鈍化する。

UPTREND_HIGH_LOW_LENGTH = 5  
# ✅ 高値・安値の連続切り上げ/下げの確認に使う本数。  
# 適正値：3～10　本数が多いほど強い傾向を示すが、検出数は減る（精度↑）

MA_SHORT_WINDOW = 5  
# ✅ 短期移動平均線（MA_5）。短期の価格変動を捉える。  
# 適正値：3～10　小さいほど早く反応（精度↓）、大きいと安定（精度↑）

MA_MID_WINDOW = 25  
# ✅ 中期移動平均線。全体の流れを把握するのに使う。  
# 適正値：20～50　短すぎるとノイズ↑、大きすぎると遅れる。

MA_LONG_WINDOW = 60  
# ✅ 長期移動平均線。トレンド全体の方向性判断。  
# 適正値：50～100　大きいほど長期トレンドに忠実（精度↑）

VOLUME_RECENT_WINDOW = 5  
# ✅ 出来高の直近平均。直近5本程度で勢いを見る。  
# 適正値：3～10　小さいと反応早く検出多め（精度↓）

VOLUME_PAST_WINDOW = 55  
# ✅ 出来高の過去平均（比較用）。  
# 適正値：30～100　大きいほど長期傾向を反映（精度↑）

STD_WINDOW = 20  
# ✅ 標準偏差の計算に使う期間（ボラティリティ判断）。  
# 適正値：10～30　大きいと滑らかになるが、急変に弱い。

VOLATILITY_THRESHOLD = 0.5  
# ✅ 標準偏差がこれ以下なら「安定」とみなす。  
# 適正値：0.3～1.0　小さいほど静かな相場しか通さない（精度↑）

RSI_PERIOD = 26  
# ✅ RSIの計算期間。価格の上昇・下落の強さの平均から算出。  
# 適正値：14～30　大きいと滑らかだが反応は遅め（精度↑）

RSI_UP_THRESHOLD = 40  
# ✅ RSIがこれを上回ると「買い勢力あり」と判断（順張り用）  
# 適正値：30～50　低いほど感度↑、高いほど強気な相場のみ検出（精度↑）

RSI_DOWN_THRESHOLD = 60  
# ✅ RSIがこれを下回ると「売り勢力あり」と判断（順張り用）  
# 適正値：50～70　高いほど慎重に売り判断（精度↑）

MACD_SHORT = 12  
# ✅ MACDの短期EMAの期間。短いほど早く反応（精度↓）  
# 適正値：10～15

MACD_LONG = 26  
# ✅ MACDの長期EMAの期間。MACD全体の傾向を決める。  
# 適正値：20～30

MACD_SIGNAL = 9  
# ✅ MACDのシグナルライン（MACDのEMA）期間。  
# 適正値：5～10　小さいと反応が早く、感度↑だがノイズも↑

PULLBACK_LOOKBACK = 10  
# ✅ 押し目・戻りパターン検出に使う本数。  
# 適正値：5～15　本数が多いと確実な反発だがタイミング遅れる。

CROSS_LOOKBACK = 2  
# ✅ 【買い目】ゴールデンクロス／【売り目】デッドクロスで過去何本見るか。  
# 通常は2本で十分。1本だと誤判定↑（精度↓）

BOX_RANGE_WINDOW = 30  
# ✅ ボックスレンジの分析対象期間（範囲）。  
# 適正値：20～50　大きいと安定して認識される（精度↑）

BOX_TOLERANCE = 0.01  
# ✅ 現在値と平均の誤差がこの値以下ならボックス内と判断。  
# 適正値：0.005～0.02　小さいほど厳しい（精度↑）

BOX_EDGE_THRESHOLD = 0.8  
# ✅ ボックスレンジの上下8%で買い／売りを判断。  
# 適正値：0.7～0.9　高すぎるとチャンス減、低すぎると誤判定↑

BREAKOUT_LOOKBACK = 20  
# ✅ 過去の高値・安値ブレイクを確認する期間。  
# 適正値：15～30　大きいほど重要ラインを検出（精度↑）

BREAKOUT_VOLUME_RATIO = 1.5  
# ✅ ブレイク時に出来高が平均の何倍なら「有効」とみなすか。  
# 適正値：1.2～2.0　大きいほど強い確信が必要（精度↑）

DOUBLE_PATTERN_LOOKBACK = 40  
# ✅ 検出に使うローソクの本数（ダブルパターン全体を見る範囲）
# 推奨値：30〜60　本数が多いほど安定パターンに対応

DOUBLE_PATTERN_MIN_PEAKS = 2  
# ✅ 山（または谷）の数。最低何個あればパターンと見なすか
# 通常は2で良いが、精度を高めたい場合は3以上もあり得る

DOUBLE_PATTERN_TOLERANCE = 0.005  
# ✅ 高値A≒高値B（または安値A≒安値B）と見なす誤差率
# 推奨：0.003〜0.01　小さいと精度↑だが検出減る

DOUBLE_PATTERN_VOLUME_SPIKE_RATIO = 1.5  
# ✅ ピーク時の出来高が平均の何倍以上ならスパイクと判定

DOUBLE_PATTERN_VOLATILITY_JUMP = True  
# ✅ ボラティリティ急増も検出条件に含めるかどうか

DOUBLE_PATTERN_VOLATILITY_RATIO = 1.3  
# ✅ ボラ急増とみなす倍率（現在のstd > 平均std×この値）
# 推奨：1.2〜1.5


VOLATILITY_JUMP_RATIO = 1.3
# ✅ 現在の標準偏差が平均の1.3倍を超えていれば「ボラティリティ急増」と判断
# 推奨値：1.2～1.5（小さすぎると過検出、大きすぎると検出減）

# ボックスレンジに出来高・ボラ条件を追加
BOX_USE_VOLUME_SPIKE = True
BOX_USE_VOLATILITY_FILTER = True
BOX_VOLATILITY_RATIO = 1.2  # 現在のstdが過去の1.2倍以下ならOK


# ▼ ブレイクアウト拡張条件（ボラ急増を有効化）
BREAKOUT_USE_VOLATILITY_SPIKE = True   # ブレイク時にボラ急増していることを確認
BREAKOUT_VOLATILITY_RATIO = 1.2        # 現在のボラが過去の1.2倍以上で「急増」とみなす

CROSS_USE_VOLATILITY_FILTER = True
CROSS_VOLATILITY_THRESHOLD = 0.5




# ▼ MACDヒストグラム計算関数
def calculate_macd_hist(prices: pd.Series) -> pd.Series:
    ema_short = prices.ewm(span=MACD_SHORT, adjust=False).mean()
    ema_long = prices.ewm(span=MACD_LONG, adjust=False).mean()
    macd = ema_short - ema_long
    macd_signal = macd.ewm(span=MACD_SIGNAL, adjust=False).mean()
    return macd - macd_signal

# ▼ RSI 計算関数
def calculate_rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ▼ トレンド判定共通関数
def detect_trend(df_group, trend_type="up"):
    df = df_group.tail(300).copy()
    if len(df) < UPTREND_LOOKBACK:
        return None

    df["MA_5"] = df["現在値"].rolling(window=MA_SHORT_WINDOW).mean()
    df["MA_25"] = df["現在値"].rolling(window=MA_MID_WINDOW).mean()
    df["MA_60"] = df["現在値"].rolling(window=MA_LONG_WINDOW).mean()
    df["出来高平均_直近"] = df["出来高"].rolling(window=VOLUME_RECENT_WINDOW).mean()
    df["出来高平均_過去"] = df["出来高"].shift(VOLUME_RECENT_WINDOW).rolling(window=VOLUME_PAST_WINDOW).mean()
    df["標準偏差"] = df["現在値"].rolling(window=STD_WINDOW).std()
    df["MACDヒストグラム"] = calculate_macd_hist(df["現在値"])
    df["RSI"] = calculate_rsi(df["現在値"], period=RSI_PERIOD)

    latest = df.iloc[-1]
    highs = df["高値"].tail(UPTREND_HIGH_LOW_LENGTH).values
    lows = df["安値"].tail(UPTREND_HIGH_LOW_LENGTH).values

    if trend_type == "up":
        trend_ok = all(x < y for x, y in zip(highs, highs[1:])) and all(x < y for x, y in zip(lows, lows[1:]))
        ma_ok = latest["MA_5"] > latest["MA_25"] > latest["MA_60"]
        rsi_ok = latest["RSI"] > RSI_UP_THRESHOLD
        macd_ok = latest["MACDヒストグラム"] > 0
        trigger_cross = df["MA_5"].iloc[-2] < df["MA_25"].iloc[-2] and df["MA_5"].iloc[-1] > df["MA_25"].iloc[-1]
        recent_prices = df["現在値"].tail(PULLBACK_LOOKBACK)
        trigger_pullback = recent_prices.min() < recent_prices.iloc[-1] and recent_prices.iloc[-2] < recent_prices.iloc[-1]
    else:
        trend_ok = all(x > y for x, y in zip(highs, highs[1:])) and all(x > y for x, y in zip(lows, lows[1:]))
        ma_ok = latest["MA_5"] < latest["MA_25"] < latest["MA_60"]
        rsi_ok = latest["RSI"] < RSI_DOWN_THRESHOLD
        macd_ok = latest["MACDヒストグラム"] < 0
        trigger_cross = df["MA_5"].iloc[-2] > df["MA_25"].iloc[-2] and df["MA_5"].iloc[-1] < df["MA_25"].iloc[-1]
        recent_prices = df["現在値"].tail(PULLBACK_LOOKBACK)
        trigger_pullback = recent_prices.max() > recent_prices.iloc[-1] and recent_prices.iloc[-2] > recent_prices.iloc[-1]

    volume_ok = latest["出来高平均_直近"] > latest["出来高平均_過去"]
    std_ok = latest["標準偏差"] < VOLATILITY_THRESHOLD

    if trend_ok and ma_ok and rsi_ok and macd_ok and volume_ok and std_ok and (trigger_cross or trigger_pullback):
        return {
            "シグナル": "【買い目】上昇トレンド" if trend_type == "up" else "【売り目】下降トレンド",
            "現在値": latest["現在値"],
            "MA_5": round(latest["MA_5"], 2),
            "MA_25": round(latest["MA_25"], 2),
            "MA_60": round(latest["MA_60"], 2),
            "MACDヒストグラム": round(latest["MACDヒストグラム"], 4),
            "RSI": round(latest["RSI"], 1),
            "標準偏差": round(latest["標準偏差"], 4),
            "出来高平均_直近": round(latest["出来高平均_直近"], 2),
            "出来高平均_過去": round(latest["出来高平均_過去"], 2),
            "出来高勢い": "増加" if volume_ok else "弱含み",
            "トリガー": "クロス" if trigger_cross else "戻り"
        }
    return None

# ▼ ラッパー関数（トレンド）
def detect_uptrend(df_group):
    return detect_trend(df_group, trend_type="up")

def detect_downtrend(df_group):
    return detect_trend(df_group, trend_type="down")

# ▼ 【買い目】ゴールデンクロス検出（独立シグナル + ボラフィルター対応）
def detect_golden_cross(df_group):
    df = df_group.tail(30).copy()
    if len(df) < CROSS_LOOKBACK:
        return None

    df["MA_5"] = df["現在値"].rolling(window=MA_SHORT_WINDOW).mean()
    df["MA_25"] = df["現在値"].rolling(window=MA_MID_WINDOW).mean()
    df["標準偏差"] = df["現在値"].rolling(window=STD_WINDOW).std()

    volatility_ok = (
        df["標準偏差"].iloc[-1] < CROSS_VOLATILITY_THRESHOLD
        if CROSS_USE_VOLATILITY_FILTER else True
    )

    if (
        df["MA_5"].iloc[-2] < df["MA_25"].iloc[-2] and
        df["MA_5"].iloc[-1] > df["MA_25"].iloc[-1] and
        volatility_ok
    ):
        return {
            "シグナル": "【買い目】ゴールデンクロス",
            "現在値": df["現在値"].iloc[-1],
            "MA_5": round(df["MA_5"].iloc[-1], 2),
            "MA_25": round(df["MA_25"].iloc[-1], 2)
        }
    return None


# ▼ 【売り目】デッドクロス検出（独立シグナル + ボラフィルター対応）
def detect_dead_cross(df_group):
    df = df_group.tail(30).copy()
    if len(df) < CROSS_LOOKBACK:
        return None

    df["MA_5"] = df["現在値"].rolling(window=MA_SHORT_WINDOW).mean()
    df["MA_25"] = df["現在値"].rolling(window=MA_MID_WINDOW).mean()
    df["標準偏差"] = df["現在値"].rolling(window=STD_WINDOW).std()

    volatility_ok = (
        df["標準偏差"].iloc[-1] < CROSS_VOLATILITY_THRESHOLD
        if CROSS_USE_VOLATILITY_FILTER else True
    )

    if (
        df["MA_5"].iloc[-2] > df["MA_25"].iloc[-2] and
        df["MA_5"].iloc[-1] < df["MA_25"].iloc[-1] and
        volatility_ok
    ):
        return {
            "シグナル": "【売り目】デッドクロス",
            "現在値": df["現在値"].iloc[-1],
            "MA_5": round(df["MA_5"].iloc[-1], 2),
            "MA_25": round(df["MA_25"].iloc[-1], 2)
        }
    return None


def detect_box_range(df_group):
    required_rows = BOX_RANGE_WINDOW + VOLUME_PAST_WINDOW + VOLUME_RECENT_WINDOW
    if len(df_group) < required_rows:
        return None

    df = df_group.tail(required_rows).copy()

    # ボックスレンジ部分の価格シリーズ
    price_series = df["現在値"].iloc[-BOX_RANGE_WINDOW:]
    current = price_series.iloc[-1]
    mean = price_series.mean()

    # BOX_TOLERANCEによるボックス内判定
    if abs(current - mean) / mean > BOX_TOLERANCE:
        return None

    # 範囲計算
    range_min = price_series.min()
    range_max = price_series.max()
    band_width = range_max - range_min
    if band_width == 0:
        return None

    # 位置比率（0 = 下端、1 = 上端）
    position_ratio = (current - range_min) / band_width

    # 出来高急増判定
    volume_ok = True
    if BOX_USE_VOLUME_SPIKE:
        recent_vol = df["出来高"].iloc[-VOLUME_RECENT_WINDOW:].mean()
        past_vol = df["出来高"].iloc[-(VOLUME_RECENT_WINDOW + VOLUME_PAST_WINDOW):-VOLUME_RECENT_WINDOW].mean()
        volume_ok = recent_vol > past_vol * BREAKOUT_VOLUME_RATIO

    # ボラティリティ安定判定
    volatility_ok = True
    if BOX_USE_VOLATILITY_FILTER:
        std_now = price_series.std()
        std_past = df["現在値"].iloc[-(VOLUME_RECENT_WINDOW + VOLUME_PAST_WINDOW):-VOLUME_RECENT_WINDOW].std()
        volatility_ok = std_now < std_past * BOX_VOLATILITY_RATIO

    # 条件満たせばシグナル返す
    if position_ratio <= (1 - BOX_EDGE_THRESHOLD) and volume_ok and volatility_ok:
        return {
            "シグナル": "【買い目】ボックスレンジ",
            "現在値": current,
            "平均値": round(mean, 2)
        }
    elif position_ratio >= BOX_EDGE_THRESHOLD and volume_ok and volatility_ok:
        return {
            "シグナル": "【売り目】ボックスレンジ",
            "現在値": current,
            "平均値": round(mean, 2)
        }

    return None


def detect_breakout(df_group):
    required_len = BREAKOUT_LOOKBACK + VOLUME_RECENT_WINDOW + VOLUME_PAST_WINDOW
    if len(df_group) < required_len:
        return None

    df = df_group.tail(required_len).copy()
    price_series = df["現在値"]
    volume_series = df["出来高"]
    current = price_series.iloc[-1]

    # 高値・安値ブレイク判定
    high_max = df["高値"].iloc[-(BREAKOUT_LOOKBACK+1):-1].max()
    low_min = df["安値"].iloc[-(BREAKOUT_LOOKBACK+1):-1].min()

    # 出来高急増チェック
    recent_volume = volume_series.iloc[-1]
    avg_volume = volume_series.iloc[-(BREAKOUT_LOOKBACK+1):-1].mean()
    volume_ok = recent_volume > avg_volume * BREAKOUT_VOLUME_RATIO

    # ボラティリティ急増チェック
    volatility_ok = True
    if BREAKOUT_USE_VOLATILITY_SPIKE:
        std_now = price_series.iloc[-BREAKOUT_LOOKBACK:].std()
        std_past = price_series.iloc[-(BREAKOUT_LOOKBACK + VOLUME_PAST_WINDOW):-BREAKOUT_LOOKBACK].std()
        volatility_ok = std_now > std_past * BREAKOUT_VOLATILITY_RATIO

    # 判定
    if current > high_max and volume_ok and volatility_ok:
        return {
            "シグナル": "【買い目】ブレイクアウト",
            "現在値": current,
            "高値上抜け基準": round(high_max, 2)
        }
    elif current < low_min and volume_ok and volatility_ok:
        return {
            "シグナル": "【売り目】ブレイクアウト",
            "現在値": current,
            "安値下抜け基準": round(low_min, 2)
        }

    return None


# ▼ ダブルトップ・ボトム検出（ピーク自動判定付き）
def detect_double_pattern(df_group):
    if len(df_group) < DOUBLE_PATTERN_LOOKBACK:
        return None

    df = df_group.tail(DOUBLE_PATTERN_LOOKBACK).copy()
    price = df["現在値"].iloc[-1]
    highs = df["高値"].values
    lows = df["安値"].values
    volumes = df["出来高"].values
    std_series = df["現在値"].rolling(window=STD_WINDOW).std()
    std_now = std_series.iloc[-1]
    std_avg = std_series.mean()

    peaks_high = [i for i in range(1, len(highs)-1) if highs[i-1] < highs[i] > highs[i+1]]
    valleys_low = [i for i in range(1, len(lows)-1) if lows[i-1] > lows[i] < lows[i+1]]

    # ▼ ダブルトップ検出
    if len(peaks_high) >= DOUBLE_PATTERN_MIN_PEAKS:
        i1, i2 = peaks_high[-2], peaks_high[-1]
        high1, high2 = highs[i1], highs[i2]
        mid_low = lows[min(i1+1, i2-1):max(i1, i2)].min()
        volume_avg = df["出来高"].mean()

        price_diff_ratio = abs(high1 - high2) / high1
        volume_spike = volumes[i1] > volume_avg * DOUBLE_PATTERN_VOLUME_SPIKE_RATIO and \
                       volumes[i2] > volume_avg * DOUBLE_PATTERN_VOLUME_SPIKE_RATIO
        volatility_jump = std_now > std_avg * DOUBLE_PATTERN_VOLATILITY_RATIO if DOUBLE_PATTERN_VOLATILITY_JUMP else True

        if price_diff_ratio < DOUBLE_PATTERN_TOLERANCE and price < mid_low and volume_spike and volatility_jump:
            return {
                "シグナル": "【売り目】ダブルトップ",
                "現在値": price,
                "ネックライン": round(mid_low, 2),
                "高値1": round(high1, 2),
                "高値2": round(high2, 2),
                "出来高急増": True,
                "ボラ急増": volatility_jump
            }

    # ▼ ダブルボトム検出
    if len(valleys_low) >= DOUBLE_PATTERN_MIN_PEAKS:
        i1, i2 = valleys_low[-2], valleys_low[-1]
        low1, low2 = lows[i1], lows[i2]
        mid_high = highs[min(i1+1, i2-1):max(i1, i2)].max()
        volume_avg = df["出来高"].mean()

        price_diff_ratio = abs(low1 - low2) / low1
        volume_spike = volumes[i1] > volume_avg * DOUBLE_PATTERN_VOLUME_SPIKE_RATIO and \
                       volumes[i2] > volume_avg * DOUBLE_PATTERN_VOLUME_SPIKE_RATIO
        volatility_jump = std_now > std_avg * DOUBLE_PATTERN_VOLATILITY_RATIO if DOUBLE_PATTERN_VOLATILITY_JUMP else True

        if price_diff_ratio < DOUBLE_PATTERN_TOLERANCE and price > mid_high and volume_spike and volatility_jump:
            return {
                "シグナル": "【買い目】ダブルボトム",
                "現在値": price,
                "ネックライン": round(mid_high, 2),
                "安値1": round(low1, 2),
                "安値2": round(low2, 2),
                "出来高急増": True,
                "ボラ急増": volatility_jump
            }

    return None





# ▼ 出力データから HTML テーブルを生成

def format_output_html(df):
    signal_order = [
        "【買い目】上昇トレンド", "【売り目】下降トレンド",
        "【買い目】ゴールデンクロス", "【売り目】デッドクロス",
        "【買い目】ボックスレンジ", "【売り目】ボックスレンジ",
        "【買い目】ブレイクアウト", "【売り目】ブレイクアウト",
        "【買い目】ダブルボトム", "【売り目】ダブルトップ"
    ]

    html = ["""
        <html><body>
        <style>
            table { border-collapse: collapse; width: 100%; font-family: sans-serif; }
            th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
            th { background-color: #f2f2f2; }
            h3 { margin-top: 24px; }
        </style>
        <table>
        """]

    for signal in signal_order:
        group = df[df["シグナル"] == signal]
        if group.empty:
            continue  # ⚠️ シグナルがない場合はそのセクションごとスキップ

        html.append(f"<tr><td colspan='5'><h3>■ {signal}</h3></td></tr>")
        for _, row in group.iterrows():
            code = str(row["銘柄コード"])
            name_full = str(row["銘柄名称"])
            name = name_full[:8] + "..." if len(name_full) > 8 else name_full
            price = f"{int(row['現在値']):,}円" if not pd.isna(row['現在値']) else "-"
            matsui_url = f"https://finance.matsui.co.jp/stock/{code}/index"
            x_url = f"https://x.com/search?q={code}%20{name}&src=typed_query&f=live"

            html.append(f"""
            <tr>
                <td>{code}</td>
                <td>{name}</td>
                <td>{price}</td>
                <td style='padding-left: 16px;'><a href="{matsui_url}" target="_blank">松井証券</a></td>
                <td style='padding-left: 16px;'><a href="{x_url}" target="_blank">X検索</a></td>
            </tr>""")

    html.append("</table>")
    html.append("""
                    <br><br>
                    <div style='font-family: sans-serif; font-size: 14px;'>
                        <strong><span style="color:red;">【注意】</span></strong><br>
                        <span style="color:red;">
                        本分析は、特定の銘柄の売買を推奨するものではありません。<br>
                        出力内容はあくまでテクニカル分析に基づく参考情報であり、最終的な投資判断はご自身の責任で慎重に行ってください。<br>
                        市場動向は常に変動するため、本分析の結果に過信せず、複数の情報を組み合わせた冷静な判断を心がけてください。
                        </span><br><br>

                        <strong>【シグナルの種類と意味】</strong><br>
                        - 【買い目】上昇トレンド：<br>
                        株価が短期・中期・長期の移動平均線の順に上向いており、トレンド、RSI、MACD、出来高が総合的に好調な場面で検出される買いシグナルです。<br>
                        特に「戻り」や「クロス」などの押し目を示唆する動きが直近に現れている銘柄が対象です。<br><br>

                        - 【売り目】下降トレンド：<br>
                        株価が移動平均線の順に下向きに並び、トレンド、RSI、MACD、出来高が総じて弱含む状況で検出される売りシグナルです。<br>
                        「戻り売り」や「デッドクロス」を伴う局面で、下落トレンドの加速が予測される銘柄が対象です。<br><br>

                        - 【買い目】ゴールデンクロス：<br>
                        短期移動平均線（MA5）が中期移動平均線（MA25）を下から上へ突き抜けたときの買いシグナルです。<br>
                        相場転換の兆しとして注目され、特にボラティリティが安定している局面でのシグナルが有効です。<br><br>

                        - 【売り目】デッドクロス：<br>
                        短期移動平均線（MA5）が中期移動平均線（MA25）を上から下へ割り込んだときの売りシグナルです。<br>
                        調整や下降局面の初動を捉える目的で使用され、安定的な下落圧力を示唆します。<br><br>

                        - 【買い目】ボックスレンジ：<br>
                        一定期間内の株価がレンジを形成し、その下限付近（サポートライン）で反発の兆しを見せている銘柄に対する逆張りの買いシグナルです。<br>
                        出来高が直近で急増し、ボラティリティが落ち着いていることが条件となります。<br><br>

                        - 【売り目】ボックスレンジ：<br>
                        ボックスレンジの上限（レジスタンス）に接近し、反落の兆しを見せている銘柄に対する逆張りの売りシグナルです。<br>
                        過熱感や出来高急増が確認されており、下落への転換が意識される局面で検出されます。<br><br>

                        - 【買い目】ブレイクアウト：<br>
                        過去の上値抵抗線を明確に突破し、かつ出来高も平均を大きく上回る場面で発生する強気の買いシグナルです。<br>
                        ボラティリティの急増とともに価格上昇が勢いを持っている初動を捉えます。<br><br>

                        - 【売り目】ブレイクアウト：<br>
                        サポートラインや直近安値を割り込み、出来高も伴って下方向への勢いが強まっているときの売りシグナルです。<br>
                        急落の始まりやトレンド転換のきっかけを狙う場面で効果的です。<br><br>

                        - 【買い目】ダブルボトム：<br>
                        株価が2度安値を付けた後、ネックラインを上抜けることで反転上昇の兆候とみなされる買いシグナルです。<br>
                        安値の水準がほぼ同じであり、出来高やボラティリティの急増を伴う場合に有効な買いタイミングとされます。<br><br>

                        - 【売り目】ダブルトップ：<br>
                        高値圏で2つの山を形成した後、ネックラインを下抜けることで下落トレンド入りを示唆する売りシグナルです。<br>
                        直近の高値水準が近く、出来高増加やボラティリティの上昇が確認できる局面で強い売りシグナルとして機能します。<br><br>


                    </div>
                    </body></html>
                    """)
    return "\n".join(html)


# ▼ SendGridでHTMLメール送信（BCCモード）
def send_output_dataframe_via_email(output_data, current_time):
    try:
        output_df = pd.DataFrame(output_data)
        signal_priority = [
            "【買い目】上昇トレンド", "【売り目】下降トレンド",
            "【買い目】ゴールデンクロス", "【売り目】デッドクロス",
            "【買い目】ボックスレンジ", "【売り目】ボックスレンジ",
            "【買い目】ブレイクアウト", "【売り目】ブレイクアウト",
            "【買い目】ダブルボトム", "【売り目】ダブルトップ"
        ]
        output_df["シグナル"] = pd.Categorical(output_df["シグナル"], categories=signal_priority, ordered=True)
        output_df = output_df.sort_values(by=["シグナル", "現在値"], ascending=[True, False])

        html_content = format_output_html(output_df)
        sendgrid_api_key = os.environ.get("SENDGRID_API_KEY")
        sender_email = os.environ.get("SENDER_EMAIL")
        email_list_path = "email_list.txt"
        formatted_time = f"{current_time[:2]}:{current_time[2:]}"
        email_subject = f"【{formatted_time}】株式 - テクニカルシグナル通知"

        with open(email_list_path, "r", encoding="utf-8") as f:
            recipient_emails = [email.strip() for email in f if email.strip()]

        message = Mail(
            from_email=Email(sender_email),
            to_emails=To(sender_email),
            subject=email_subject,
            html_content=html_content
        )
        message.bcc = [Bcc(email) for email in recipient_emails]
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"✅ HTMLメール送信完了（BCCモード）: ステータスコード = {response.status_code}")
    except Exception as e:
        print(f"🚫 メール送信エラー: {e}")


# ▼ ファイルを分析してメール送信する関数（修正済み: dfを直接渡す）
def analyze_and_display_filtered_signals(df, current_time):
    try:
        df.columns = df.columns.str.strip().str.replace("　", "").str.replace(" ", "")

        output_data = []
        for code, df_group in df.groupby("銘柄コード"):
            try:
                name = df_group["銘柄名称"].iloc[-1]
                signal = None

                # 各シグナルの評価
                for detector in [
                    detect_uptrend, detect_downtrend,
                    detect_golden_cross, detect_dead_cross,
                    detect_box_range, detect_breakout, detect_double_pattern
                ]:
                    result = detector(df_group)
                    if result:
                        result.update({"銘柄コード": code, "銘柄名称": name})
                        output_data.append(result)
                        break

            except Exception as e:
                print(f"⚠️ シグナル処理エラー（{code}）: {e}")

        # メール送信
        if output_data:
            send_output_dataframe_via_email(output_data, current_time)
        else:
            print("ℹ️ シグナルなし。メール送信スキップ")

    except Exception as e:
        print(f"🚫 データ処理エラー: {e}")


# ▼ 修正済み：監視ループ本体（build_intraday_dataframe() で当日CSVを全件取得）
while True:
    try:
        now = get_japan_time()

        # ▼ テスト日・テスト時刻があればそれを使う
        check_date = datetime.strptime(TEST_DATE, "%Y%m%d").date() if TEST_DATE else now.date()
        check_time = datetime.strptime(TEST_TIME, "%H%M").time() if TEST_TIME else now.time()
        current_time_str = TEST_TIME if TEST_TIME else now.strftime("%H%M")
        today_date_str = TEST_DATE if TEST_DATE else now.strftime("%Y%m%d")

        # ▼ 稼働条件チェック
        is_weekday = check_date.weekday() < 5
        is_not_holiday = not jpholiday.is_holiday(check_date)
        is_within_trading_time = (
            datetime.strptime("09:02", "%H:%M").time() <= check_time <= datetime.strptime("11:30", "%H:%M").time()
            or datetime.strptime("12:30", "%H:%M").time() <= check_time <= datetime.strptime("15:00", "%H:%M").time()
        )

        if is_weekday and is_not_holiday and is_within_trading_time:
            print(f"📂 処理対象日: {today_date_str}（時刻: {current_time_str}）")

            # ▼ 当日の全CSVを結合して分析
            df_all = build_intraday_dataframe(target_date=today_date_str)
            if not df_all.empty:
                print("🔎 データ結合完了。全銘柄分析を開始...")
                analyze_and_display_filtered_signals(df_all, current_time_str)
            else:
                print("📭 データが存在しないため、処理をスキップします。")
        else:
            print(f"⏳ 非稼働時間（週末 or 祝日 or 取引時間外）: {check_date} {check_time.strftime('%H:%M')}")

        print("⏲️ 1分間待機中...")
        time.sleep(60)

    except Exception as e:
        print(f"🚫 メインループエラー: {e}")
