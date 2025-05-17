import os
import dropbox
import pandas as pd
import numpy as np
import datetime
import time

# ▼ 設定値
TEST_DATE = "20250515"
TEST_TIMES = ["1000"]

# ▼ 環境変数またはaccess_token.txtからアクセストークンを取得
ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")

if not ACCESS_TOKEN:
    with open("access_token.txt", "r") as f:
        ACCESS_TOKEN = f.read().strip()

# ▼ Dropboxクライアントの初期化
try:
    dbx = dropbox.Dropbox(ACCESS_TOKEN)
    print("✅ Dropboxに接続しました。")
except Exception as e:
    print(f"🚫 Dropbox接続エラー: {e}")
    exit(1)

# ▼ ファイルダウンロード関数
def download_csv_from_dropbox(file_name):
    try:
        dropbox_path = "/デイトレファイル/" + file_name
        local_path = "/mnt/data/" + file_name
        
        with open(local_path, "wb") as f:
            metadata, res = dbx.files_download(path=dropbox_path)
            f.write(res.content)
        
        print(f"✅ ダウンロード完了: {dropbox_path} -> {local_path}")
        return local_path
    except Exception as e:
        print(f"🚫 ファイルのダウンロードエラー: {e}")
        return None

# ▼ ファイル名の確認（テスト）
for test_time in TEST_TIMES:
    file_name = f"kabuteku{TEST_DATE}_{test_time}.csv"
    print(f"🔄 チェック中ファイル: {file_name}")

    file_path = download_csv_from_dropbox(file_name)
    if file_path:
        print(f"✅ ファイルが見つかりました: {file_path}")
    else:
        print(f"🚫 ファイルが見つかりません: {file_name}")
