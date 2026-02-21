import time
import random
import pandas as pd
import urllib.parse
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# --- 設定 ---
# フォロワー数フィルタリングが終わったファイル名
INPUT_CSV_FILE = "filtered_1000_list.csv"  

# 最終的に保存するファイル名
OUTPUT_CSV_FILE = "final_verified_list.csv" 

# プロフィールに含んでいてほしいキーワード（条件）
MUST_HAVE_KEYWORDS = [
    "資産", "投資", "NISA", "お金", "マネー", 
    "株", "貯金", "FP", "ファイナンシャル", "不動産", 
    "配当", "優待", "債券", "FX", "仮想通貨", "暗号資産",
    "運用", "貯蓄"
]

def setup_driver():
    """テストで成功したシンプルな起動設定"""
    options = webdriver.ChromeOptions()
    options.add_argument('--lang=ja-JP')
    options.add_argument("--window-size=1280,800")
    
    # 画像を読み込まない設定（高速化）
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    # ★ここが変更点: webdriver_managerを使わず、シンプルに起動
    driver = webdriver.Chrome(options=options)
    return driver

def check_bio_text(text):
    if not text: return False
    for keyword in MUST_HAVE_KEYWORDS:
        if keyword in text:
            return True
    return False

def get_username_from_url(url):
    try:
        parsed = urllib.parse.urlparse(url)
        path_parts = parsed.path.strip("/").split("/")
        return path_parts[-1]
    except:
        return None

def main():
    print("=== Instagram プロフィール判定（最終版）開始 ===")
    
    # 1. データ読み込み
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"エラー: {INPUT_CSV_FILE} が見つかりません。")
        return

    try:
        df = pd.read_csv(INPUT_CSV_FILE)
        print(f"入力データ: {len(df)} 件")
    except Exception as e:
        print(f"エラー: ファイル読み込み失敗 ({e})")
        return

    # 2. 既に完了したURLがあればスキップ（途中再開用）
    processed_urls = set()
    if os.path.exists(OUTPUT_CSV_FILE):
        try:
            done_df = pd.read_csv(OUTPUT_CSV_FILE)
            if 'URL' in done_df.columns:
                processed_urls = set(done_df['URL'].tolist())
                print(f"既存の完了データ {len(processed_urls)} 件をスキップします。")
        except:
            pass
            
    # 出力ファイルの準備（ヘッダー作成）
    if not os.path.exists(OUTPUT_CSV_FILE):
        pd.DataFrame(columns=['URL']).to_csv(OUTPUT_CSV_FILE, index=False, encoding="utf-8-sig")

    # ブラウザ起動
    print("ブラウザを起動中...")
    driver = setup_driver()
    driver.implicitly_wait(5)
    print("チェックを開始します。")

    total = len(df)
    success_count = 0

    try:
        for i, row in df.iterrows():
            target_url = row['URL']
            
            # スキップ処理
            if target_url in processed_urls:
                continue
                
            username = get_username_from_url(target_url)
            if not username:
                continue
            
            print(f"[{i+1}/{total}] {username} ...", end="")
            
            # DuckDuckGoでプロフィール検索
            query = f'site:instagram.com/{username}'
            encoded_query = urllib.parse.quote(query)
            ddg_url = f"https://duckduckgo.com/?q={encoded_query}&ia=web"
            
            is_verified = False
            try:
                driver.get(ddg_url)
                # 読み込み待ち（ランダムにしてブロック回避）
                time.sleep(random.uniform(2, 3))
                
                # ページ全体のテキストを取得
                body_text = driver.find_element(By.TAG_NAME, "body").text
                
                # キーワード判定
                if check_bio_text(body_text):
                    is_verified = True
                else:
                    # DDGで見つからない場合、念のためBingで再確認
                    print(" (Bingで再確認)...", end="")
                    bing_url = f"https://www.bing.com/search?q={encoded_query}"
                    driver.get(bing_url)
                    time.sleep(random.uniform(2, 3))
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    if check_bio_text(body_text):
                        is_verified = True

            except Exception as e:
                print(f" [Error] 通信エラー: {e}")
                continue

            # 結果処理
            if is_verified:
                print(" -> [OK] 合格")
                # URLのみを追記保存
                pd.DataFrame({'URL': [target_url]}).to_csv(OUTPUT_CSV_FILE, mode='a', header=False, index=False, encoding="utf-8-sig")
                success_count += 1
            else:
                print(" -> [NG] 除外")

    except KeyboardInterrupt:
        print("\n\n[停止] ユーザー操作により中断されました。")
        print("ここまでのデータは保存されています。")
    except Exception as e:
        print(f"\n[エラー] 予期せぬエラー: {e}")
    finally:
        driver.quit()
        print(f"\n=== 終了 ===")
        print(f"今回保存された件数: {success_count} 件")
        print(f"ファイル: {OUTPUT_CSV_FILE}")

if __name__ == "__main__":
    main()