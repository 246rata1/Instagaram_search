import time
import random
import pandas as pd
import urllib.parse
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================
# 設定エリア（ここを変更するだけで調整可能）
# ==========================================

# 1. 保存ファイル名
OUTPUT_FILE = "instagram_asset_list2.csv"

# 2. 検索キーワード構成（資産形成・教育メイン）
# これらを組み合わせて検索します
MAIN_KEYWORDS = [
    # "新NISA", "つみたてNISA", "iDeCo", "資産形成", "資産", "家計管理",
    # "節約", "貯金", "家計簿", "老後資金", "貯蓄", "積立", 
    # "投資", "インデックス投資", "ポイ活", "ふるさと納税",
    # "マネーリテラシー", "お金の勉強", "FP", "ファイナンシャルプランナー"
    "不動産", "不動産投資", "資産形成"
]

SUB_KEYWORDS = [
    "初心者", "ロードマップ", "始め方", "主婦", "ママ", 
    "共働き", "20代", "30代", "40代", "低収入", 
    "公務員", "看護師", "会社員", "ズボラ"
]

# 3. NGワード設定（絶対除外）
# これらがプロフィールや検索結果に含まれていたら即除外
NG_WORDS = [
    "FX", "fx", "ＦＸ", "バイナリー", "暗号資産", "仮想通貨", "ビットコイン", "BTC",
    "自動売買", "EA", "ツール", "サイン", "先出し", "爆益", "日利", "月利",
    "ギャンブル", "バカラ", "競艇", "競馬", "パチンコ", "オンラインカジノ",
    "借金返済", "即日", "現金", "プレゼント", "副業紹介", "コンサル生募集", "料理", "飯"
]


# 4. フォロワー数条件
MIN_FOLLOWERS = 5000     # 最低フォロワー数
MAX_FOLLOWERS = 500000   # 最大フォロワー数（有名人すぎる人を除外したい場合）

# 5. システム設定
MAX_WORKERS = 1          # ブラウザを同時に立ち上げる数（PCが重ければ減らす）
SEARCH_LIMIT_PER_KEYWORD = 50 # 1つのキーワード検索で深掘りする件数

# ==========================================
# 内部ロジック
# ==========================================

def setup_driver():
    """ブラウザの設定"""
    options = Options()
    options.add_argument('--lang=ja-JP')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # 画像読み込み無効化（高速化）
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    
    # ヘッドレスモード（画面を表示しない）
    options.add_argument('--headless')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver

def clean_instagram_url(url):
    """URLを綺麗な形（instagram.com/username/）にする"""
    try:
        if "?" in url:
            url = url.split("?")[0]
        if not url.endswith("/"):
            url += "/"
        return url
    except:
        return url

def get_username(url):
    """URLからユーザー名を取得"""
    try:
        parsed = urllib.parse.urlparse(url)
        path_parts = parsed.path.strip("/").split("/")
        # /p/xxxxx などの投稿URLを除外
        if any(x in path_parts for x in ["p", "reel", "stories", "explore", "tags", "tv"]):
            return None
        return path_parts[0]
    except:
        return None

def extract_followers_from_text(text):
    """テキスト（検索スニペット）からフォロワー数を抽出"""
    if not text: return 0
    
    # パターン: "フォロワー 1.2万人", "10K Followers"
    patterns = [
        r'フォロワー[:\s]*([\d,\.]+[万KkMm]?)人?',
        r'([\d,\.]+[KkMm万]?)\s*Followers',
        r'Followers:?\s*([\d,\.]+[KkMm万]?)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw_num = match.group(1)
            multiplier = 1
            raw_num = raw_num.replace(",", "")
            
            if "万" in raw_num: 
                multiplier = 10000
                raw_num = raw_num.replace("万", "")
            elif "K" in raw_num.upper(): 
                multiplier = 1000
                raw_num = raw_num.upper().replace("K", "")
            elif "M" in raw_num.upper(): 
                multiplier = 1000000
                raw_num = raw_num.upper().replace("M", "")
                
            try:
                return int(float(raw_num) * multiplier)
            except:
                continue
    return 0

def check_ng_words(text):
    """NGワードが含まれているかチェック"""
    if not text: return False # テキストがない場合はセーフ扱い（後で目視）
    for ng in NG_WORDS:
        if ng in text:
            return True # NGワード発見
    return False

def process_search_query(worker_id, queries):
    """検索を実行して候補URLを集める（フェーズ1）"""
    driver = setup_driver()
    found_urls = set()
    
    start_time = time.time()
    print(f"[Worker-{worker_id}] 検索開始: 担当キーワード {len(queries)} 個🔍")
    
    try:
        for idx, keyword in enumerate(queries):
            progress_pct = int((idx / len(queries)) * 100)
            elapsed = int(time.time() - start_time)
            print(f"  [Worker-{worker_id}] 進捗 [{idx+1}/{len(queries)}] ({progress_pct}%) | {elapsed}秒経過 | キーワード: '{keyword}'")
            
            # 検索クエリ作成：インスタ指定 + キーワード + NGワード除外
            # 例: site:instagram.com 新NISA -FX -バイナリー
            exclude_str = " ".join([f"-{w}" for w in NG_WORDS[:5]]) # 長すぎるとエラーになるので主要なものだけ
            full_query = f"site:instagram.com {keyword} {exclude_str}"
            
            driver.get("https://duckduckgo.com/")
            try:
                # 検索ボックスに入力
                search_box = driver.find_element(By.NAME, "q")
                search_box.clear()
                search_box.send_keys(full_query)
                search_box.send_keys(Keys.RETURN)
                time.sleep(3) # 読み込み待ち

                # スクロールして件数を稼ぐ
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)

                # URL取得
                elements = driver.find_elements(By.XPATH, "//a[contains(@href, 'instagram.com')]")
                found_count = 0
                for elem in elements:
                    url = elem.get_attribute("href")
                    username = get_username(url)
                    if username:
                        clean_url = f"https://www.instagram.com/{username}/"
                        found_urls.add(clean_url)
                        found_count += 1
                
                print(f"    → {keyword}: {found_count}個 取得 (合計: {len(found_urls)}個)")
            
            except Exception as e:
                print(f"[Worker-{worker_id}] キーワード '{keyword}' でエラー: {e}")
                continue
                
            time.sleep(random.uniform(2, 4)) # レート制限回避
            
    finally:
        driver.quit()
        total_time = int(time.time() - start_time)
        print(f"[Worker-{worker_id}] ✅ 検索完了: {len(found_urls)}個 | 所要時間: {total_time}秒")
        
    return found_urls

def process_verification(worker_id, urls):
    """URLごとの詳細チェック（フェーズ2：フォロワー数＆NG判定）"""
    driver = setup_driver()
    valid_accounts = []
    
    start_time = time.time()
    print(f"[Worker-{worker_id}] 詳細チェック開始: {len(urls)}件 ✓")
    
    try:
        for i, url in enumerate(urls):
            username = get_username(url)
            if not username: continue
            
            # 進捗表示（5件ごと）
            if (i+1) % 5 == 0:
                progress_pct = int(((i+1) / len(urls)) * 100)
                elapsed = int(time.time() - start_time)
                print(f"  [Worker-{worker_id}] 検査中 [{i+1}/{len(urls)}] ({progress_pct}%) | {elapsed}秒経過")
            
            # DuckDuckGoで「username followers」と検索してスニペットを見る
            # これによりインスタにログインせずに情報を抜く
            search_query = f'site:instagram.com/{username}'
            
            try:
                driver.get(f"https://duckduckgo.com/?q={urllib.parse.quote(search_query)}")
                time.sleep(random.uniform(2, 3))
                
                # ページテキスト取得
                body_element = driver.find_element(By.TAG_NAME, "body")
                page_text = body_element.text
                
                # 1. NGワードチェック
                if check_ng_words(page_text):
                    # print(f"[Worker-{worker_id}] ❌ NGワード検出 -> {username}")
                    continue
                
                # 2. フォロワー数チェック
                followers = extract_followers_from_text(page_text)
                
                if followers >= MIN_FOLLOWERS:
                    print(f"[Worker-{worker_id}] ✅ 合格! {followers:,}人 -> @{username}")
                    valid_accounts.append({
                        "Title": username, # 仮
                        "URL": url,
                        "Followers": followers,
                        "Note": "自動判定OK"
                    })
                else:
                    # フォロワー数が取れなかった、または足りない
                    pass

            except Exception as e:
                continue

    finally:
        driver.quit()
        total_time = int(time.time() - start_time)
        print(f"[Worker-{worker_id}] ✅ チェック完了: {len(valid_accounts)}個合格 | 所要時間: {total_time}秒")
    
    return valid_accounts

# ==========================================
# メイン実行部
# ==========================================
def main():
    print("=" * 60)
    print("=== Instagram 自動リストアップツール（統合版） ===")
    print("=" * 60)
    
    # ---------------------------
    # Phase 1: キーワード生成と検索
    # ---------------------------
    all_queries = []
    for m in MAIN_KEYWORDS:
        for s in SUB_KEYWORDS:
            all_queries.append(f"{m} {s}")
    # 単体キーワードも追加
    all_queries.extend(MAIN_KEYWORDS)
    
    # ランダムにシャッフル
    random.shuffle(all_queries)
    
    print(f"\n📋 検索パターン数: {len(all_queries)} 通り")
    print(f"⚙️  並列ワーカー数: {MAX_WORKERS}")
    print("\n" + "=" * 60)
    print("🔍 Phase 1: アカウント候補を収集中...")
    print("=" * 60)

    phase1_start = time.time()
    candidate_urls = set()
    
    # 並列処理で検索
    chunk_size = (len(all_queries) // MAX_WORKERS) + 1
    chunks = [all_queries[i:i + chunk_size] for i in range(0, len(all_queries), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_search_query, i+1, chunk) for i, chunk in enumerate(chunks)]
        for future in as_completed(futures):
            candidate_urls.update(future.result())
            
    phase1_time = int(time.time() - phase1_start)
    print(f"\n✅ Phase 1 完了")
    print(f"   📊 ユニークURL候補数: {len(candidate_urls)} 件")
    print(f"   ⏱️  所要時間: {phase1_time}秒")
    
    if len(candidate_urls) == 0:
        print("\n❌ 候補が見つかりませんでした。終了します。")
        return

    # ---------------------------
    # Phase 2: 詳細フィルタリング
    # ---------------------------
    print("\n" + "=" * 60)
    print("✓ Phase 2: フォロワー数とNGワードのチェック中...")
    print("=" * 60)
    
    phase2_start = time.time()
    url_list = list(candidate_urls)
    verified_data = []
    
    # 並列処理でチェック
    chunk_size_v = (len(url_list) // MAX_WORKERS) + 1
    chunks_v = [url_list[i:i + chunk_size_v] for i in range(0, len(url_list), chunk_size_v)]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_verification, i+1, chunk) for i, chunk in enumerate(chunks_v)]
        for future in as_completed(futures):
            verified_data.extend(future.result())
    
    phase2_time = int(time.time() - phase2_start)
    print(f"\n✅ Phase 2 完了")
    print(f"   📊 合格アカウント数: {len(verified_data)} 件")
    print(f"   ⏱️  所要時間: {phase2_time}秒")
    
    # ---------------------------
    # 保存処理
    # ---------------------------
    print("\n" + "=" * 60)
    print("💾 結果を保存中...")
    print("=" * 60)
    
    if verified_data:
        df = pd.DataFrame(verified_data)
        # フォロワー数で降順ソート
        df = df.sort_values(by="Followers", ascending=False)
        
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print("\n" + "=" * 60)
        print("🎉 完了！")
        print("=" * 60)
        print(f"✅ {len(df)} 件のアカウントリストを作成しました。")
        print(f"📁 保存先: {OUTPUT_FILE}")
        print(f"⏱️  総処理時間: {phase1_time + phase2_time}秒")
        print("\n📌 次のステップ:")
        print(f"   1. Excelで {OUTPUT_FILE} を開く")
        print("   2. URLをクリックしてアカウントを確認")
        print("   3. 「いいね数/再生数」を目視チェック")
        print("=" * 60)
    else:
        print("\n❌ 条件に合うアカウントが残りませんでした。")

if __name__ == "__main__":
    main()