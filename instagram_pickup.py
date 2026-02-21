import re
import time
import pandas as pd
from ddgs import DDGS
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------
# 設定・条件定義
# ---------------------------------------------------------

# 検索したいキーワード（資産形成・教育系）
SEARCH_KEYWORDS = [
    # NISA関連
    "新NISA 資産形成",
    "つみたてNISA 家計管理",
    "一般NISA 投資初心者",
    "NISA口座開設",
    "NISAロールオーバー",
    # 資産形成・貯蓄
    "資産形成 初心者",
    "貯蓄 計画",
    "貯金 貯蓄",
    "貯金術 節約",
    "家計管理 資産",
    # 投資
    "投資信託 初心者",
    "投資初心者 勉強",
    "株式投資 初心者",
    "配当金 タダ飯",
    "インデックス投資",
    # 節約
    "節約術 貯金",
    "節約 家計簿",
    "家計簿公開",
    "固定費削減",
    "家計管理 簿",
    # ポイント・お金
    "ポイ活 節約",
    "ポイント獲得 貯金",
    "ポイ活 初心者",
    "お金の勉強",
    "金融リテラシー",
    # その他
    "ファイナンシャルプランナー",
    "老後資金 対策",
    "ジュニアNISA 教育資金",
    "副業 起業",
    "給与 手取り"
]

# NGワード（これらが含まれていたら除外）
NG_WORDS = [
    "FX", "バイナリー", "暗号資産", "仮想通貨", "ビットコイン", "BTC",
    "自動売買", "ツール", "爆益", "先出し", "シグナル", "ギャンブル",
    "日利", "月利", "借金返済", "不労所得" # 不労所得は文脈によるが怪しいものが多いので一旦追加
]

# 目標収集数（各キーワードごとの最大取得数）
MAX_RESULTS_PER_KEYWORD = 1000 

# フォロワー数の最低ライン
MIN_FOLLOWERS = 5000

# ---------------------------------------------------------
# 関数定義
# ---------------------------------------------------------

def extract_follower_count(text):
    """
    検索スニペットからフォロワー数を抽出して数値化する関数
    戻り値: (推定人数(int), テキスト(str))
    """
    if not text:
        return 0, ""

    # パターン: "フォロワー 1.2万人", "10K Followers", "5000 Followers"
    # DuckDuckGoの検索結果には "〇〇 Followers" と出ることが多い
    
    # パターンA: 日本語「万人」表記 (例: 1.5万人)
    match_jp = re.search(r'(\d+(?:\.\d+)?)万\s*人?\s*フォロワー', text)
    if match_jp:
        num = float(match_jp.group(1)) * 10000
        return int(num), match_jp.group(0)

    # パターンB: "K"表記 (例: 10.5K Followers)
    match_k = re.search(r'(\d+(?:\.\d+)?)K\s*Followers', text, re.IGNORECASE)
    if match_k:
        num = float(match_k.group(1)) * 1000
        return int(num), match_k.group(0)

    # パターンC: 通常の数字 (例: 5000 Followers)
    match_num = re.search(r'([\d,]+)\s*Followers', text, re.IGNORECASE)
    if match_num:
        num = int(match_num.group(1).replace(',', ''))
        return num, match_num.group(0)

    return 0, "記載なし"

def is_profile_url(url):
    """
    Instagramのアカウントプロフィールページのみを許可する関数
    允許パターン: instagram.com/username/ または instagram.com/username
    除外パターン: /p/, /reel/, /explore/, /tv/, /stories/, /tagged/ など
    """
    if not url:
        return False
    
    # 除外パターン
    exclude_patterns = [
        '/p/', '/reel/', '/explore/', '/tv/', '/stories/', 
        '/tagged/', '/popular/', '/accounts/login', '?'
    ]
    
    for pattern in exclude_patterns:
        if pattern in url:
            return False
    
    # instagram.com/username/ または instagram.com/username の形式のみ許可
    # 正規表現で instagram.com/[username]/ の形式を確認
    pattern = r'instagram\.com/[a-zA-Z0-9._-]+/?$'
    match = re.search(pattern, url)
    return match is not None

def is_safe_content(text):
    """NGワードが含まれていないかチェック"""
    if not text:
        return True
    for ng in NG_WORDS:
        if ng in text:
            return False
    return True

def search_keyword(keyword, seen_urls):
    """1つのキーワードで検索を実行する関数（並列実行用）"""
    results_list = []
    query = f"site:instagram.com {keyword}"
    print(f"検索開始: {query}")
    
    try:
        with DDGS(timeout=30) as ddgs:
            results = ddgs.text(query, region='jp-jp', safesearch='off', max_results=MAX_RESULTS_PER_KEYWORD)
            
            for r in results:
                url = r.get('href', '')
                title = r.get('title', '')
                body = r.get('body', '')

                # URLがInstagramアカウントプロフィール以外の場合は除外
                if not is_profile_url(url):
                    continue
                
                # 重複除外
                if url in seen_urls:
                    continue

                # テキスト結合してチェック
                full_text = f"{title} {body}"

                # 1. NGワードチェック
                if not is_safe_content(full_text):
                    continue

                # 2. フォロワー数チェック（スニペットに記載がある場合のみ）
                follower_count, count_text = extract_follower_count(full_text)
                
                # フォロワー数が取得できて、かつ5000人未満なら除外
                if count_text != "記載なし" and follower_count < MIN_FOLLOWERS:
                    continue

                results_list.append({
                    "Keyword": keyword,
                    "Title": title,
                    "URL": url,
                    "Snippet": body,
                    "Estimated_Followers": follower_count if count_text != "記載なし" else "要確認",
                    "Follower_Text_Source": count_text
                })
                
                seen_urls.add(url)  # 重複チェック用に追加
        
        print(f"  完了: {keyword} -> {len(results_list)} 件")
        return results_list
        
    except Exception as e:
        print(f"  エラー {keyword}: {e}")
        return []

def search_instagram_candidates():
    results_list = []
    seen_urls = set()

    print(f"検索を開始します（並列実行）... 目標: フィルタリング前に約30000件")
    print(f"使用キーワード数: {len(SEARCH_KEYWORDS)} | 各キーワード最大: {MAX_RESULTS_PER_KEYWORD} 件\n")

    # ThreadPoolExecutorで複数キーワードを並列実行
    max_workers = min(6, len(SEARCH_KEYWORDS))  # 最大6スレッド（DuckDuckGoのレート制限回避）
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 各キーワードの検索タスクを送信
        future_to_keyword = {
            executor.submit(search_keyword, keyword, seen_urls): keyword 
            for keyword in SEARCH_KEYWORDS
        }
        
        # 完了した順に結果を取得
        for future in as_completed(future_to_keyword):
            keyword = future_to_keyword[future]
            try:
                keyword_results = future.result()
                results_list.extend(keyword_results)
                print(f"  合計候補数: {len(results_list)} 件")
            except Exception as e:
                print(f"  キーワード '{keyword}' 処理中にエラー: {e}")
            
            time.sleep(1)  # レート制限回避のための待機

    # DataFrame作成
    df = pd.DataFrame(results_list)
    
    # 重複削除（異なるキーワードで同じ人が引っかかるため）
    df = df.drop_duplicates(subset=['URL'])
    
    print("-" * 30)
    print(f"検索完了。重複削除後の候補数: {len(df)} 件")
    
    return df

# ---------------------------------------------------------
# メイン処理
# ---------------------------------------------------------

if __name__ == "__main__":
    df_result = search_instagram_candidates()
    
    if not df_result.empty:
        # Titleからアカウント名を抽出
        def extract_display_name(title):
            if not title:
                return "不明"
            
            # パターン1: "Account Name (@username) • Instagram photos and videos" の形式
            match = re.search(r'^([^(@]*?)\s*\(@', title)
            if match:
                name = match.group(1).strip()
                if name:
                    return name
            
            # パターン2: "Name • Description" 形式
            if ' • ' in title:
                name = title.split(' • ')[0].strip()
                if name and not name.startswith('http'):
                    return name
            
            # パターン3: "Name | Description" 形式
            if ' | ' in title:
                name = title.split(' | ')[0].strip()
                if name and not name.startswith('http'):
                    return name
            
            # パターン4: "Instagram" や "Хэштег" などの言語が含まれている場合は最初の部分を抽出
            if 'Instagram' in title:
                name = title.split('Instagram')[0].strip()
                if name and len(name) > 1:
                    return name
            
            # パターン5: "@" で始まっていない場合は最初の50文字までを取得
            name = title.split(' - ')[0].strip() if ' - ' in title else title.strip()
            if name and not name.startswith('http') and len(name) < 100:
                return name
            
            # URLから@usernameを抽出（最後の手段）
            return "不明"
        
        df_result['Account_Name'] = df_result['Title'].apply(extract_display_name)
        
        # アカウント名とURLだけの新しいDataFrameを作成
        df_simple = df_result[['Account_Name', 'URL']].copy()
        
        # 重複削除
        df_simple = df_simple.drop_duplicates(subset=['URL'])
        
        # CSVファイルとして保存
        filename = "instagram_candidates.csv"
        df_simple.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n結果を {filename} に保存しました。")
        print(f"アカウント数: {len(df_simple)} 件")
    else:
        print("候補が見つかりませんでした。")