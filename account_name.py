import time
import pandas as pd
from selenium import webdriver

# --- 設定 ---
INPUT_FILE = "verified_list_cleaned.csv"
OUTPUT_FILE = "final_delivery_list.csv"

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--lang=ja-JP')
    # 画像オフで高速化
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    return driver

def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    driver = setup_driver()
    results = []

    print(f"{len(urls)}件のアカウント名を取得します...")

    try:
        for i, url in enumerate(urls):
            print(f"[{i+1}/{len(urls)}] Accessing: {url}")
            driver.get(url)
            time.sleep(2) # 読み込み待ち
            
            # ページタイトルから名前を抜き出す
            # タイトル形式: "名前 (@username) • Instagram photos and videos"
            page_title = driver.title
            if "• Instagram photos and videos" in page_title:
                page_title = page_title.replace("• Instagram photos and videos", "")
            
            # 名前部分だけを抽出（" (" より前の文字）
            display_name = page_title.split(" (")[0]
            
            # ログイン画面などで取れなかった場合の処理
            if "Instagram" in display_name and len(display_name) < 15:
                # URLからIDを代わりに入れる
                display_name = url.rstrip('/').split('/')[-1]

            results.append({"アカウント名": display_name, "URL": url})

    finally:
        driver.quit()

    # 保存
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n保存完了: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()