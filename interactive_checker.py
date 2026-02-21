import pandas as pd
import webbrowser
import os

# --- 設定 ---
INPUT_FILE = "master_url_list.csv"   # チェックするリスト
OUTPUT_FILE = "delivery_list.txt"    # 納品用ファイル（ここに自動で書き込まれます）

def main():
    print("=== 爆速選別ツール（コピペ不要版） ===")
    
    # 1. リスト読み込み
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: {INPUT_FILE} が見つかりません。")
        return

    try:
        df = pd.read_csv(INPUT_FILE)
        # ヘッダーがない場合やカラム名が違う場合の対応
        if 'URL' in df.columns:
            urls = df['URL'].dropna().tolist()
        else:
            urls = df.iloc[:, 0].dropna().tolist()
    except:
        print("CSVの読み込みに失敗しました。")
        return

    # 2. 既にチェック済みのURLを除外（途中再開用）
    checked_urls = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            checked_urls = set([line.strip() for line in lines])
            print(f"※ 既に {len(checked_urls)} 件が納品リストに含まれています。これらはスキップします。")

    # フィルタリング（未チェックのものだけ残す）
    targets = [u for u in urls if u not in checked_urls]
    total = len(targets)

    if total == 0:
        print("全てのチェックが完了しています！")
        return

    print(f"\n残り {total} 件のチェックを開始します。")
    print("------------------------------------------------")
    print("【操作方法】")
    print("  Enterキーのみ : 「合格」 (保存して次へ)")
    print("  n + Enter    : 「不合格」 (保存せず次へ)")
    print("  q + Enter    : 「中断」 (終了)")
    print("------------------------------------------------\n")

    input("準備ができたらEnterを押してください（ブラウザが起動します）>> ")

    count = 0
    for i, url in enumerate(targets):
        print(f"\n[{i+1}/{total}] {url}")
        
        # ブラウザで開く
        webbrowser.open(url)
        
        # 判定待ち
        # 画面を切り替えてチェックした後、ここに戻ってキーを押す
        choice = input("合格ならEnter / ダメなら n >> ").lower().strip()

        if choice == 'q':
            print("中断します。お疲れ様でした。")
            break
        
        elif choice == 'n':
            print(" -> [不合格] スキップ")
        
        else:
            # Enterだけ押された場合（合格）
            print(" -> [合格！] 保存しました")
            with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
                f.write(url + "\n")
            count += 1

    print(f"\n=== 完了 ===")
    print(f"今回追加された合格件数: {count} 件")
    print(f"納品用ファイル: {OUTPUT_FILE}")
    print("最後にこのファイルの中身をWordに貼り付ければ納品完了です！")

if __name__ == "__main__":
    main()