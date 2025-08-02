from supabase import create_client
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# Supabase設定
url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

def check_production_data():
    try:
        # Supabaseクライアントの初期化
        supabase = create_client(url, key)
        
        # regulationsテーブルのDMAデータを確認
        print("\nregulationsテーブルの確認...")
        response = supabase.table('regulations').select('*').eq('name', 'DMA').execute()
        if response.data:
            regulation_id = response.data[0]['id']
            print(f"DMAが見つかりました（ID: {regulation_id}）")
            
            # 関連テーブルのデータ数を確認
            tables = ['recitals', 'chapters', 'articles', 'paragraphs', 'paragraph_elements']
            for table in tables:
                count = supabase.table(table).select('id').eq('regulation_id', regulation_id).execute()
                print(f"{table}テーブル: {len(count.data)}件")
        else:
            print("DMAは見つかりませんでした")
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    check_production_data() 