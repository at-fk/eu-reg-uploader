import json
from supabase import create_client
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# Supabase クライアントの初期化
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_ANON_KEY')
)

def insert_recitals_from_json(json_file_path):
    # JSONファイルを読み込む
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    try:
        # EHDSのregulation_idを取得
        result = supabase.table('regulations').select('id').eq('name', 'EHDS').execute()
        regulation_id = result.data[0]['id']
        
        # データを整形
        values = [
            {
                'regulation_id': regulation_id,
                'recital_number': recital['recital_num'],
                'text': recital['text'],
                'metadata': {}
            }
            for recital in data['recitals']
        ]
        
        # データを挿入
        result = supabase.table('recitals').insert(values).execute()
        
        print(f"Successfully inserted {len(values)} recitals")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    insert_recitals_from_json('parsed_recitals_par.json')