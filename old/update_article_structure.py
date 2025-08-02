import pandas as pd
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

def update_article_structure(markdown_file_path):
    # Markdownファイルをパンダスで読み込む
    df = pd.read_table(markdown_file_path, sep='|')
    
    # データのクリーニング
    df = df.iloc[2:]  # ヘッダーと区切り行を削除
    df.columns = ['', 'chapter', 'chapter_title', 'section', 'section_title', 'article', '']
    df = df.drop(df.columns[[0, -1]], axis=1)  # 最初と最後の空列を削除
    df = df.apply(lambda x: x.str.strip())  # 空白の削除

    try:
        # 各articleに対して章と節のIDを取得して更新
        for _, row in df.iterrows():
            # 章のIDを取得
            chapter_result = supabase.table('chapters')\
                .select('id')\
                .eq('chapter_number', row['chapter'])\
                .execute()
            
            if not chapter_result.data:
                print(f"Chapter not found for article {row['article']}")
                continue
            
            chapter_id = chapter_result.data[0]['id']
            
            # 節のIDを取得（存在する場合）
            section_id = None
            if row['section'] != 'NaN':
                section_result = supabase.table('sections')\
                    .select('id')\
                    .eq('chapter_id', chapter_id)\
                    .eq('section_number', row['section'])\
                    .execute()
                
                if section_result.data:
                    section_id = section_result.data[0]['id']
            
            # articleの更新
            supabase.table('articles')\
                .update({
                    'chapter_id': chapter_id,
                    'section_id': section_id
                })\
                .eq('article_number', str(row['article']))\
                .execute()
            
        print("Successfully updated article structure")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_article_structure('extended_structure_table.md') 