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

def insert_articles_from_json(json_file_path):
    # JSONファイルを読み込む
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    try:
        # EHDSのregulation_idを取得
        result = supabase.table('regulations').select('id').eq('name', 'EHDS').execute()
        regulation_id = result.data[0]['id']
        
        # 各条文のデータを整形
        articles_values = []
        paragraphs_values = []
        subparagraphs_values = []
        
        for article in data['articles']:
            # 条文全体のテキストを構築
            content_full = ""
            if article.get('paragraphs'):
                for para in article['paragraphs']:
                    content_full += para['chapeau'] or ""
                    if para.get('subparagraphs'):
                        for subpara in para['subparagraphs']:
                            content_full += " " + subpara['text']
                    content_full += "\n"

            # 条文データ
            article_data = {
                'regulation_id': regulation_id,
                'article_number': article['article_num'],
                'title': article.get('title'),
                'content_full': content_full.strip(),
                'metadata': {
                    'source_file': data['metadata']['source_file']
                }
            }
            
            # 条文を挿入
            article_result = supabase.table('articles').insert(article_data).execute()
            article_id = article_result.data[0]['id']
            
            # パラグラフデータを処理
            if article.get('paragraphs'):
                for para in article['paragraphs']:
                    para_content = para['chapeau'] or ""
                    if para.get('subparagraphs'):
                        for subpara in para['subparagraphs']:
                            para_content += " " + subpara['text']
                    
                    # パラグラフデータ
                    paragraph_data = {
                        'article_id': article_id,
                        'paragraph_number': para['paragraph_num'],
                        'chapeau': para['chapeau'],
                        'content_full': para_content.strip()
                    }
                    
                    # パラグラフを挿入
                    para_result = supabase.table('paragraphs').insert(paragraph_data).execute()
                    paragraph_id = para_result.data[0]['id']
                    
                    # サブパラグラフデータを処理
                    if para.get('subparagraphs'):
                        for idx, subpara in enumerate(para['subparagraphs']):
                            subparagraph_data = {
                                'paragraph_id': paragraph_id,
                                'subparagraph_id': subpara['subparagraph_id'],
                                'content': subpara['text'],
                                'type': subpara['type'],
                                'order_index': idx
                            }
                            # サブパラグラフを挿入
                            supabase.table('subparagraphs').insert(subparagraph_data).execute()
        
        print(f"Successfully processed {len(data['articles'])} articles")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    insert_articles_from_json('parsed_articles_par.json') 