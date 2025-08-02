import json
from supabase import create_client
import os
from datetime import datetime

# Supabase設定（ローカル環境用）
SUPABASE_URL = "http://127.0.0.1:54321"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0"

# 本番環境のキーは以下のように保存しておく（コメントアウト）
# PROD_SUPABASE_URL = "https://gujeqgawwsnzkglyqeqq.supabase.co"
# PROD_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imd1amVxZ2F3d3NuemtnbHlxZXFxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzU2MTMyMzUsImV4cCI6MjA1MTE4OTIzNX0.3cwaiSSE4c3DoAf6EHbR4cF0lM_6J7pnwTR0Nrw3rsE"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_dma_data():
    """DMAのデータを読み込む"""
    with open('dma_data/dma_structured.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def upload_regulation():
    """規制情報をアップロード"""
    try:
        dma_data = load_dma_data()
        print("Loaded DMA data:", dma_data['metadata'])
        
        regulation_data = {
            "name": "DMA",
            "official_title": dma_data['metadata']['title'],
            "short_title": "Digital Markets Act",
            "jurisdiction_id": "60e78380-0253-4cc4-a24b-c15c2eb1a375",  # EUのjurisdiction_id
            "document_date": "2022-09-14",  # DMAの制定日
            "effective_date": "2022-11-01",  # DMAの施行日
            "version": "1.0",
            "status": "in_force"
        }
        
        print("Attempting to insert regulation data:", regulation_data)
        
        # まず、jurisdictionsテーブルの存在を確認
        check_jurisdiction = supabase.table('jurisdictions').select('*').execute()
        print("Jurisdictions in database:", check_jurisdiction.data)
        
        # regulations テーブルの構造を確認
        try:
            check_regulations = supabase.table('regulations').select('*').limit(1).execute()
            print("Regulations table exists, current data:", check_regulations.data)
        except Exception as e:
            print("Error checking regulations table:", str(e))
        
        response = supabase.table('regulations').insert(regulation_data).execute()
        print("Response from Supabase:", response)
        print("Regulation uploaded successfully")
        return response.data[0]['id']
    except Exception as e:
        print(f"Error occurred in upload_regulation: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def upload_recitals(regulation_id):
    """前文をアップロード"""
    try:
        dma_data = load_dma_data()
        recitals_data = []
        
        for recital in dma_data['recitals']:
            recitals_data.append({
                "regulation_id": regulation_id,
                "recital_number": recital['recital_number'],
                "text": recital['text'],
                "metadata": recital['metadata']
            })
        
        result = supabase.table('recitals').insert(recitals_data).execute()
        print(f"Uploaded {len(recitals_data)} recitals successfully")
        return result
    except Exception as e:
        print(f"Error uploading recitals: {e}")
        return None

def upload_chapters(regulation_id):
    """章をアップロード"""
    try:
        dma_data = load_dma_data()
        chapters_data = []
        
        if 'chapters' in dma_data:
            for chapter in dma_data['chapters']:
                chapters_data.append({
                    "regulation_id": regulation_id,
                    "chapter_number": chapter['chapter_number'],
                    "title": chapter['title'],
                    "order_index": chapter.get('order_index', 0)
                })
            
            result = supabase.table('chapters').insert(chapters_data).execute()
            print(f"Uploaded {len(chapters_data)} chapters successfully")
            return result
        else:
            print("No chapters found in DMA data")
            return None
    except Exception as e:
        print(f"Error uploading chapters: {e}")
        return None

def load_article_mappings():
    """document.mdから章と条文の対応関係を読み込む"""
    mappings = {}
    with open('document.md', 'r', encoding='utf-8') as f:
        next(f)  # ヘッダー行をスキップ
        for line in f:
            if line.strip():  # 空行をスキップ
                chapter, section, article = line.strip().split(',')
                mappings[str(article)] = {'chapter': str(chapter), 'section': section if section != 'null' else None}
    return mappings

def roman_to_int(roman):
    """ローマ数字を整数に変換"""
    roman_values = {
        'I': 1,
        'II': 2,
        'III': 3,
        'IV': 4,
        'V': 5,
        'VI': 6,
        'VII': 7,
        'VIII': 8,
        'IX': 9,
        'X': 10
    }
    return roman_values.get(roman, 0)

def get_chapter_id(chapter_number, chapters_data):
    """章番号からchapter_idを取得"""
    target_number = int(chapter_number)
    for chapter in chapters_data:
        roman_num = chapter['chapter_number']
        if roman_to_int(roman_num) == target_number:
            return chapter['id']
    return None

def upload_paragraphs_and_subparagraphs(article_id, paragraphs_data):
    """パラグラフとサブパラグラフをアップロード"""
    try:
        for paragraph in paragraphs_data:
            # パラグラフをアップロード
            paragraph_data = {
                "article_id": article_id,
                "paragraph_number": paragraph.get('paragraph_number', ''),
                "content_full": paragraph.get('content_full', ''),
                "chapeau": paragraph.get('chapeau', ''),
                "metadata": paragraph.get('metadata', {})
            }
            
            paragraph_result = supabase.table('paragraphs').insert(paragraph_data).execute()
            paragraph_id = paragraph_result.data[0]['id']
            
            # サブパラグラフがある場合はアップロード
            if 'subparagraphs' in paragraph and paragraph['subparagraphs']:
                for subparagraph in paragraph['subparagraphs']:
                    subparagraph_data = {
                        "paragraph_id": paragraph_id,
                        "subparagraph_id": subparagraph.get('subparagraph_id', subparagraph.get('subparagraph_number', '')),
                        "content": subparagraph.get('content', ''),
                        "type": subparagraph.get('type', 'alphabetic'),
                        "order_index": subparagraph.get('order_index', 0)
                    }
                    supabase.table('subparagraphs').insert(subparagraph_data).execute()
        
        return True
    except Exception as e:
        print(f"Error uploading paragraphs and subparagraphs: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def upload_articles(regulation_id):
    """条文をアップロード"""
    try:
        dma_data = load_dma_data()
        article_mappings = load_article_mappings()
        articles_data = []
        
        # まず、chaptersテーブルから全ての章情報を取得
        chapters_result = supabase.table('chapters').select('*').eq('regulation_id', regulation_id).execute()
        chapters_data = chapters_result.data
        print(f"Found {len(chapters_data)} chapters")
        print("Chapter data:", chapters_data)
        print("Article mappings:", article_mappings)
        
        if 'articles' in dma_data:
            for article in dma_data['articles']:
                article_number = str(article['article_number'])
                mapping = article_mappings.get(article_number)
                
                if mapping:
                    chapter_id = get_chapter_id(mapping['chapter'], chapters_data)
                    print(f"Article {article_number} -> Chapter {mapping['chapter']} -> ID {chapter_id}")
                    
                    # パラグラフデータの構造を確認
                    if 'paragraphs' in article:
                        print(f"\nParagraphs data for Article {article_number}:")
                        print(json.dumps(article['paragraphs'], indent=2))
                    
                    # 記事データを作成
                    article_data = {
                        "regulation_id": regulation_id,
                        "chapter_id": chapter_id,
                        "article_number": article_number,
                        "title": article.get('title', ''),
                        "content_full": article.get('content_full', article.get('content', '')),
                        "order_index": article.get('order_index', 0),
                        "metadata": article.get('metadata', {})
                    }
                    
                    # 記事をアップロード
                    article_result = supabase.table('articles').insert(article_data).execute()
                    article_id = article_result.data[0]['id']
                    
                    # パラグラフとサブパラグラフをアップロード
                    if 'paragraphs' in article:
                        success = upload_paragraphs_and_subparagraphs(article_id, article['paragraphs'])
                        if success:
                            print(f"Successfully uploaded paragraphs for Article {article_number}")
                        else:
                            print(f"Failed to upload paragraphs for Article {article_number}")
                    
                    articles_data.append(article_data)
                else:
                    print(f"Warning: No mapping found for article {article_number}")
            
            if articles_data:
                print("First article data:", articles_data[0])
                print(f"Uploaded {len(articles_data)} articles successfully")
                return True
            else:
                print("No articles data to upload")
                return None
        else:
            print("No articles found in DMA data")
            return None
    except Exception as e:
        print(f"Error uploading articles: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def main():
    try:
        # 規制情報をアップロード
        print("Uploading regulation data...")
        regulation_id = upload_regulation()
        if not regulation_id:
            raise Exception("Failed to upload regulation")
        print(f"Regulation uploaded with ID: {regulation_id}")
        
        # 前文をアップロード
        print("Uploading recitals...")
        recitals_result = upload_recitals(regulation_id)
        if not recitals_result:
            raise Exception("Failed to upload recitals")
        print("Recitals uploaded successfully")
        
        # 章をアップロード
        print("Uploading chapters...")
        chapters_result = upload_chapters(regulation_id)
        if chapters_result:
            print("Chapters uploaded successfully")
        
        # 条文をアップロード
        print("Uploading articles...")
        articles_result = upload_articles(regulation_id)
        if articles_result:
            print("Articles uploaded successfully")
        
        print("All DMA data uploaded successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    main() 