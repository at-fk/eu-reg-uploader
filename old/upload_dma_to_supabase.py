from supabase import create_client
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import traceback
import uuid
import roman  # ローマ数字変換用パッケージを追加

# .envファイルから環境変数を読み込む
load_dotenv(override=True)

# 環境変数の値を確認
print("Environment variables:")
print(f"SUPABASE_URL: {os.environ.get('SUPABASE_URL')}")
print(f"SUPABASE_KEY: {os.environ.get('SUPABASE_KEY')}")

# Supabase設定
url: str = os.environ['SUPABASE_URL']
key: str = os.environ['SUPABASE_KEY']

print(f"Connecting to Supabase at {url}")

if not url or not key:
    print("Error: SUPABASE_URL または SUPABASE_KEY が設定されていません。")
    exit(1)

def load_dma_data():
    """DMAの構造化データを読み込む"""
    try:
        with open('dma_data/dma_structured.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"DMAデータの読み込み中にエラー: {e}")
        return None

def verify_data_structure(data):
    """データ構造の検証"""
    required_keys = ['metadata', 'recitals', 'chapters', 'articles']
    
    # 必要なキーの存在確認
    for key in required_keys:
        if key not in data:
            print(f"エラー: {key} が見つかりません")
            return False
    
    return True

def check_existing_dma(supabase):
    """既存のDMAデータをチェック"""
    try:
        response = supabase.table('regulations').select('*').eq('name', 'DMA').execute()
        return len(response.data) > 0
    except Exception as e:
        print(f"既存データのチェック中にエラー: {e}")
        return None

def delete_existing_dma(supabase):
    """既存のDMAデータを削除"""
    try:
        # regulationsテーブルからDMAを検索
        response = supabase.table('regulations').select('id').eq('name', 'DMA').execute()
        if not response.data:
            return True
        
        regulation_id = response.data[0]['id']
        
        # articlesテーブルから関連する記事のIDを取得
        articles_response = supabase.table('articles').select('id').eq('regulation_id', regulation_id).execute()
        article_ids = [article['id'] for article in articles_response.data]
        
        # paragraphsテーブルから関連するパラグラフのIDを取得
        for article_id in article_ids:
            paragraphs_response = supabase.table('paragraphs').select('id').eq('article_id', article_id).execute()
            # 各パラグラフに関連するparagraph_elementsを削除
            for paragraph in paragraphs_response.data:
                supabase.table('paragraph_elements').delete().eq('paragraph_id', paragraph['id']).execute()
            # パラグラフを削除
            supabase.table('paragraphs').delete().eq('article_id', article_id).execute()
        
        # 残りの関連データを削除
        tables = ['articles', 'chapters', 'recitals', 'annexes']
        for table in tables:
            supabase.table(table).delete().eq('regulation_id', regulation_id).execute()
        
        # 最後にregulationを削除
        supabase.table('regulations').delete().eq('id', regulation_id).execute()
        
        print("既存のDMAデータを削除しました")
        return True
    except Exception as e:
        print(f"既存データの削除中にエラー: {e}")
        traceback.print_exc()
        return False

def roman_to_int(roman_num):
    """ローマ数字を整数に変換"""
    try:
        return roman.fromRoman(roman_num)
    except roman.InvalidRomanNumeralError:
        print(f"Warning: Invalid Roman numeral {roman_num}")
        return 0

def to_roman(num):
    """整数をローマ数字に変換"""
    try:
        return roman.toRoman(num)
    except (ValueError, TypeError):
        print(f"Warning: Cannot convert {num} to Roman numeral")
        return str(num)

def upload_dma_data(supabase, data):
    """DMAデータをアップロード"""
    try:
        # document.mdからchapter-article対応を読み込む
        chapter_article_map = {}
        with open('document.md', 'r') as f:
            next(f)  # ヘッダー行をスキップ
            for line in f:
                chapter, section, article = line.strip().split(',')
                chapter_article_map[article] = chapter  # 既にローマ数字

        # jurisdictionsテーブルからEUのIDを取得
        response = supabase.table('jurisdictions').select('id').eq('code', 'EU').execute()
        if response.data:
            jurisdiction_id = response.data[0]['id']
            print(f"既存のJurisdictionを使用します（ID: {jurisdiction_id}）")
        else:
            # 存在しない場合は新規作成
            jurisdiction_data = {
                "code": "EU",
                "name": "European Union",
                "description": "European Union - Supranational organization in Europe",
                "metadata": {
                    "type": "supranational",
                    "region": "Europe"
                }
            }
            response = supabase.table('jurisdictions').insert(jurisdiction_data).execute()
            jurisdiction_id = response.data[0]['id']
            print(f"新規Jurisdictionを登録しました（ID: {jurisdiction_id}）")

        # 基本情報の登録
        regulation_data = {
            "jurisdiction_id": jurisdiction_id,
            "name": "DMA",
            "official_title": "Digital Markets Act",
            "short_title": "Digital Markets Act",
            "document_date": "2022-09-14",
            "effective_date": "2022-11-01",
            "version": "1.0",
            "status": "enacted",
            "metadata": {
                "uploaded_at": datetime.now().isoformat(),
                "source": "EUR-Lex",
                "source_url": "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925"
            }
        }
        
        # regulationsテーブルに登録
        response = supabase.table('regulations').insert(regulation_data).execute()
        if not response.data:
            print("Error: regulationsテーブルへの登録に失敗")
            return False
        
        regulation_id = response.data[0]['id']
        print(f"DMAを登録しました（ID: {regulation_id}）")
        
        # 前文の登録
        for recital in data['recitals']:
            recital_data = {
                "regulation_id": regulation_id,
                "recital_number": recital['recital_number'],
                "text": recital['text'],
                "metadata": recital.get('metadata', {})
            }
            supabase.table('recitals').insert(recital_data).execute()
        print(f"前文を登録しました（{len(data['recitals'])}件）")
        
        # チャプターの登録とIDのマッピング
        chapter_id_map = {}
        unique_chapters = sorted(set(chapter_article_map.values()), key=roman_to_int)
        for index, chapter_num in enumerate(unique_chapters, 1):
            chapter_data = {
                "regulation_id": regulation_id,
                "chapter_number": chapter_num,  # ローマ数字のまま
                "title": next((ch['title'] for ch in data['chapters'] if ch['chapter_number'] == chapter_num), f"Chapter {chapter_num}"),
                "order_index": index  # 1から始まる連番（アラビア数字）
            }
            response = supabase.table('chapters').insert(chapter_data).execute()
            chapter_id = response.data[0]['id']
            chapter_id_map[chapter_num] = chapter_id
            print(f"チャプター {chapter_num} を登録しました（order_index: {index}）")
        
        # 条文の登録とIDのマッピング
        article_id_map = {}
        for article in data['articles']:
            article_num = str(article['article_number'])
            chapter_num = chapter_article_map.get(article_num)
            if not chapter_num:
                print(f"Warning: Article {article_num} のchapter_id が見つかりません")
                continue

            chapter_id = chapter_id_map.get(chapter_num)
            if not chapter_id:
                print(f"Warning: Chapter {chapter_num} のIDが見つかりません")
                continue

            article_data = {
                "regulation_id": regulation_id,
                "chapter_id": chapter_id,
                "article_number": article_num,
                "title": article.get('title', ''),
                "content_full": '\n'.join([p.get('content_full', '') for p in article.get('paragraphs', [])]),
                "metadata": article.get('metadata', {}),
                "order_index": int(article_num)
            }
            response = supabase.table('articles').insert(article_data).execute()
            article_id = response.data[0]['id']
            article_id_map[article_num] = article_id
            
            # パラグラフの登録
            if 'paragraphs' in article:
                for paragraph in article['paragraphs']:
                    paragraph_data = {
                        "id": str(uuid.uuid4()),
                        "article_id": article_id,
                        "paragraph_number": paragraph.get('paragraph_number', ''),
                        "chapeau": None,  # chapeauはparagraph_elementsで管理
                        "content_full": paragraph.get('content_full', ''),
                        "metadata": paragraph.get('metadata', {})
                    }
                    response = supabase.table('paragraphs').insert(paragraph_data).execute()
                    paragraph_id = response.data[0]['id']
                    
                    # paragraph_elementsの登録
                    if 'ordered_contents' in paragraph:
                        for content in paragraph['ordered_contents']:
                            element_data = {
                                "id": str(uuid.uuid4()),
                                "paragraph_id": paragraph_id,
                                "type": content['type'],
                                "element_id": content.get('subparagraph_id', None),
                                "content": content['content'],
                                "order_index": content['order_index']
                            }
                            supabase.table('paragraph_elements').insert(element_data).execute()
                    
                    print(f"パラグラフ {paragraph.get('paragraph_number', '')} の要素を登録しました")
        
        print(f"条文、パラグラフ、パラグラフ要素を登録しました")

        # 附属書の登録
        if 'annexes' in data:
            for annex in data['annexes']:
                annex_data = {
                    "regulation_id": regulation_id,
                    "annex_number": annex['annex_number'],
                    "title": annex['title'],
                    "content": annex['content'],
                    "metadata": annex.get('metadata', {})
                }
                response = supabase.table('annexes').insert(annex_data).execute()
                print(f"附属書 {annex['annex_number']} を登録しました")

        return True
    
    except Exception as e:
        print(f"データのアップロード中にエラー: {e}")
        traceback.print_exc()
        return False

def main():
    try:
        # Supabaseクライアントの初期化
        supabase = create_client(url, key)
        
        # DMAデータの読み込み
        print("DMAデータを読み込んでいます...")
        data = load_dma_data()
        if not data:
            print("DMAデータの読み込みに失敗しました。")
            return
        
        # データ構造の検証
        print("\nデータ構造を検証しています...")
        if not verify_data_structure(data):
            print("データ構造の検証に失敗しました。")
            return
        
        # 既存のDMAデータをチェック
        print("\n既存のDMAデータをチェックしています...")
        has_existing = check_existing_dma(supabase)
        if has_existing:
            print("警告: DMAデータは既に存在します。")
            confirmation = input("既存のデータを削除して再アップロードしますか？ (y/N): ")
            if confirmation.lower() != 'y':
                print("アップロードを中止します。")
                return
            
            print("\n既存のDMAデータを削除しています...")
            if not delete_existing_dma(supabase):
                print("既存データの削除に失敗しました。")
                return
        
        # データのアップロード
        print("\nDMAデータをアップロードしています...")
        if upload_dma_data(supabase, data):
            print("\nDMAデータのアップロードが完了しました。")
        else:
            print("\nDMAデータのアップロードに失敗しました。")
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 