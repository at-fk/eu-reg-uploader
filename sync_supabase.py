import os
from dotenv import load_dotenv
from supabase import create_client, Client
import logging
from typing import List, Dict, Any

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()

# Supabaseクライアントの設定
local_supabase: Client = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_ANON_KEY')
)

cloud_supabase: Client = create_client(
    os.getenv('CLOUD_SUPABASE_URL'),
    os.getenv('CLOUD_SUPABASE_ANON_KEY')
)

# 同期が必要なテーブルのリスト
TABLES_TO_SYNC = [
    #実際にあるのは、jurisdictions, regulations, chapters, sections, articles, recitals, 
    # annexes, paragraphs, paragraph_elements, embeddings
    'jurisdictions',
    'tags',
    'regulations',
    'chapters',
    'sections',
    'articles',
    'recitals',
    'annexes',
    'paragraphs',
    'paragraph_elements',
    'legal_references',
    'content_tags',
    'embeddings',
]

def fetch_table_data(supabase: Client, table_name: str) -> List[Dict[Any, Any]]:
    """指定されたテーブルからすべてのデータを取得"""
    try:
        response = supabase.table(table_name).select("*").execute()
        return response.data
    except Exception as e:
        logger.error(f"テーブル {table_name} からのデータ取得中にエラーが発生: {str(e)}")
        raise

def sync_table(table_name: str, batch_size: int = 100) -> None:
    """ローカルからクラウドへテーブルデータを同期"""
    try:
        logger.info(f"テーブル {table_name} の同期を開始")
        
        # ローカルデータの取得
        local_data = fetch_table_data(local_supabase, table_name)
        logger.info(f"{len(local_data)} 件のレコードを取得")

        # バッチ処理
        for i in range(0, len(local_data), batch_size):
            batch = local_data[i:i + batch_size]
            try:
                # upsertを使用してデータを挿入または更新
                cloud_supabase.table(table_name).upsert(batch).execute()
                logger.info(f"バッチ {i//batch_size + 1} を同期完了 ({len(batch)} レコード)")
            except Exception as e:
                logger.error(f"バッチ {i//batch_size + 1} の同期中にエラーが発生: {str(e)}")
                raise

        logger.info(f"テーブル {table_name} の同期が完了")

    except Exception as e:
        logger.error(f"テーブル {table_name} の同期中にエラーが発生: {str(e)}")
        raise

def main():
    """メイン実行関数"""
    try:
        logger.info("データ同期プロセスを開始")
        
        for table in TABLES_TO_SYNC:
            sync_table(table)
            
        logger.info("すべてのテーブルの同期が完了")
        
    except Exception as e:
        logger.error(f"同期プロセス中にエラーが発生: {str(e)}")
        raise

if __name__ == "__main__":
    main() 