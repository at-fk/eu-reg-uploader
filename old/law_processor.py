import os
import json
from supabase import create_client
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import time
from rich.console import Console
from rich.table import Table
from rich import print as rprint

# .envファイルから環境変数を読み込む
load_dotenv()

# Supabase クライアントの初期化
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_ANON_KEY')
)

# Jina AI の設定
JINA_API_KEY = os.getenv('JINA_API_KEY')
EMBEDDING_API_URL = "https://api.jina.ai/v1/embeddings"

class LawProcessor:
    def __init__(self):
        self.console = Console()
        
    def get_embedding(self, text: str, max_retries: int = 3, retry_delay: int = 5) -> List[float]:
        """Jina AI APIを使用してテキストのembeddingを取得"""
        headers = {
            "Authorization": f"Bearer {JINA_API_KEY}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "jina-embeddings-v3",
            "task": "retrieval.passage",
            "late_chunking": False,
            "dimensions": "256",
            "embedding_type": "float",
            "input": [text]
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.post(EMBEDDING_API_URL, headers=headers, json=data)
                if response.status_code == 200:
                    return response.json()["data"][0]["embedding"]
                elif response.status_code == 502:
                    if attempt < max_retries - 1:
                        self.console.print(f"[yellow]Bad Gateway error, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        continue
                raise Exception(f"Error getting embedding (status code {response.status_code}): {response.text}")
            except Exception as e:
                if attempt < max_retries - 1:
                    self.console.print(f"[yellow]Error occurred, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                    self.console.print(f"[red]Error details: {str(e)}")
                    time.sleep(retry_delay)
                    continue
                raise e

    def preview_data(self, regulation_id: str, data_type: str) -> None:
        """データのプレビューを表示"""
        if data_type in ["article", "chapter", "recital", "paragraph", "annex"]:
            # テーブル名を複数形に変更
            table_name = f"{data_type}s"
            data = supabase.table(table_name).select('*').eq('regulation_id', regulation_id).execute()
            total_count = len(data.data)
            table = Table(title=f"{data_type.capitalize()} Preview (showing 3 of {total_count} items)")
            table.add_column("ID")
            table.add_column("Title/Number")
            table.add_column("Content")
            
            preview_count = min(3, len(data.data))
            for item in data.data[:preview_count]:
                title = item.get('title') or item.get(f'{data_type}_number', '')
                content = item.get('content', '')[:100] + '...' if len(item.get('content', '')) > 100 else item.get('content', '')
                table.add_row(
                    str(item['id']),
                    str(title),
                    content
                )
        
        self.console.print(table)

    def process_data(self, regulation_id: str, data_type: str, mode: str = "preview", definition_article_number: str = None) -> None:
        """データの処理（プレビュー、アップロード、またはテスト）"""
        try:
            if mode == "preview":
                self.preview_data(regulation_id, data_type)
            elif mode == "test":
                self.process_test_data(regulation_id, data_type, definition_article_number)
            elif mode == "upload":
                if data_type == "article":
                    self.process_articles(regulation_id)
                elif data_type == "definition":
                    if definition_article_number:
                        self.process_definitions(regulation_id, definition_article_number)
                    else:
                        self.console.print("[red]Definition article number is required for processing definitions")
                elif data_type == "paragraph":
                    if definition_article_number:
                        self.process_paragraphs(regulation_id, definition_article_number)
                    else:
                        self.console.print("[red]Definition article number is required for processing paragraphs")
                elif data_type == "chapter":
                    self.process_chapters(regulation_id)
                elif data_type == "section":
                    self.process_sections(regulation_id)
                elif data_type == "recital":
                    self.process_recitals(regulation_id)
        except Exception as e:
            self.console.print(f"[red]Error processing {data_type}: {str(e)}")
            raise e

    def process_articles(self, regulation_id: str) -> None:
        """Articlesのタイトル情報と全文のembeddingを生成して保存"""
        articles = supabase.table('articles')\
            .select(
                'id, article_number, title, content_full, chapter_id, section_id'
            )\
            .eq('regulation_id', regulation_id)\
            .execute()
        
        for article in articles.data:
            # Chapter情報の取得
            chapter = supabase.table('chapters')\
                .select('chapter_number, title')\
                .eq('id', article['chapter_id'])\
                .execute()
            
            # Section情報の取得（存在する場合）
            section_info = ""
            if article['section_id']:
                section = supabase.table('sections')\
                    .select('section_number, title')\
                    .eq('id', article['section_id'])\
                    .execute()
                if section.data:
                    section_info = f", Section {section.data[0]['section_number']}: {section.data[0]['title']}"
            
            # コンテキストを含むタイトル情報を作成
            title_content = f"Chapter {chapter.data[0]['chapter_number']}: {chapter.data[0]['title']}{section_info}, Article {article['article_number']}: {article['title']}"
            
            # タイトルのembeddingを生成
            existing_title_embedding = supabase.table('embeddings')\
                .select('id')\
                .eq('source_type', 'article')\
                .eq('source_id', article['id'])\
                .eq('content_type', 'title_only')\
                .execute()
            
            if not existing_title_embedding.data:
                embedding = self.get_embedding(title_content)
                
                # embeddingsテーブルに保存
                supabase.table('embeddings').insert({
                    'source_type': 'article',
                    'source_id': article['id'],
                    'regulation_id': regulation_id,
                    'language_code': 'en',
                    'is_original': True,
                    'content_type': 'title_only',
                    'input_text': title_content,
                    'embedding': embedding,
                    'model_name': 'jina-embeddings-v3',
                    'model_version': 'base'
                }).execute()
                
                self.console.print(f"[green]Created title embedding for Article {article['article_number']}")
                time.sleep(0.5)  # API制限を考慮した待機
            
            # 全文のembeddingを生成
            existing_full_embedding = supabase.table('embeddings')\
                .select('id')\
                .eq('source_type', 'article')\
                .eq('source_id', article['id'])\
                .eq('content_type', 'full_text')\
                .execute()
            
            if not existing_full_embedding.data:
                full_content = f"{title_content}\n\n{article['content_full']}"
                embedding = self.get_embedding(full_content)
                
                # embeddingsテーブルに保存
                supabase.table('embeddings').insert({
                    'source_type': 'article',
                    'source_id': article['id'],
                    'regulation_id': regulation_id,
                    'language_code': 'en',
                    'is_original': True,
                    'content_type': 'full_text',
                    'input_text': full_content,
                    'embedding': embedding,
                    'model_name': 'jina-embeddings-v3',
                    'model_version': 'base'
                }).execute()
                
                self.console.print(f"[green]Created full text embedding for Article {article['article_number']}")
                time.sleep(0.5)  # API制限を考慮した待機

    def process_definitions(self, regulation_id: str, definition_article_number: str) -> None:
        """指定された条項のDefinitionsのサブパラグラフのembeddingを生成して保存"""
        # 指定された定義条項を取得
        article = supabase.table('articles')\
            .select('id')\
            .eq('regulation_id', regulation_id)\
            .eq('article_number', definition_article_number)\
            .execute()
        
        if not article.data:
            raise Exception(f"Article {definition_article_number} not found")
        
        article_id = article.data[0]['id']
        
        # 指定されたArticleに属するparagraphsを取得
        paragraphs = supabase.table('paragraphs')\
            .select('id')\
            .eq('article_id', article_id)\
            .execute()
        
        for paragraph in paragraphs.data:
            # 各paragraphのサブパラグラフを取得
            elements = supabase.table('paragraph_elements')\
                .select('id, element_id, content')\
                .eq('paragraph_id', paragraph['id'])\
                .eq('element_type', 'subparagraph')\
                .execute()
            
            for element in elements.data:
                # 既存のembeddingをチェック
                existing_embedding = supabase.table('embeddings')\
                    .select('id')\
                    .eq('source_type', 'subparagraph')\
                    .eq('source_id', element['id'])\
                    .eq('content_type', 'definition')\
                    .execute()
                
                if not existing_embedding.data:
                    # 定義の形式でコンテンツを作成
                    content = f"Definition {element['element_id']}: {element['content']}"
                    
                    embedding = self.get_embedding(content)
                    
                    # embeddingsテーブルに保存
                    supabase.table('embeddings').insert({
                        'source_type': 'subparagraph',
                        'source_id': element['id'],
                        'regulation_id': regulation_id,
                        'language_code': 'en',
                        'is_original': True,
                        'content_type': 'definition',
                        'input_text': content,
                        'embedding': embedding,
                        'model_name': 'jina-embeddings-v3',
                        'model_version': 'base'
                    }).execute()
                    
                    self.console.print(f"[green]Created embedding for Definition {element['element_id']}")
                    time.sleep(0.5)  # API制限を考慮した待機

    def process_chapters(self, regulation_id: str) -> None:
        """Chaptersのタイトル情報のembeddingを生成して保存"""
        chapters = supabase.table('chapters')\
            .select('id, chapter_number, title')\
            .eq('regulation_id', regulation_id)\
            .execute()
        
        for chapter in chapters.data:
            # 既存のembeddingをチェック
            existing_embedding = supabase.table('embeddings')\
                .select('id')\
                .eq('source_type', 'chapter')\
                .eq('source_id', chapter['id'])\
                .eq('content_type', 'title_only')\
                .execute()
            
            if not existing_embedding.data:
                # タイトル情報を作成
                content = f"Chapter {chapter['chapter_number']}: {chapter['title']}"
                
                embedding = self.get_embedding(content)
                
                # embeddingsテーブルに保存
                supabase.table('embeddings').insert({
                    'source_type': 'chapter',
                    'source_id': chapter['id'],
                    'regulation_id': regulation_id,
                    'language_code': 'en',
                    'is_original': True,
                    'content_type': 'title_only',
                    'input_text': content,
                    'embedding': embedding,
                    'model_name': 'jina-embeddings-v3',
                    'model_version': 'base'
                }).execute()
                
                self.console.print(f"[green]Created embedding for Chapter {chapter['chapter_number']}")
                time.sleep(0.5)  # API制限を考慮した待機

    def process_sections(self, regulation_id: str) -> None:
        """Sectionsのタイトル情報のembeddingを生成して保存"""
        sections = supabase.table('sections')\
            .select('id, section_number, title')\
            .eq('regulation_id', regulation_id)\
            .execute()
        
        for section in sections.data:
            # 既存のembeddingをチェック
            existing_embedding = supabase.table('embeddings')\
                .select('id')\
                .eq('source_type', 'section')\
                .eq('source_id', section['id'])\
                .eq('content_type', 'title_only')\
                .execute()
            
            if not existing_embedding.data:
                # タイトル情報を作成
                content = f"Section {section['section_number']}: {section['title']}"
                
                embedding = self.get_embedding(content)
                
                # embeddingsテーブルに保存
                supabase.table('embeddings').insert({
                    'source_type': 'section',
                    'source_id': section['id'],
                    'regulation_id': regulation_id,
                    'language_code': 'en',
                    'is_original': True,
                    'content_type': 'title_only',
                    'input_text': content,
                    'embedding': embedding,
                    'model_name': 'jina-embeddings-v3',
                    'model_version': 'base'
                }).execute()
                
                self.console.print(f"[green]Created embedding for Section {section['section_number']}")
                time.sleep(0.5)  # API制限を考慮した待機

    def process_recitals(self, regulation_id: str) -> None:
        """Recitalsのembeddingを生成して保存"""
        try:
            recitals = supabase.table('recitals')\
            .select('id, recital_number, text')\
            .eq('regulation_id', regulation_id)\
            .execute()
        
            for recital in recitals.data:
                # 既存のembeddingをチェック
                existing_embedding = supabase.table('embeddings')\
                    .select('id')\
                    .eq('source_type', 'recital')\
                    .eq('source_id', recital['id'])\
                    .eq('content_type', 'full_text')\
                    .execute()
            
                if not existing_embedding.data:
                    # 前文番号とテキストを組み合わせたコンテンツを作成
                    content = f"Recital {recital['recital_number']}: {recital['text']}"
                    
                    embedding = self.get_embedding(content)
                    
                    # embeddingsテーブルに保存
                    supabase.table('embeddings').insert({
                        'source_type': 'recital',
                        'source_id': recital['id'],
                        'regulation_id': regulation_id,
                        'language_code': 'en',
                        'is_original': True,
                        'content_type': 'full_text',
                        'input_text': content,
                        'embedding': embedding,
                        'model_name': 'jina-embeddings-v3',
                        'model_version': 'base'
                    }).execute()
                
                    self.console.print(f"[green]Created embedding for Recital {recital['recital_number']}")
                    time.sleep(0.5)  # API制限を考慮した待機

        except Exception as e:
            self.console.print("[red]Error processing {}: {}".format(data_type, str(e)))
            raise e

    def confirm_overwrite(self) -> bool:
        """既存のembeddingを上書きするか確認"""
        response = input("Existing embeddings found. Do you want to overwrite? (y/n): ")
        return response.lower() == 'y'

    def save_embedding(self, source_type: str, source_id: str, regulation_id: str, 
                      content_type: str, input_text: str, embedding: List[float]) -> None:
        """embeddingをデータベースに保存"""
        try:
            supabase.table('embeddings').insert({
                'source_type': source_type,
                'source_id': source_id,
                'regulation_id': regulation_id,
                'language_code': 'en',
                'is_original': True,
                'content_type': content_type,
                'input_text': input_text,
                'embedding': embedding,
                'model_name': 'jina-embeddings-v3',
                'model_version': 'base'
            }).execute()
            self.console.print(f"[green]Saved embedding for {source_type} {source_id}")
        except Exception as e:
            self.console.print(f"[red]Error saving embedding for {source_type} {source_id}: {e}")
            raise e
    
    def process_article_paragraphs(self, regulation_id: str, article_id: str, article_number: str) -> None:
        """条文のパラグラフを処理"""
        try:
            # パラグラフを取得
            paragraphs = supabase.table('paragraphs')\
                .select('id, paragraph_number, content_full')\
                .eq('article_id', article_id)\
                .execute()
            
            for paragraph in paragraphs.data:
                if paragraph.get('content_full'):
                    content = f"Article {article_number}, Paragraph {paragraph['paragraph_number']}: {paragraph['content_full']}"
                    embedding = self.get_embedding(content)
                    self.save_embedding(
                        source_type='paragraph',
                        source_id=str(paragraph['id']),
                        regulation_id=regulation_id,
                        content_type='full_text',
                        input_text=content,
                        embedding=embedding
                    )
            
            self.console.print(f"[green]Successfully processed paragraphs for Article {article_number}")
        
        except Exception as e:
            self.console.print(f"[red]Error processing paragraphs for Article {article_number}: {str(e)}")
    
    def process_paragraphs(self, regulation_id: str, definition_article_number: str) -> None:
        """定義条項以外の全ての条文のパラグラフのembeddingを生成して保存"""
        # 定義条項以外の全ての条文を取得
        articles = supabase.table('articles')\
            .select('id, article_number')\
            .eq('regulation_id', regulation_id)\
            .neq('article_number', definition_article_number)\
            .execute()
        
        for article in articles.data:
            # 各条文のパラグラフを処理
            self.process_article_paragraphs(regulation_id, article['id'], article['article_number'])
            
    def check_existing_embedding(self, source_type: str, regulation_id: str) -> bool:
        """既存のembeddingの存在チェック"""
        result = supabase.table('embeddings')\
            .select('id')\
            .eq('source_type', source_type)\
            .eq('regulation_id', regulation_id)\
            .execute()
        return bool(result.data)

    def process_test_data(self, regulation_id: str, data_type: str, definition_article_number: str = None) -> None:
        """各データタイプから3つのサンプルを取得してembeddingを生成"""
        try:
            if data_type == "article":
                articles = supabase.table('articles')\
                    .select('id, article_number, title, content_full')\
                    .eq('regulation_id', regulation_id)\
                    .limit(3)\
                    .execute()
                
                for article in articles.data:
                    content = f"Article {article['article_number']}: {article['title']}\n\n{article['content_full']}"
                    embedding = self.get_embedding(content)
                    self.save_embedding(
                        source_type='article',
                        source_id=article['id'],
                        regulation_id=regulation_id,
                        content_type='test_sample',
                        input_text=content,
                        embedding=embedding
                    )

            elif data_type == "chapter":
                chapters = supabase.table('chapters')\
                    .select('id, chapter_number, title')\
                    .eq('regulation_id', regulation_id)\
                    .limit(3)\
                    .execute()
                
                for chapter in chapters.data:
                    content = f"Chapter {chapter['chapter_number']}: {chapter['title']}"
                    embedding = self.get_embedding(content)
                    self.save_embedding(
                        source_type='chapter',
                        source_id=chapter['id'],
                        regulation_id=regulation_id,
                        content_type='test_sample',
                        input_text=content,
                        embedding=embedding
                    )

            elif data_type == "recital":
                recitals = supabase.table('recitals')\
                    .select('id, recital_number, text')\
                    .eq('regulation_id', regulation_id)\
                    .limit(3)\
                    .execute()
                
                for recital in recitals.data:
                    content = f"Recital {recital['recital_number']}: {recital['text']}"
                    embedding = self.get_embedding(content)
                    self.save_embedding(
                        source_type='recital',
                        source_id=recital['id'],
                        regulation_id=regulation_id,
                        content_type='test_sample',
                        input_text=content,
                        embedding=embedding
                    )

            elif data_type == "paragraph":
                if not definition_article_number:
                    self.console.print("[red]Definition article number is required for processing paragraphs")
                    return
                
                articles = supabase.table('articles')\
                    .select('id, article_number')\
                    .eq('regulation_id', regulation_id)\
                    .neq('article_number', definition_article_number)\
                    .limit(3)\
                    .execute()
                
                for article in articles.data:
                    paragraphs = supabase.table('paragraphs')\
                        .select('id, paragraph_number, content_full')\
                        .eq('article_id', article['id'])\
                        .limit(1)\
                        .execute()
                    
                    for paragraph in paragraphs.data:
                        content = f"Article {article['article_number']}, Paragraph {paragraph['paragraph_number']}: {paragraph['content_full']}"
                        embedding = self.get_embedding(content)
                        self.save_embedding(
                            source_type='paragraph',
                            source_id=paragraph['id'],
                            regulation_id=regulation_id,
                            content_type='test_sample',
                            input_text=content,
                            embedding=embedding
                        )

            elif data_type == "definition":
                if not definition_article_number:
                    self.console.print("[red]Definition article number is required for processing definitions")
                    return
                
                article = supabase.table('articles')\
                    .select('id')\
                    .eq('regulation_id', regulation_id)\
                    .eq('article_number', definition_article_number)\
                    .single()\
                    .execute()
                
                paragraphs = supabase.table('paragraphs')\
                    .select('id')\
                    .eq('article_id', article.data['id'])\
                    .execute()
                
                processed_count = 0
                for paragraph in paragraphs.data:
                    elements = supabase.table('paragraph_elements')\
                        .select('id, element_id, content')\
                        .eq('paragraph_id', paragraph['id'])\
                        .eq('type', 'subparagraph')\
                        .execute()
                    
                    for element in elements.data:
                        print(element)
                        if processed_count >= 3:  # 3つのサンプルを処理したら終了
                            break
                        
                        content = f"Definition {element['element_id']}: {element['content']}"
                        embedding = self.get_embedding(content)
                        self.save_embedding(
                            source_type='definition',
                            source_id=element['id'],
                            regulation_id=regulation_id,
                            content_type='test_sample',
                            input_text=content,
                            embedding=embedding
                        )
                        processed_count += 1
                    
                    if processed_count >= 3:
                        break

            elif data_type == "annex":
                annexes = supabase.table('annexes')\
                    .select('id, annex_number, title, content')\
                    .eq('regulation_id', regulation_id)\
                    .limit(3)\
                    .execute()
                
                for annex in annexes.data:
                    content = f"Annex {annex['annex_number']}: {annex['title']}\n\n{annex['content']}"
                    embedding = self.get_embedding(content)
                    self.save_embedding(
                        source_type='annex',
                        source_id=annex['id'],
                        regulation_id=regulation_id,
                        content_type='test_sample',
                        input_text=content,
                        embedding=embedding
                    )

            self.console.print(f"[green]Successfully processed test samples for {data_type}")

        except Exception as e:
            self.console.print(f"[red]Error processing test samples for {data_type}: {str(e)}")
            raise e

def main():
    processor = LawProcessor()
    console = Console()

    # 最初に定義条項の番号を入力してもらう
    definition_article_number = input("\nEnter the article number containing definitions (e.g., '2'): ")

    while True:
        console.print("\n[bold cyan]Law Processing Menu[/bold cyan]")
        console.print("1. Process Articles")
        console.print("2. Process Chapters")
        console.print("3. Process Sections")
        console.print("4. Process Recitals")
        console.print("5. Process Paragraphs")
        console.print("6. Process Definitions")
        console.print("7. Process Annexes")
        console.print("8. Test Sample Processing")
        console.print("9. Exit")

        choice = input("\nEnter your choice (1-9): ")

        if choice == "9":
            break

        if choice == "8":
            # テストモードの場合、データタイプを選択
            console.print("\n[bold cyan]Select Data Type for Testing[/bold cyan]")
            console.print("1. Articles")
            console.print("2. Chapters")
            console.print("3. Recitals")
            console.print("4. Paragraphs")
            console.print("5. Definitions")
            console.print("6. Annexes")
            
            test_choice = input("\nEnter your choice (1-6): ")
            data_type = None
            if test_choice == "1":
                data_type = "article"
            elif test_choice == "2":
                data_type = "chapter"
            elif test_choice == "3":
                data_type = "recital"
            elif test_choice == "4":
                data_type = "paragraph"
            elif test_choice == "5":
                data_type = "definition"
            elif test_choice == "6":
                data_type = "annex"
            else:
                console.print("[red]Invalid choice for test mode. Please try again.")
                continue
            
            # regulation_idの入力
            regulation_id = input("\nEnter the regulation ID: ")
            processor.process_data(regulation_id, data_type, "test", definition_article_number)
            continue

        # 通常の処理モードの場合
        data_types = {
            "1": "article",
            "2": "chapter",
            "3": "section",
            "4": "recital",
            "5": "paragraph",
            "6": "definition",
            "7": "annex"
        }

        if choice in data_types:
            regulation_id = input("Enter regulation ID: ")
            mode = input("Enter mode (preview/upload): ").lower()
            
            if mode not in ["preview", "upload"]:
                console.print("[red]Invalid mode. Please enter 'preview' or 'upload'")
                continue
            
            processor.process_data(regulation_id, data_types[choice], mode, definition_article_number)
        else:
            console.print("[red]Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
