import google.generativeai as genai
import sqlite3
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import PyPDF2
import pdfplumber
import re
from datetime import datetime
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeminiAPIClient:
    """Gemini API統合クライアント"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        
        # モデルの初期化
        self.text_model = genai.GenerativeModel('gemini-2.5-pro')
        self.embedding_model = genai.GenerativeModel('text-embedding-004')
        
    def generate_summary(self, text: str) -> str:
        """Gemini 2.5 Proを使用してサマリーを生成"""
        try:
            prompt = f"""
            Create a detailed summary of this EDPB/WP29 guideline document. Start with a 2-3 sentence executive summary 
            that provides a high-level overview of the document's significance and main points.

            Then, cover the following aspects:

            1. Document Overview:
               - Main purpose and objectives
               - Target audience
               - Scope of application

            2. Key Topics and Requirements:
               - List main topics covered (e.g., consent, data processing, security measures)
               - Key obligations and requirements
               - Important definitions or concepts introduced

            3. Practical Implementation:
               - Required actions for compliance
               - Technical and organizational measures
               - Specific procedures or safeguards

            4. Related Areas:
               - Connection to other GDPR articles or guidelines
               - Relevant industry sectors or use cases
               - Cross-border implications if any

            Important Guidelines:
            - Include relevant keywords and phrases that users might search for
            - Use clear, specific language
            - Maximum length: 800 words
            - Structure the summary with clear sections
            - Include specific article numbers and references where relevant

            Document text:
            {text}
            """
            
            response = self.text_model.generate_content(prompt)
            return response.text
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            raise
    
    def get_embedding(self, text: str, dimensions: int = 768) -> List[float]:
        """text-embedding-004を使用して埋め込みを生成（768次元）"""
        try:
            result = genai.embed_content(
                model="models/text-embedding-004",
                content=text,
                output_dimensionality=dimensions
            )
            return result['embedding']
            
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            raise

class EDPBMetadataExtractor:
    """PDFメタデータ抽出クラス"""
    
    def __init__(self, gemini_client: GeminiAPIClient):
        self.gemini_client = gemini_client
    
    def normalize_text(self, text: str) -> str:
        """テキストの正規化処理"""
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\n', ' ')
        return text.strip()
    
    def extract_metadata(self, pdf_path: Path) -> Dict:
        """PDFからメタデータを抽出"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                page_count = len(pdf.pages)
                
                # 最初の3ページからテキスト抽出
                text = ''
                for i in range(min(3, len(pdf.pages))):
                    text += pdf.pages[i].extract_text() + '\n'
                
                normalized_text = self.normalize_text(text)
                logger.info(f"Extracting metadata from first pages of {pdf_path.name}")
                
                # Geminiを使用してメタデータを抽出
                prompt = f"""
                Extract the following information from this EDPB/WP29 document:
                1. Version
                2. Adoption date (format: YYYY-MM-DD)
                3. Document type (Guidelines/Opinion/Recommendation/Statement/Decision/Letter)
                4. Title (official title of the document)
                5. Working Party number (if WP29 document)
                6. EDPB number (if EDPB document)
                7. Subject matter
                8. Related GDPR articles (comma-separated)
                
                Format your response as JSON:
                {{
                    "version": "version or null",
                    "adopted_date": "YYYY-MM-DD or null",
                    "document_type": "type",
                    "title": "title",
                    "working_party_number": "WP number or null",
                    "edpb_number": "EDPB number or null",
                    "subject_matter": "subject",
                    "related_articles": "article numbers or null"
                }}

                Document text:
                {normalized_text}
                """
                
                response = self.gemini_client.text_model.generate_content(prompt)
                logger.debug(f"Raw API response: {response.text}")
                
                # JSONパースの改善
                response_text = response.text.strip()
                if response_text.startswith('```json'):
                    response_text = response_text[7:]
                if response_text.endswith('```'):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
                
                try:
                    metadata = json.loads(response_text)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parse error: {e}")
                    logger.error(f"Response text: {response_text}")
                    # デフォルト値で処理続行
                    metadata = {
                        "version": None,
                        "adopted_date": None,
                        "document_type": "Guidelines",
                        "title": pdf_path.stem,
                        "working_party_number": None,
                        "edpb_number": None,
                        "subject_matter": None,
                        "related_articles": None
                    }
                
                # ファイル情報を追加
                metadata['page_count'] = page_count
                metadata['file_size_bytes'] = pdf_path.stat().st_size
                metadata['filename'] = pdf_path.name
                
                logger.info(f"Extracted metadata: {json.dumps(metadata, indent=2)}")
                return metadata
                
        except Exception as e:
            logger.error(f"Metadata extraction error for {pdf_path}: {str(e)}")
            raise

class EDPBTextExtractor:
    """PDF全文抽出クラス"""
    
    def normalize_text(self, text: str) -> str:
        """テキストの正規化処理"""
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\n', ' ')
        return text.strip()
    
    def extract_full_text(self, pdf_path: Path) -> str:
        """PDFから全文を抽出"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = ''
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + '\n'
                    logger.debug(f"Page {i+1} extracted text length: {len(page_text)}")
                
                normalized_text = self.normalize_text(text)
                logger.info(f"Extracted {len(normalized_text)} characters from {pdf_path.name}")
                return normalized_text
                
        except Exception as e:
            logger.error(f"Text extraction error for {pdf_path}: {str(e)}")
            raise

class EDPBChunkProcessor:
    """テキストチャンク分割・埋め込み生成クラス"""
    
    def __init__(self, gemini_client: GeminiAPIClient, chunk_size: int = 1000, chunk_overlap: int = 100):
        self.gemini_client = gemini_client
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", " ", ""]
        )
    
    def create_chunks_with_embeddings(self, text: str) -> List[Dict]:
        """テキストをチャンクに分割し、埋め込みを生成"""
        chunks = self.text_splitter.split_text(text)
        chunk_data = []
        
        for i, chunk in enumerate(chunks):
            try:
                embedding = self.gemini_client.get_embedding(chunk, dimensions=768)
                chunk_info = {
                    'chunk_index': i,
                    'content': chunk,
                    'token_count': len(chunk.split()),
                    'embedding_vector': embedding,
                    'embedding_status': 'completed'
                }
                chunk_data.append(chunk_info)
                logger.info(f"Generated embedding for chunk {i+1}/{len(chunks)}")
                
                # API制限対応
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error processing chunk {i}: {str(e)}")
                chunk_info = {
                    'chunk_index': i,
                    'content': chunk,
                    'token_count': len(chunk.split()),
                    'embedding_vector': None,
                    'embedding_status': 'failed'
                }
                chunk_data.append(chunk_info)
        
        return chunk_data

class EDPBDatabaseHandler:
    """データベース保存処理クラス"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def _get_connection(self) -> sqlite3.Connection:
        """データベース接続を取得"""
        conn = sqlite3.Connection(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def log_processing_step(self, guideline_id: int, step: str, status: str, 
                          error_message: str = None, processing_time: float = None):
        """処理ステップをログに記録"""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO edpb_processing_log 
                (guideline_id, processing_step, status, error_message, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, (guideline_id, step, status, error_message, processing_time))
            conn.commit()
    
    def save_guideline(self, metadata: Dict) -> int:
        """ガイドライン基本情報を保存"""
        with self._get_connection() as conn:
            # ファイル名が重複している場合は、ユニークなファイル名を生成
            original_filename = metadata['filename']
            filename = original_filename
            counter = 1
            
            while True:
                try:
                    cursor = conn.execute("""
                        INSERT INTO edpb_guidelines 
                        (filename, title, document_type, version, adoption_date, page_count, 
                         file_size_bytes, working_party_number, edpb_number, subject_matter, 
                         related_articles, processing_status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing')
                    """, (
                        filename,
                        metadata['title'],
                        metadata['document_type'],
                        metadata.get('version'),
                        metadata.get('adopted_date'),
                        metadata['page_count'],
                        metadata['file_size_bytes'],
                        metadata.get('working_party_number'),
                        metadata.get('edpb_number'),
                        metadata.get('subject_matter'),
                        metadata.get('related_articles')
                    ))
                    conn.commit()
                    
                    # ファイル名を変更した場合はログ出力
                    if filename != original_filename:
                        logger.info(f"Filename modified to avoid duplicate: {original_filename} -> {filename}")
                    
                    return cursor.lastrowid
                    
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed: edpb_guidelines.filename" in str(e):
                        # ファイル名にカウンターを追加してリトライ
                        name_parts = original_filename.rsplit('.', 1)
                        if len(name_parts) == 2:
                            filename = f"{name_parts[0]}_{counter}.{name_parts[1]}"
                        else:
                            filename = f"{original_filename}_{counter}"
                        counter += 1
                        logger.info(f"Filename collision detected, trying: {filename}")
                        continue
                    else:
                        raise
    
    def update_guideline_content(self, guideline_id: int, summary: str, full_text: str):
        """ガイドラインのサマリーと全文を更新"""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE edpb_guidelines 
                SET summary = ?, full_text = ?, updated_at = CURRENT_TIMESTAMP
                WHERE guideline_id = ?
            """, (summary, full_text, guideline_id))
            conn.commit()
    
    def save_summary_embedding(self, guideline_id: int, embedding: List[float]) -> bool:
        """サマリーの埋め込みを保存"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT INTO edpb_summary_embeddings 
                    (guideline_id, embedding_vector)
                    VALUES (?, ?)
                """, (guideline_id, json.dumps(embedding)))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving summary embedding: {str(e)}")
            return False
    
    def save_chunks(self, guideline_id: int, chunks: List[Dict]) -> bool:
        """チャンクを保存"""
        try:
            with self._get_connection() as conn:
                for chunk in chunks:
                    embedding_json = json.dumps(chunk['embedding_vector']) if chunk['embedding_vector'] else None
                    conn.execute("""
                        INSERT INTO edpb_chunks 
                        (guideline_id, chunk_index, content, token_count, embedding_vector, embedding_status)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        guideline_id,
                        chunk['chunk_index'],
                        chunk['content'],
                        chunk['token_count'],
                        embedding_json,
                        chunk['embedding_status']
                    ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error saving chunks: {str(e)}")
            return False
    
    def mark_completed(self, guideline_id: int):
        """処理完了をマーク"""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE edpb_guidelines 
                SET processing_status = 'completed', updated_at = CURRENT_TIMESTAMP
                WHERE guideline_id = ?
            """, (guideline_id,))
            conn.commit()
    
    def mark_failed(self, guideline_id: int):
        """処理失敗をマーク"""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE edpb_guidelines 
                SET processing_status = 'failed', updated_at = CURRENT_TIMESTAMP
                WHERE guideline_id = ?
            """, (guideline_id,))
            conn.commit()

class EDPBProcessor:
    """メインプロセッサクラス - 全体の処理を統合"""
    
    def __init__(self, gemini_api_key: str, db_path: str, chunk_size: int = 1000, chunk_overlap: int = 100):
        self.gemini_client = GeminiAPIClient(gemini_api_key)
        self.metadata_extractor = EDPBMetadataExtractor(self.gemini_client)
        self.text_extractor = EDPBTextExtractor()
        self.chunk_processor = EDPBChunkProcessor(self.gemini_client, chunk_size, chunk_overlap)
        self.db_handler = EDPBDatabaseHandler(db_path)
    
    def process_pdf(self, pdf_path: Path) -> bool:
        """単一PDFファイルの完全処理"""
        start_time = time.time()
        guideline_id = None
        
        try:
            logger.info(f"Processing PDF: {pdf_path.name}")
            
            # 1. メタデータ抽出
            step_start = time.time()
            metadata = self.metadata_extractor.extract_metadata(pdf_path)
            step_time = time.time() - step_start
            
            # 2. データベースに基本情報保存
            guideline_id = self.db_handler.save_guideline(metadata)
            self.db_handler.log_processing_step(guideline_id, "metadata_extraction", "completed", processing_time=step_time)
            
            # 3. 全文抽出
            step_start = time.time()
            full_text = self.text_extractor.extract_full_text(pdf_path)
            step_time = time.time() - step_start
            self.db_handler.log_processing_step(guideline_id, "text_extraction", "completed", processing_time=step_time)
            
            # 4. サマリー生成
            step_start = time.time()
            summary = self.gemini_client.generate_summary(full_text[:8000])  # 最初の8000文字でサマリー生成
            step_time = time.time() - step_start
            self.db_handler.log_processing_step(guideline_id, "summary_generation", "completed", processing_time=step_time)
            
            # 5. ガイドライン内容を更新
            self.db_handler.update_guideline_content(guideline_id, summary, full_text)
            
            # 6. サマリーの埋め込み生成・保存
            step_start = time.time()
            summary_embedding = self.gemini_client.get_embedding(summary, dimensions=768)
            self.db_handler.save_summary_embedding(guideline_id, summary_embedding)
            step_time = time.time() - step_start
            self.db_handler.log_processing_step(guideline_id, "summary_embedding", "completed", processing_time=step_time)
            
            # 7. チャンク分割・埋め込み生成
            step_start = time.time()
            chunks = self.chunk_processor.create_chunks_with_embeddings(full_text)
            self.db_handler.save_chunks(guideline_id, chunks)
            step_time = time.time() - step_start
            self.db_handler.log_processing_step(guideline_id, "chunking_embedding", "completed", processing_time=step_time)
            
            # 8. 処理完了
            self.db_handler.mark_completed(guideline_id)
            
            total_time = time.time() - start_time
            logger.info(f"Successfully processed {pdf_path.name} in {total_time:.2f}s")
            return True
            
        except Exception as e:
            error_msg = f"Error processing {pdf_path.name}: {str(e)}"
            logger.error(error_msg)
            
            if guideline_id:
                self.db_handler.mark_failed(guideline_id)
                self.db_handler.log_processing_step(guideline_id, "processing", "failed", error_message=error_msg)
            
            return False
    
    def process_directory(self, directory_path: Path) -> Dict[str, int]:
        """ディレクトリ内のすべてのPDFを処理"""
        pdf_files = list(directory_path.glob("*.pdf"))
        results = {"success": 0, "failed": 0, "total": len(pdf_files)}
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        for pdf_file in pdf_files:
            if self.process_pdf(pdf_file):
                results["success"] += 1
            else:
                results["failed"] += 1
            
            logger.info(f"Progress: {results['success'] + results['failed']}/{results['total']}")
        
        return results