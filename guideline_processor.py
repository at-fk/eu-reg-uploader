import tiktoken
from openai import OpenAI
from typing import List, Dict, Optional
import time
from tqdm import tqdm
import logging
from pathlib import Path
import PyPDF2
import json
import numpy as np
from langchain.text_splitter import RecursiveCharacterTextSplitter
import os
import pdfplumber
import re

# tiktokenのデバッグ出力を抑制
logging.getLogger('tiktoken').setLevel(logging.WARNING)

class GuidelineProcessor:
    def __init__(self, supabase_client, chunk_size: int = 1000, chunk_overlap: int = 100):
        self.supabase = supabase_client
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
        # 環境変数からAPIキーを読み込み
        openai_api_key = os.getenv('OPENAI_API_KEY')
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY is not set in environment variables")
            
        self.client = OpenAI(api_key=openai_api_key)
        
        # テキストスプリッターの初期化
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", " ", ""]
        )

    def normalize_text(self, text: str) -> str:
        """テキストの正規化処理"""
        # 複数の空白を単一の空白に置換
        text = re.sub(r'\s+', ' ', text)
        # 不要な改行を削除
        text = text.replace('\n', ' ')
        # 文字列の前後の空白を削除
        text = text.strip()
        return text

    def extract_metadata(self, pdf_path: str) -> Dict:
        """PDFからメタデータを抽出"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # ページ数を取得
                page_count = len(pdf.pages)
                
                # 最初の3ページまでを取得（タイトルページと重要な情報を含む可能性が高い）
                text = ''
                for i in range(min(3, len(pdf.pages))):
                    text += pdf.pages[i].extract_text() + '\n'
                
                # キストの正規化
                normalized_text = self.normalize_text(text)
                logging.info(f"\nFirst pages content for metadata extraction:")
                logging.info(f"{normalized_text[:500]}...\n")  # 最初の500文字のみログ出力

                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "You are an expert in analyzing GDPR documents."},
                        {"role": "user", "content": f"""
                        Extract the following information from this document:
                        1. Version
                        2. Adoption date
                        3. Document type (Guidelines/Opinion/Recommendation/Statement/Decision/Letter)
                        4. Title (official title of the document)
                        
                        Format your response as JSON:
                        {{
                            "version": "version",
                            "adopted_date": "date",
                            "document_type": "type",
                            "title": "title"
                        }}

                        Document text:
                        {normalized_text}
                        """}
                    ]
                )

                metadata = json.loads(response.choices[0].message.content)
                # ページ数を追加
                metadata['page_count'] = page_count
                
                logging.info("\nExtracted metadata:")
                logging.info(json.dumps(metadata, indent=2))
                return metadata

        except Exception as e:
            logging.error(f"Metadata extraction error: {str(e)}")
            return None

    def generate_summary(self, pdf_path: str) -> str:
        """PDFのサマリーを生成"""
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                intro_text = ''
                for i in range(min(6, len(reader.pages))):
                    intro_text += reader.pages[i].extract_text() + '\n'

                response = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": """You are an expert in GDPR and data protection law. 
                        Your task is to create searchable and comprehensive summaries of GDPR documents that will be used for vector similarity search."""},
                        {"role": "user", "content": f"""
                        Create a detailed summary of this GDPR document. Start with a 2-3 sentence executive summary 
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
                        {intro_text}
                        """}
                    ]
                )

                return response.choices[0].message.content

        except Exception as e:
            logging.error(f"Summary generation error: {str(e)}")
            return None

    def extract_text(self, pdf_path: str) -> str:
        """PDFからテキストを抽出"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = ''
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    logging.debug(f"Page {i+1} extracted text length: {len(page_text)}")
                    text += page_text + '\n'
                return self.normalize_text(text)
        except Exception as e:
            logging.error(f"Text extraction error: {str(e)}")
            return None

    def create_chunks(self, text: str) -> List[str]:
        """テキストをチャンクに分割"""
        return self.text_splitter.split_text(text)

    def get_embedding(self, text: str) -> List[float]:
        """テキストの埋め込みを生成"""
        try:
            response = self.client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
                encoding_format="float"
            )
            return response.data[0].embedding[:256]
        except Exception as e:
            logging.error(f"Embedding generation error: {str(e)}")
            return None

    def save_guideline(self, guideline_data: Dict) -> Dict:
        """ガイドラインをデータベースに保存"""
        try:
            # embedding_statusをprocessing��設定
            guideline_data['embedding_status'] = 'processing'
            result = self.supabase.table('guidelines').insert(guideline_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logging.error(f"Error saving guideline: {str(e)}")
            return None

    def save_summary_embedding(self, guideline_id: int, embedding: List[float]) -> bool:
        """サマリーの埋め込みを保存"""
        try:
            data = {
                'guideline_id': guideline_id,
                'embedding': embedding
            }
            self.supabase.table('guideline_summaries_embeddings').insert(data).execute()
            return True
        except Exception as e:
            logging.error(f"Error saving summary embedding: {str(e)}")
            return False

    def save_chunks(self, guideline_id: int, chunks: List[str]) -> bool:
        """チャンクを保存"""
        try:
            for chunk in chunks:
                embedding = self.get_embedding(chunk)
                if embedding:
                    chunk_data = {
                        'guideline_id': guideline_id,
                        'content': chunk,
                        'embedding': embedding
                    }
                    self.supabase.table('chunks').insert(chunk_data).execute()
            
            # 全てのチャンクの保存が完了したら、embedding_statusをcompletedに更新
            self.supabase.table('guidelines')\
                .update({'embedding_status': 'completed'})\
                .eq('id', guideline_id)\
                .execute()
            return True
        except Exception as e:
            # エラーが発生した場合、embedding_statusをfailedに更新
            self.supabase.table('guidelines')\
                .update({'embedding_status': 'failed'})\
                .eq('id', guideline_id)\
                .execute()
            logging.error(f"Error saving chunks: {str(e)}")
            return False