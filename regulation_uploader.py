import argparse
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import Json
import uuid
from dataclasses import dataclass
import re
from eu_reg_html_analyzer_wrapper import EURegulationAnalyzerWrapper
from structure_analyzer import StructureAnalyzer

@dataclass
class RegulationMetadata:
    name: str
    official_title: str
    short_title: str
    document_date: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegulationMetadata':
        return cls(
            name=data['name'],
            official_title=data['official_title'],
            short_title=data['short_title'],
            document_date=data['document_date']
        )
    
    def validate(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self.name:
            errors.append("name is required")
        if not self.official_title:
            errors.append("official_title is required")
        if not self.short_title:
            errors.append("short_title is required")
        if not self.document_date:
            errors.append("document_date is required")
        
        # 日付フォーマットの検証
        try:
            datetime.strptime(self.document_date, '%Y-%m-%d')
        except ValueError:
            errors.append("document_date must be in YYYY-MM-DD format")
        
        return len(errors) == 0, errors

class RegulationUploader:
    def __init__(self):
        self.preview_dir = Path("previews")
        self.preview_dir.mkdir(exist_ok=True)
        
        # Supabase（PostgreSQL）接続情報
        self.db_url = "postgres://postgres:postgres@localhost:54322/postgres"
    
    def validate_structure(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """データ構造の検証"""
        errors = []
        
        # 必須セクションの存在チェック
        required_sections = ["recitals", "chapters", "articles", "annexes"]
        for section in required_sections:
            if section not in data:
                errors.append(f"Missing required section: {section}")
        
        # 各セクションの構造チェック
        if "recitals" in data:
            for recital in data["recitals"]:
                if not all(k in recital for k in ["recital_number", "text", "metadata"]):
                    errors.append("Invalid recital structure")
        
        if "chapters" in data:
            for chapter in data["chapters"]:
                if not all(k in chapter for k in ["chapter_number", "title", "order_index"]):
                    errors.append("Invalid chapter structure")
        
        if "sections" in data:
            for section in data["sections"]:
                if not all(k in section for k in ["section_number", "title", "order_index"]):
                    errors.append("Invalid section structure")
        
        if "articles" in data:
            for article in data["articles"]:
                if not all(k in article for k in ["article_number", "title", "paragraphs"]):
                    errors.append("Invalid article structure")
                if "paragraphs" in article:
                    for para in article["paragraphs"]:
                        if not all(k in para for k in ["paragraph_number", "ordered_contents", "content_full", "metadata"]):
                            errors.append("Invalid paragraph structure")
        
        if "annexes" in data:
            for annex in data["annexes"]:
                if not all(k in annex for k in ["annex_number", "title", "content"]):
                    errors.append("Invalid annex structure")
        
        return len(errors) == 0, errors
    
    def save_preview(self, parsed_data: Dict[str, Any], metadata: Dict[str, Any]) -> Path:
        """プレビューJSONの保存"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        preview_file = self.preview_dir / f"preview_{metadata['name']}_{timestamp}.json"
        
        # プレビュー用のデータを準備
        preview_data = {
            "metadata": metadata,
            "preview": {
                # 最初の5つの前文
                "recitals_sample": parsed_data["recitals"][:5] if parsed_data.get("recitals") else [],
                
                # 最初の5つの条文
                "articles_sample": parsed_data["articles"][:5] if parsed_data.get("articles") else [],
                
                # 章立て構造の概要
                "structure_overview": {
                    "chapters": [{
                        "chapter_number": chapter["chapter_number"],
                        "title": chapter["title"],
                        "sections": [
                            {
                                "section_number": section["section_number"],
                                "title": section["title"]
                            }
                            for section in parsed_data.get("sections", [])
                            if section.get("chapter_number") == chapter["chapter_number"]
                        ],
                        "articles": [
                            {
                                "article_number": article["article_number"],
                                "title": article["title"]
                            }
                            for article in parsed_data.get("articles", [])
                            if article.get("chapter_number") == chapter["chapter_number"]
                        ]
                    } for chapter in parsed_data.get("chapters", [])],
                },
                
                # 附属書の冒頭部分
                "annexes_preview": [{
                    "annex_number": annex["annex_number"],
                    "title": annex["title"],
                    "content_preview": str(annex.get("content", ""))[:500] + "..."
                    if len(str(annex.get("content", ""))) > 500 else str(annex.get("content", ""))
                } for annex in parsed_data.get("annexes", [])]
            },
            
            # 完全なデータ（オプション）
            "full_data": parsed_data
        }
        
        # プレビューの保存
        preview_file.write_text(json.dumps(preview_data, indent=2, ensure_ascii=False))
        
        # プレビュー内容を表示
        print("\nPreview Summary:")
        print(f"\nMetadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
        
        print(f"\nRecitals: {len(parsed_data.get('recitals', []))} items (showing first 5)")
        print(f"Chapters: {len(parsed_data.get('chapters', []))} items")
        print(f"Sections: {len(parsed_data.get('sections', []))} items")
        print(f"Articles: {len(parsed_data.get('articles', []))} items")
        print(f"Annexes: {len(parsed_data.get('annexes', []))} items")
        
        print(f"\nPreview saved to: {preview_file}")
        return preview_file
    
    def upload_to_supabase(self, parsed_data: Dict[str, Any], metadata: RegulationMetadata) -> bool:
        """Supabaseへのアップロード（トランザクション管理付き）"""
        try:
            conn = psycopg2.connect(self.db_url)
            with conn:
                with conn.cursor() as cur:
                    # regulations テーブルに基本情報を挿入
                    regulation_id = str(uuid.uuid4())
                    cur.execute("""
                        INSERT INTO regulations (id, name, official_title, short_title, document_date)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                    """, (regulation_id, metadata.name, metadata.official_title, 
                          metadata.short_title, metadata.document_date))
                    
                    # recitals の挿入
                    for recital in parsed_data.get("recitals", []):
                        cur.execute("""
                            INSERT INTO recitals (id, regulation_id, recital_number, text, metadata)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (str(uuid.uuid4()), regulation_id, recital["recital_number"],
                              recital["text"], Json(recital["metadata"])))
                    
                    # chapters の挿入
                    chapter_id_map = {}
                    for chapter in parsed_data.get("chapters", []):
                        chapter_id = str(uuid.uuid4())
                        chapter_id_map[chapter["chapter_number"]] = chapter_id
                        cur.execute("""
                            INSERT INTO chapters (id, regulation_id, chapter_number, title, order_index)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (chapter_id, regulation_id, chapter["chapter_number"],
                              chapter["title"], chapter["order_index"]))
                    
                    # sections の挿入
                    section_id_map = {}
                    for section in parsed_data.get("sections", []):
                        section_id = str(uuid.uuid4())
                        section_id_map[section["section_number"]] = section_id
                        cur.execute("""
                            INSERT INTO sections (id, chapter_id, section_number, title, order_index)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (section_id, chapter_id_map.get(section.get("chapter_number")),
                              section["section_number"], section["title"], section["order_index"]))
                    
                    # articles の挿入
                    # 章と節の関係を正しく取得
                    chapter_articles_map = {}
                    for chapter in parsed_data.get("chapters", []):
                        chapter_articles = []
                        for article in chapter.get("articles", []):
                            chapter_articles.append({
                                "article_number": str(article["article_number"]),
                                "chapter_number": chapter["chapter_number"],
                                "section_number": None
                            })
                        for section in chapter.get("sections", []):
                            for article in section.get("articles", []):
                                chapter_articles.append({
                                    "article_number": str(article["article_number"]),
                                    "chapter_number": chapter["chapter_number"],
                                    "section_number": section["section_number"]
                                })
                        chapter_articles_map.update({art["article_number"]: art for art in chapter_articles})

                    # articles の挿入
                    for article in parsed_data.get("articles", []):
                        article_id = str(uuid.uuid4())
                        article_number = str(article["article_number"])
                        article_mapping = chapter_articles_map.get(article_number, {})

                        cur.execute("""
                            INSERT INTO articles (id, regulation_id, chapter_id, section_id,
                                               article_number, title, content_full, order_index, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (article_id, regulation_id,
                              chapter_id_map.get(article_mapping.get("chapter_number")),
                              section_id_map.get(article_mapping.get("section_number")),
                              article["article_number"], article["title"],
                              article.get("content_full"), article.get("order_index"),
                              Json(article.get("metadata", {}))))
                        
                        # paragraphs の挿入
                        for para in article.get("paragraphs", []):
                            para_id = str(uuid.uuid4())
                            cur.execute("""
                                INSERT INTO paragraphs (id, article_id, paragraph_number,
                                                      content_full, metadata)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (para_id, article_id, para["paragraph_number"],
                                  para["content_full"], Json(para.get("metadata", {}))))
                            
                            # paragraph_elements の挿入
                            for element in para.get("ordered_contents", []):
                                # element_id の設定を修正
                                element_id = None
                                if element["type"] in ["subparagraph", "definition"]:
                                    element_id = element.get("element_id")
                                
                                cur.execute("""
                                    INSERT INTO paragraph_elements (id, paragraph_id, type,
                                                                  element_id, content, order_index)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """, (str(uuid.uuid4()), para_id, element["type"],
                                      element_id, element["content"],
                                      element["order_index"]))
                    
                    # annexes の挿入
                    for annex in parsed_data.get("annexes", []):
                        cur.execute("""
                            INSERT INTO annexes (id, regulation_id, annex_number, title, content)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (str(uuid.uuid4()), regulation_id, annex["annex_number"],
                              annex["title"], Json(annex["content"])))
            
            return True
        
        except Exception as e:
            print(f"Error during upload: {e}")
            return False
        finally:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='EU Regulation Uploader')
    parser.add_argument('command', choices=['upload', 'preview'],
                       help='Command to execute')
    parser.add_argument('--url', required=True,
                       help='URL of the regulation')
    parser.add_argument('--metadata', required=True,
                       help='Path to metadata JSON file')
    parser.add_argument('--definition-articles', type=int, nargs='+',
                       help='定義条項の条番号をスペース区切りで指定（例: 2 4）。指定がない場合は[2, 4]を使用')
    
    args = parser.parse_args()
    
    # メタデータの読み込みと検証
    try:
        with open(args.metadata, 'r', encoding='utf-8') as f:
            metadata = RegulationMetadata.from_dict(json.load(f))
    except Exception as e:
        print(f"Error reading metadata file: {e}")
        return
    
    is_valid, errors = metadata.validate()
    if not is_valid:
        print("Metadata validation failed:")
        for error in errors:
            print(f"- {error}")
        return
    
    # HTMLの解析
    try:
        analyzer_wrapper = EURegulationAnalyzerWrapper(args.url, metadata.__dict__, args.definition_articles)
        parsed_content, soup = analyzer_wrapper.analyze()
            
        # 文書構造の解析
        structure_analyzer = StructureAnalyzer(soup)
        structure = structure_analyzer.analyze_structure()
        
        # データの統合
        parsed_data = {
            "recitals": parsed_content["recitals"],
            "chapters": structure["chapters"],
            "sections": structure["sections"],
            "articles": parsed_content["articles"],
            "annexes": parsed_content["annexes"]
        }
        
    except Exception as e:
        print(f"Error reading HTML file: {e}")
        return
    
    uploader = RegulationUploader()
    
    # データ構造の検証
    is_valid, errors = uploader.validate_structure(parsed_data)
    if not is_valid:
        print("Data structure validation failed:")
        for error in errors:
            print(f"- {error}")
        return
    
    # プレビューの保存
    preview_file = uploader.save_preview(parsed_data, metadata.__dict__)
    print(f"Preview saved to: {preview_file}")
    
    if args.command == 'upload':
        # Supabaseへのアップロード
        if uploader.upload_to_supabase(parsed_data, metadata):
            print("Upload successful")
        else:
            print("Upload failed")

if __name__ == "__main__":
    main()
