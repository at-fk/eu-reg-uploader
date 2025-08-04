import requests
import json
import time
from typing import Dict, Any, Optional, List
from pathlib import Path
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import traceback
import roman
import pandas as pd
from io import StringIO

class SectionBuilder:
    """Helper class for building hierarchical annex sections"""
    
    def __init__(self):
        self.sections = []
        self.current_section = None
        self.current_subsection = None
        
        # Regex patterns for different list types
        self.DASH = re.compile(r'^[—\-•]\s*(.+)')
        self.NUMBER = re.compile(r'^(\d+)[\.\)]\s+(.+)')
        self.LETTER = re.compile(r'^\(([a-z])\)\s+(.+)')
        self.ROMAN = re.compile(r'^\(([ivx]+)\)\s+(.+)', re.IGNORECASE)
        self.HIER = re.compile(r'^(\d+)\.\s+(\d+)\.\s+(.+)')
    
    def add_table_to_current_section(self, table):
        """Add a table to the current section"""
        if self.current_section is not None:
            if "tables" not in self.current_section:
                self.current_section["tables"] = []
            self.current_section["tables"].append(table)
        else:
            # If no current section, create a default one
            self.current_section = {
                "section_id": "1",
                "heading": "",
                "list_type": "ordered",
                "items": [],
                "subsections": [],
                "tables": [table]
            }
            self.sections.append(self.current_section)
    
    def _detect_list_type(self, items):
        """Detect the predominant list type from items"""
        if not items:
            return "dash"
        
        dash_count = sum(1 for item in items if self.DASH.match(item))
        if dash_count > len(items) * 0.7:
            return "dash"
        
        letter_count = sum(1 for item in items if self.LETTER.match(item))
        if letter_count > len(items) * 0.5:
            return "letter"
        
        return "ordered"
    
    def _find_existing_section(self, section_id):
        """Find existing section by ID"""
        for section in self.sections:
            if section["section_id"] == section_id:
                return section
        return None
    
    def feed_text(self, txt):
        """Process text content, handling different bullet types and hierarchy"""
        if not txt or not txt.strip():
            return
        
        txt = txt.strip()
        
        # Skip standalone bullets
        if txt in {"—", "-", "•"}:
            return
        
        # Check for Section A/B/C pattern (e.g., "Section A. List of..." or "Section A — Information...")
        section_ab_match = re.match(r'^Section\s+([A-Z])[\.\s—–\-]+(.+)', txt)
        if section_ab_match:
            section_letter = section_ab_match.group(1)
            heading = section_ab_match.group(2).strip()
            
            self.current_section = {
                "section_id": section_letter,
                "heading": heading,
                "list_type": "ordered",
                "items": [],
                "subsections": [],
                "tables": []
            }
            self.sections.append(self.current_section)
            self.current_subsection = None
            return
        
        # Check for hierarchical numbering (3. 1. content)
        hier_match = self.HIER.match(txt)
        if hier_match:
            parent_id = hier_match.group(1)
            child_id = hier_match.group(2)
            content = hier_match.group(3).strip()
            
            # Create hierarchical section ID
            section_id = f"{parent_id}.{child_id}"
            
            # Check if section exists
            existing = self._find_existing_section(section_id)
            if existing:
                existing["items"].append(content)
                self.current_section = existing
            else:
                self.current_section = {
                    "section_id": section_id,
                    "heading": content,
                    "list_type": "ordered",
                    "items": [],
                    "subsections": [],
                    "tables": []
                }
                self.sections.append(self.current_section)
            
            self.current_subsection = None
            return
        
        # Check for numbered section (1., 2., etc.)
        number_match = self.NUMBER.match(txt)
        if number_match:
            section_id = number_match.group(1)
            heading = number_match.group(2).strip()
            
            # Check if section already exists
            existing = self._find_existing_section(section_id)
            if existing:
                # Merge into existing section
                if heading and heading not in existing.get("heading", ""):
                    existing["heading"] = existing.get("heading", "") + " " + heading
                self.current_section = existing
            else:
                # Create new section
                self.current_section = {
                    "section_id": section_id,
                    "heading": heading,
                    "list_type": "ordered",  # Will be updated later
                    "items": [],
                    "subsections": [],
                    "tables": []
                }
                self.sections.append(self.current_section)
            
            self.current_subsection = None
            return
        
        # Check for lettered subsection (a), (b), etc.
        letter_match = self.LETTER.match(txt)
        if letter_match and self.current_section:
            letter_id = letter_match.group(1)
            content = letter_match.group(2).strip()
            
            # Create subsection if not exists
            if not self.current_subsection or self.current_subsection.get("subsection_id") != letter_id:
                self.current_subsection = {
                    "subsection_id": letter_id,
                    "items": []
                }
                self.current_section["subsections"].append(self.current_subsection)
            
            self.current_subsection["items"].append(content)
            return
        
        # Check for dash/bullet items
        dash_match = self.DASH.match(txt)
        if dash_match:
            content = dash_match.group(1).strip()
            if self.current_section:
                self.current_section["items"].append(content)
            return
        
        # Default: add to current section items
        if self.current_section:
            self.current_section["items"].append(txt)
        else:
            # If no current section, create a default one
            self.current_section = {
                "section_id": "1",
                "heading": "",
                "list_type": "dash",
                "items": [txt],
                "subsections": [],
                "tables": []
            }
            self.sections.append(self.current_section)
    
    def feed_list(self, li_text):
        """Process list item content"""
        self.feed_text(li_text)
    
    def flush(self):
        """Return the built sections and reset state"""
        # Update list types based on actual content
        for section in self.sections:
            if section["items"]:
                section["list_type"] = self._detect_list_type(section["items"])
        
        result = self.sections
        self.sections = []
        self.current_section = None
        self.current_subsection = None
        return result


def _merge_sections(existing_sections, new_sections):
    """Merge new sections into existing sections, avoiding duplicates"""
    existing_ids = {sec["section_id"] for sec in existing_sections}
    
    for new_sec in new_sections:
        section_id = new_sec["section_id"]
        if section_id in existing_ids:
            # Find existing section and merge content
            for existing_sec in existing_sections:
                if existing_sec["section_id"] == section_id:
                    # Merge items, avoiding duplicates
                    for item in new_sec.get("items", []):
                        if item not in existing_sec.get("items", []):
                            existing_sec["items"].append(item)
                    # Merge subsections
                    _merge_subsections(existing_sec.get("subsections", []), new_sec.get("subsections", []))
                    # Merge tables within sections
                    _merge_section_tables(existing_sec.get("tables", []), new_sec.get("tables", []))
                    break
        else:
            # Add new section
            existing_sections.append(new_sec)
            existing_ids.add(section_id)


def _merge_section_tables(existing_tables, new_tables):
    """Merge tables within sections, avoiding exact duplicates"""
    for new_table in new_tables:
        # Check if table already exists (basic duplicate detection)
        is_duplicate = False
        for existing_table in existing_tables:
            if (existing_table.get("caption") == new_table.get("caption") and
                len(existing_table.get("rows", [])) == len(new_table.get("rows", []))):
                is_duplicate = True
                break
        
        if not is_duplicate:
            existing_tables.append(new_table)


def _merge_subsections(existing_subsections, new_subsections):
    """Merge subsections, avoiding duplicates"""
    existing_ids = {sub.get("subsection_id") for sub in existing_subsections}
    
    for new_sub in new_subsections:
        sub_id = new_sub.get("subsection_id")
        if sub_id not in existing_ids:
            existing_subsections.append(new_sub)
            existing_ids.add(sub_id)



def _preprocess_orphan_numbers(content_nodes):
    """Pre-process content nodes to join orphan numbers with following content"""
    buffer = []
    i = 0
    
    while i < len(content_nodes):
        node = content_nodes[i]
        if not node or not hasattr(node, 'get_text'):
            i += 1
            continue
            
        txt = node.get_text().strip() if hasattr(node, 'get_text') else str(node).strip()
        
        # Check for orphan number (just "1." or "2." etc.)
        if re.fullmatch(r'\d+\.', txt):
            # Look for next non-empty content
            j = i + 1
            while j < len(content_nodes):
                next_node = content_nodes[j]
                if not next_node or not hasattr(next_node, 'get_text'):
                    j += 1
                    continue
                    
                next_txt = next_node.get_text().strip() if hasattr(next_node, 'get_text') else str(next_node).strip()
                if next_txt:
                    # Join orphan number with following content
                    joined_text = f"{txt} {next_txt}"
                    buffer.append(joined_text)
                    i = j + 1
                    break
                j += 1
            else:
                # No following content found, keep orphan as is
                buffer.append(txt)
                i += 1
        else:
            buffer.append(txt)
            i += 1
    
    return buffer


class EURegulationAnalyzer:
    def __init__(self, regulation_url: str, regulation_metadata: Dict[str, Any], definition_articles: List[int] = None):
        """
        初期化
        Args:
            regulation_url: EUR-Lexの法令URL
            regulation_metadata: 法令のメタデータ（名前、施行日等）を含む辞書
            definition_articles: 定義条項の条番号のリスト。指定がない場合は[2, 4]を使用
        """
        self.regulation_url = regulation_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.soup = None
        self.regulation_data = regulation_metadata
        self.definition_articles = definition_articles if definition_articles is not None else [2, 4]

    def _download_content(self) -> bool:
        """HTMLコンテンツのダウンロード"""
        try:
            response = self.session.get(self.regulation_url)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.text, 'html.parser')
            return True
        except Exception as e:
            print(f"HTMLコンテンツのダウンロード中にエラー: {e}")
            return False

    def _normalize_text(self, text: str) -> str:
        """テキストの正規化
        - 複数の空白を1つに
        - 改行を空白に
        - 前後の空白を削除
        - 特殊文字の正規化
        """
        if not text:
            return ""
        
        # 改行を空白に置換
        text = text.replace('\n', ' ')
        
        # 複数の空白を1つに
        text = ' '.join(text.split())
        
        # 特殊な空白文字を通常の空白に
        text = text.replace('\u00A0', ' ')  # NOBSPの置換
        text = text.replace('\u200B', '')   # ゼロ幅スペースの削除
        
        # 句読点の後に空白を追加（ない場合）
        text = re.sub(r'([.,;:])(?!\s)', r'\1 ', text)
        
        # かっこの前後の空白を調整
        text = re.sub(r'\s*\(\s*', ' (', text)  # 開きかっこの前に空白、後ろの空白を削除
        text = re.sub(r'\s*\)\s*', ') ', text)  # 閉じかっこの前の空白を削除、後ろに空白
        
        # 最後の整形
        text = text.strip()
        
        return text

    def _is_definition_article(self, title: str) -> bool:
        """
        記事のタイトルに基づいて定義条項かどうかを判定する
        
        Args:
            title: 記事のタイトル
            
        Returns:
            bool: 定義条項の場合True
        """
        if not title:
            return False
        return 'definition' in title.lower()

    def _extract_recitals(self) -> List[Dict[str, Any]]:
        """前文の抽出"""
        recitals = []
        if not self.soup:
            return recitals

        try:
            # 前文セクションを特定
            recital_elements = self.soup.find_all('div', class_='eli-subdivision', id=lambda x: x and x.startswith('rct_'))
            
            for element in recital_elements:
                # 前文番号を取得
                number_element = element.find('p', class_='oj-normal')
                if not number_element:
                    continue
                
                text = number_element.get_text(strip=True)
                number_match = re.match(r'\((\d+)\)', text)
                if not number_match:
                    continue
                
                recital_number = number_match.group(1)
                
                # 前文のテキストを取得
                content_elements = element.find_all('p', class_='oj-normal')
                # 最初の要素（番号を含む）から番号部分を除去
                first_text = text[len(number_match.group(0)):].strip()
                # 残りの要素のテキストを結合
                remaining_text = ' '.join(p.get_text(strip=True) for p in content_elements[1:])
                
                # 完全なテキストを構築
                full_text = first_text
                if remaining_text:
                    full_text += ' ' + remaining_text
                
                # テキストを正規化
                full_text = self._normalize_text(full_text)
                
                recitals.append({
                    "recital_number": recital_number,
                    "text": full_text,
                    "metadata": {
                        "id": element.get('id', ''),
                        "extracted_at": datetime.now().isoformat()
                    }
                })
        except Exception as e:
            print(f"前文の抽出中にエラー: {e}")
            import traceback
            traceback.print_exc()

        return sorted(recitals, key=lambda x: int(x["recital_number"]))

    def _extract_chapters(self) -> List[Dict[str, Any]]:
        """チャプターの抽出"""
        chapters = []
        if not self.soup:
            return chapters

        try:
            # GDPRのチャプター構造に対応: div[id^="cpt_"]
            chap_divs = self.soup.select('div[id^="cpt_"]')
            
            # 重複を避けるために、既に処理したチャプター番号を追跡
            processed_chapters = set()
            
            for idx, chap_div in enumerate(chap_divs, 1):
                # チャプターIDからローマ数字を取得（例: cpt_I → I）
                chap_id = chap_div.get('id', '')
                roman_match = re.search(r'cpt_([IVX]+)', chap_id)
                if not roman_match:
                    continue
                
                roman_numeral = roman_match.group(1)
                
                # ローマ数字をアラビア数字に変換
                try:
                    chapter_number = roman.fromRoman(roman_numeral)
                except roman.InvalidRomanNumeralError:
                    print(f"Invalid Roman numeral: {roman_numeral}")
                    continue
                
                # 既に処理済みのチャプターはスキップ
                if chapter_number in processed_chapters:
                    continue
                
                processed_chapters.add(chapter_number)
                
                # チャプタータイトルを取得
                title_element = chap_div.find('div', class_='eli-title')
                title = ""
                if title_element:
                    title_p = title_element.find('p', class_='oj-ti-section-2')
                    if title_p:
                        title_span = title_p.find('span', class_='oj-bold')
                        if title_span:
                            title = self._normalize_text(title_span.get_text().strip())
                
                # チャプター内の条文番号を収集
                article_numbers = []
                # チャプター内のすべての条文を検索
                article_divs = chap_div.find_all('div', id=lambda x: x and x.startswith('art_'))
                for art_div in article_divs:
                    art_id = art_div.get('id', '')
                    art_match = re.search(r'art_(\d+)', art_id)
                    if art_match:
                        article_num = int(art_match.group(1))
                        if article_num not in article_numbers:  # 重複を避ける
                            article_numbers.append(article_num)
                
                # 条文番号をソート
                article_numbers.sort()
                
                chapters.append({
                    "chapter_number": chapter_number,
                    "title": title,
                    "article_numbers": article_numbers,
                    "order_index": len(chapters) + 1
                })
                
                print(f"Chapter {chapter_number}: {title} - Articles: {article_numbers}")
                
        except Exception as e:
            print(f"チャプターの抽出中にエラー: {e}")
            import traceback
            traceback.print_exc()

        return chapters

    def _parse_subparagraphs(self, table, parent_title=None):
        """
        テーブルからサブパラグラフを抽出します。
        親タイトルが"Definitions"の場合でも、数字形式のサブパラグラフを正しく処理します。
        """
        subparagraphs = []
        current_order_index = 1

        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 2:
                continue

            # 最初のセルから番号を抽出
            number_cell = cells[0].find('p', class_='oj-normal')
            if not number_cell:
                continue
            number = number_cell.get_text().strip()
            if not number.startswith('('):
                continue
            number = number.strip('()')

            # 2番目のセルから内容を抽出
            content_cell = cells[1].find('p', class_='oj-normal')
            if not content_cell:
                continue
            content = self._normalize_text(content_cell.get_text().strip())

            # サブパラグラフの種類を判定
            is_definition = parent_title == "Definitions"
            is_alphabetic = number.isalpha()
            is_numeric = number.isdigit()

            # サブパラグラフを追加
            subparagraph_type = "definition" if is_definition else ("alphabetic" if is_alphabetic else "numeric")
            subparagraphs.append({
                "subparagraph_id": number,
                "element_id": number,
                "content": content,
                "type": subparagraph_type,
                "order_index": current_order_index
            })
            current_order_index += 1

        return subparagraphs

    def _parse_paragraph(self, paragraph_element, article_number: int=None, title: str=""):
        """
        パラグラフ要素を解析し、構造化されたデータを返します。
        HTML構造に基づいて、テーブル内の要素をサブパラグラフとして、
        テーブル外の要素をchapeauとして扱います。
        定義条項の場合は、subparagraphをdefinitionとして扱います。
        """
        print("\nParsing paragraph element...")
        ordered_contents = []
        current_order_index = 1
        processed_texts = set()  # 重複チェック用のセット
        
        is_definition = self._is_definition_article(title)

        # パラグラフ番号を探す
        paragraph_number = None
        first_p = paragraph_element.find('p', class_='oj-normal')
        if first_p:
            number_match = re.match(r'^\s*(\d+)\.\s*', first_p.get_text())
            if number_match:
                paragraph_number = number_match.group(1)
                # パラグラフ番号を除いたテキストを取得
                text = first_p.get_text()[len(number_match.group(0)):].strip()
                if text and text not in processed_texts:
                    ordered_contents.append({
                        "type": "chapeau",
                        "content": self._normalize_text(text),
                        "order_index": current_order_index
                    })
                    processed_texts.add(text)
                    current_order_index += 1
        
        # テーブル要素の処理（すべてサブパラグラフとして扱う）
        tables = paragraph_element.find_all('table')
        for table in tables:
            print(f"Processing table with {len(table.find_all('tr'))} rows")
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == 2:  # サブパラグラフの形式を確認
                    symbol = cells[0].get_text().strip()
                    content = cells[1].get_text().strip()
                    
                    # サブパラグラフIDの正規化
                    subparagraph_id = re.sub(r'[()]', '', symbol).strip()
                    
                    # 既に処理済みのテキストは除外
                    if content not in processed_texts:
                        if is_definition:
                            ordered_contents.append({
                                "type": "definition",
                                "element_id": subparagraph_id,
                                "subparagraph_id": subparagraph_id,
                                "content": self._normalize_text(content),
                                "order_index": current_order_index
                            })
                        else:
                            ordered_contents.append({
                                "type": "subparagraph",
                                "element_id": subparagraph_id,
                                "subparagraph_id": subparagraph_id,
                                "content": self._normalize_text(content),
                                "order_index": current_order_index
                            })
                        processed_texts.add(content)
                        current_order_index += 1
                        print(f"Added subparagraph {subparagraph_id} with order_index {current_order_index-1}")
        
        # テーブル外のp.oj-normal要素の処理（最初のパラグラフ以外はすべてchapeauとして扱う）
        for p in paragraph_element.find_all('p', class_='oj-normal'):
            if p != first_p:  # 最初のパラグラフは既に処理済み
                # テーブル内のp要素は除外
                if not p.find_parent('table'):
                    text = p.get_text().strip()
                    if text and text not in processed_texts:
                        ordered_contents.append({
                            "type": "chapeau",
                            "content": self._normalize_text(text),
                            "order_index": current_order_index
                        })
                        processed_texts.add(text)
                        current_order_index += 1
                        print(f"Added additional chapeau with order_index {current_order_index-1}")
        
        if not ordered_contents:
            print("No ordered contents found, returning None")
            return None
        
        # content_fullの構築
        content_parts = []
        for item in ordered_contents:
            if item["type"] == "chapeau":
                content_parts.append(item["content"])
            else:
                content_parts.append(f"({item['subparagraph_id']}) {item['content']}")
        
        content_full = "\n\n".join(content_parts)
        
        # メタデータの構築
        metadata = {
            "total_elements": len(ordered_contents),
            "chapeau_count": sum(1 for item in ordered_contents if item["type"] == "chapeau"),
            "subparagraph_count": sum(1 for item in ordered_contents if item["type"] == "subparagraph")
        }
        
        return {
            "paragraph_number": paragraph_number,
            "ordered_contents": ordered_contents,
            "content_full": content_full,
            "metadata": metadata
        }

    def _extract_paragraphs(self, article_element, article_number, title=""):
        """段落の抽出（定義規定を含む）"""
        paragraphs = []
        
        try:
            print(f"\nProcessing Article {article_number}...")
            
            # 最初の柱書きを探す
            intro_text = None
            intro_p = article_element.find('p', class_='oj-normal')
            if intro_p and not re.match(r'^\d+\.\s*', intro_p.get_text(strip=True)):
                intro_text = self._normalize_text(intro_p.get_text(strip=True))
                print(f"Found intro text: {intro_text[:100]}...")
            
            # 定義規定の特別処理
            if self._is_definition_article(title):
                print("Processing Definitions...")
                # 定義を含むテーブルを探す
                definition_tables = article_element.find_all('table', recursive=False)
                if definition_tables:
                    print(f"Found {len(definition_tables)} definition tables")
                    # 定義を1つの段落として扱う
                    ordered_contents = []
                    content_parts = []
                    current_order_index = 1
                    
                    if intro_text:
                        ordered_contents.append({
                            'type': 'chapeau',
                            'content': intro_text,
                            'order_index': current_order_index
                        })
                        content_parts.append(intro_text)
                        current_order_index += 1
                    
                    # 各定義をサブパラグラフとして処理
                    for table in definition_tables:
                        subparagraphs = self._parse_subparagraphs(table, parent_title="Definitions")
                        for sp in subparagraphs:
                            ordered_contents.append({
                                'type': 'definition',
                                'element_id': sp['subparagraph_id'],
                                'subparagraph_id': sp['subparagraph_id'],
                                'content': sp['content'],
                                'order_index': current_order_index
                            })
                            content_parts.append(f"({sp['subparagraph_id']}) {sp['content']}")
                            current_order_index += 1
                    
                    paragraph = {
                        'paragraph_number': '1',
                        'ordered_contents': ordered_contents,
                        'content_full': '\n\n'.join(content_parts),
                        'metadata': {
                            'extracted_at': datetime.now().isoformat(),
                            'is_definitions': True
                        }
                    }
                    paragraphs.append(paragraph)
                    return paragraphs
            
            # 通常の条文の処理
            print("Processing regular article...")
            paragraph_elements = article_element.find_all(['div', 'p', 'table'], recursive=False)
            print(f"Found {len(paragraph_elements)} paragraph elements")
            
            # パラグラフ番号がある要素を探す
            has_numbered_paragraphs = False
            for element in paragraph_elements:
                text = element.get_text(strip=True)
                if text and re.match(r'^\d+\.\s*', text):
                    has_numbered_paragraphs = True
                    break
            
            if not has_numbered_paragraphs:
                # パラグラフ番号がない場合は、条文全体を1つのパラグラフとして処理
                print("No numbered paragraphs found, treating entire article as single paragraph")
                full_text = ""
                for element in paragraph_elements:
                    if element.name == 'p' and 'oj-normal' in element.get('class', []):
                        text = self._normalize_text(element.get_text())
                        if text:
                            full_text += text + "\n"
                
                if full_text.strip():
                    paragraph = {
                        "paragraph_number": None,
                        "ordered_contents": [{
                            "type": "chapeau",
                            "content": full_text.strip(),
                            "order_index": 1
                        }],
                        "content_full": full_text.strip(),
                        "metadata": {
                            "total_elements": 1,
                            "chapeau_count": 1,
                            "subparagraph_count": 0
                        }
                    }
                    paragraphs.append(paragraph)
                return paragraphs
            
            # パラグラフ番号がある場合の既存の処理
            current_paragraph = None
            current_paragraph_number = None
            
            for element in paragraph_elements:
                # 段落番号を探す
                text = element.get_text(strip=True)
                if not text:
                    continue
                
                print(f"\nProcessing element: {element.name}")
                print(f"Text preview: {text[:100]}...")
                
                # 段落番号のパターン（例：1., 2., など）
                number_match = re.match(r'^(\d+)\.\s*', text)
                if number_match:
                    print(f"Found paragraph number: {number_match.group(1)}")
                    # 新しい段落の開始
                    if current_paragraph:
                        print(f"Appending paragraph {current_paragraph_number}")
                        paragraphs.append(current_paragraph)
                    current_paragraph_number = number_match.group(1)
                    current_paragraph = self._parse_paragraph(element, article_number, title)
                    if current_paragraph:
                        print(f"Created new paragraph with {len(current_paragraph.get('ordered_contents', []))} ordered contents")
                elif current_paragraph:
                    # 既存の段落に要素を追加
                    parsed = self._parse_paragraph(element, article_number, title)
                    if parsed and parsed.get('ordered_contents'):
                        print(f"Adding {len(parsed['ordered_contents'])} elements to paragraph {current_paragraph_number}")
                        current_paragraph['ordered_contents'].extend(parsed['ordered_contents'])
                        current_paragraph['content_full'] = '\n\n'.join([
                            current_paragraph['content_full'],
                            parsed['content_full']
                        ])
            
            # 最後の段落を追加
            if current_paragraph:
                print(f"Appending final paragraph {current_paragraph_number}")
                paragraphs.append(current_paragraph)
            
            print(f"Extracted {len(paragraphs)} paragraphs")
            return paragraphs

        except Exception as e:
            print(f"Error extracting paragraphs: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []

    def _extract_articles(self):  # soup 引数を削除
        """
        条文を抽出するメソッド。
        """
        if not self.soup:
            print("Error: HTML content not loaded")
            return []
        
        articles = []
        article_elements = self.soup.find_all('div', class_='eli-subdivision', id=lambda x: x and x.startswith('art_'))
    
    
        for article_element in article_elements:
            # 条文番号を取得
            article_number_element = article_element.find('p', class_='oj-ti-art')
            if not article_number_element:
                continue
            
            # テキストを正規化して条文番号を抽出
            article_text = self._normalize_text(article_number_element.get_text())
            article_number = int(article_text.replace('Article', '').strip())

            # タイトルを取得
            title = ""
            subtitle_element = article_element.find('p', class_='oj-sti-art')
            if subtitle_element:
                title = self._normalize_text(subtitle_element.get_text())

            # 段落を抽出（定義条項は自動検出）
            paragraphs = self._extract_paragraphs(article_element, article_number, title)

            # content_full を構築
            paragraphs_content = [
                para.get("content_full", "")
                for para in paragraphs
            ]
            content_full = "\n\n".join(filter(None, paragraphs_content))

            article = {
                'article_number': article_number,
                'title': title,
                'paragraphs': paragraphs,
                'content_full': content_full,
                'order_index': article_number,
                'metadata': {
                    'is_definitions': self._is_definition_article(title),
                    'extracted_at': datetime.now().isoformat()
                }
            }
            articles.append(article)
            print(f"条文 {article_number} を処理中:")
            print(f"タイトル: {title}")
            if paragraphs:
                print(f"-> 条文を追加しました（段落数: {len(paragraphs)}）")

        return sorted(articles, key=lambda x: x['article_number'])

    def _untruncate_hidden_text(self):
        """Un-truncate hidden text by replacing display:none spans with their text content"""
        if not self.soup:
            return
        
        for span in self.soup.select('[style*="display:none"]'):
            span.replace_with(span.get_text())
    
    def _parse_annex_tables(self, container) -> List[Dict[str, Any]]:
        """Parse tables within an annex container"""
        tables = []
        
        for table in container.find_all('table'):
            try:
                # Try to find table caption
                caption = ""
                caption_element = table.find_previous('p', class_='oj-ti-table')
                if caption_element:
                    caption = self._normalize_text(caption_element.get_text())
                
                # Use pandas to parse the table
                df = pd.read_html(StringIO(str(table)))[0]
                
                # Clean up NaN values - replace with empty strings
                df = df.fillna("")
                
                # Convert to dict records
                rows = df.to_dict("records")
                
                tables.append({
                    "caption": caption,
                    "rows": rows
                })
                
            except Exception as e:
                print(f"Error parsing table: {e}")
                # Fallback: manual table parsing
                rows = []
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        row_data = {}
                        for i, cell in enumerate(cells):
                            row_data[f"col_{i}"] = self._normalize_text(cell.get_text())
                        rows.append(row_data)
                
                if rows:
                    tables.append({
                        "caption": caption,
                        "rows": rows
                    })
        
        return tables
    
    def _validate_annex(self, annex: Dict[str, Any], original_html: str) -> None:
        """Validate annex content according to requirements"""
        # Rule 1: Each annex must have sections with at least one item of text
        if not annex.get("sections"):
            raise ValueError(f"Annex {annex.get('annex_id', 'unknown')} has no sections")
        
        has_content = False
        for section in annex["sections"]:
            if section.get("items") and any(item.strip() for item in section["items"]):
                has_content = True
                break
        
        if not has_content:
            raise ValueError(f"Annex {annex.get('annex_id', 'unknown')} has no content in sections")
        
        # Rule 2: No items may equal "—" or be empty after strip()
        for section in annex["sections"]:
            for item in section.get("items", []):
                if not item.strip() or item.strip() == "—":
                    raise ValueError(f"Annex {annex.get('annex_id', 'unknown')} contains empty or dash-only items")
        
        # Rule 3: Word-count ratio between annex HTML and extracted text ≥ 0.90
        html_words = len(original_html.split())
        extracted_words = 0
        
        # Count words in title
        extracted_words += len(annex.get("title", "").split())
        
        # Count words in sections
        for section in annex["sections"]:
            extracted_words += len(section.get("heading", "").split())
            for item in section.get("items", []):
                extracted_words += len(item.split())
            for subsection in section.get("subsections", []):
                for sub_item in subsection.get("items", []):
                    extracted_words += len(sub_item.split())
        
        # Count words in tables within sections
        for section in annex["sections"]:
            for table in section.get("tables", []):
                extracted_words += len(table.get("caption", "").split())
                for row in table.get("rows", []):
                    for value in row.values():
                        if isinstance(value, str):
                            extracted_words += len(value.split())
        
        if html_words > 0:
            ratio = extracted_words / html_words
            if ratio < 0.90:
                print(f"Warning: Annex {annex.get('annex_id', 'unknown')} word ratio {ratio:.2f} < 0.90")

    def _validate_annex_uniqueness(self, annexes: List[Dict[str, Any]]) -> None:
        """Validate that annex_ids and section_ids within each annex are unique"""
        # Check annex_id uniqueness
        seen_annex_ids = set()
        for annex in annexes:
            annex_id = annex.get("annex_id")
            if annex_id in seen_annex_ids:
                raise ValueError(f"Duplicate annex_id: {annex_id}")
            seen_annex_ids.add(annex_id)
            
            # Check section_id uniqueness within each annex
            seen_section_ids = set()
            for section in annex.get("sections", []):
                section_id = section.get("section_id")
                if section_id in seen_section_ids:
                    raise ValueError(f"Duplicate section_id '{section_id}' in Annex {annex_id}")
                seen_section_ids.add(section_id)

    def _extract_annexes(self) -> List[Dict[str, Any]]:
        """Extract annexes with comprehensive section and table parsing and deduplication"""
        if not self.soup:
            return []

        try:
            # Step 1: Un-truncate hidden text
            self._untruncate_hidden_text()
            
            # Step 2: Find annex headers
            header_q = 'p.oj-doc-ti-annex, p.oj-ti-annex, p.oj-doc-ti'
            headers = []
            
            for header in self.soup.select(header_q):
                text = header.get_text().strip()
                if 'ANNEX' in text.upper():
                    headers.append(header)
            
            if not headers:
                print("No annex headers found")
                return []
            
            # Step 3: Process each annex with deduplication
            annex_map: Dict[str, Dict[str, Any]] = {}
            
            for order_index, header in enumerate(headers, 1):
                try:
                    # Extract annex ID from header text
                    header_text = header.get_text().strip()
                    # Updated regex to handle standalone "ANNEX" or "ANNEX I", "ANNEX A", etc.
                    annex_match = re.search(r'ANNEX(?:\s+([IVXLC]+|[A-Z]))?', header_text)
                    if annex_match and annex_match.group(1):
                        annex_id = annex_match.group(1)
                        # Set title to just "ANNEX X" part, not the descriptive subtitle
                        title = f"ANNEX {annex_id}"
                        # Extract descriptive subtitle (everything after "ANNEX X")
                        subtitle_match = re.search(rf'ANNEX\s+{re.escape(annex_id)}\s*(.+)', header_text)
                        subtitle = subtitle_match.group(1).strip() if subtitle_match else ""
                    else:
                        # Special case: Long descriptive headers that belong to specific annexes
                        # Normalize spaces for comparison (handle non-breaking spaces)
                        normalized_text = re.sub(r'\s+', ' ', header_text)
                        if 'testing in real world conditions' in normalized_text.lower() and 'article 60' in normalized_text.lower():
                            # This is the ANNEX IX content header
                            annex_id = 'IX'
                            title = f"ANNEX {annex_id}"
                            subtitle = header_text
                            print(f"Detected ANNEX IX content header: {header_text[:100]}...")
                        else:
                            # Default to roman numeral based on order for standalone "ANNEX"
                            roman_numerals = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV']
                            annex_id = roman_numerals[order_index - 1] if order_index <= len(roman_numerals) else str(order_index)
                            title = f"ANNEX {annex_id}"
                            # Extract descriptive subtitle (everything after "ANNEX")
                            subtitle_match = re.search(r'ANNEX\s*(.+)', header_text)
                            subtitle = subtitle_match.group(1).strip() if subtitle_match else ""
                            print(f"Using default annex ID '{annex_id}' for standalone ANNEX: {header_text}")
                    
                    # Step 4: Collect sibling nodes until next header or end
                    current = header.next_sibling
                    content_nodes = []
                    
                    while current:
                        if current.name and current.name in ['p', 'div', 'table', 'ul', 'ol']:
                            # Check if this is another annex header
                            if (current.name == 'p' and 
                                any(cls in current.get('class', []) for cls in ['oj-doc-ti-annex', 'oj-ti-annex', 'oj-doc-ti']) and
                                'ANNEX' in current.get_text().upper()):
                                break
                            content_nodes.append(current)
                        current = current.next_sibling
                    
                    # Step 5: Filter out subtitle nodes that should not become section content
                    # Check if first few nodes are subtitle elements
                    nodes_to_remove = 0
                    for i, node in enumerate(content_nodes[:3]):  # Check first 3 nodes
                        if node.name == 'p':
                            node_text = self._normalize_text(node.get_text())
                            node_classes = node.get('class', [])
                            
                            # Skip subtitle elements or descriptive text that looks like a subtitle
                            if (any('oj-sti' in cls for cls in node_classes) or
                                # Check if this looks like a descriptive subtitle for annexes
                                (i == 0 and len(node_text) > 10 and len(node_text) < 200 and 
                                 not any(pattern.match(node_text) for pattern in [
                                     re.compile(r'^[—\-•]\s*(.+)'),
                                     re.compile(r'^(\d+)[\.\)]\s+(.+)'),
                                     re.compile(r'^\(([a-z])\)\s+(.+)'),
                                     re.compile(r'^\(([ivx]+)\)\s+(.+)', re.IGNORECASE),
                                     re.compile(r'^(\d+)\.\s+(\d+)\.\s+(.+)')
                                 ]) and
                                 not node_text.strip().endswith(':'))):
                                nodes_to_remove = i + 1
                                if not subtitle and i == 0:  # Use first removed node as subtitle
                                    subtitle = node_text
                                print(f"Marking node {i} for removal (subtitle): '{node_text[:50]}...'")
                            else:
                                break  # Stop at first non-subtitle node
                    
                    if nodes_to_remove > 0:
                        content_nodes = content_nodes[nodes_to_remove:]
                        print(f"Removed {nodes_to_remove} subtitle nodes for annex {annex_id}")
                    
                    # Step 5: Pre-process orphan numbers
                    processed_texts = _preprocess_orphan_numbers(content_nodes)
                    
                    # Step 6: Process content nodes with integrated table handling
                    builder = SectionBuilder()
                    
                    for i, node in enumerate(content_nodes):
                        if node.name == 'p':
                            text = processed_texts[i] if i < len(processed_texts) else self._normalize_text(node.get_text())
                            if text:
                                # Check if this is a DMA-style section header (oj-ti-grseq-1)
                                if 'oj-ti-grseq-1' in node.get('class', []):
                                    # This is a section header like "A. 'General'"
                                    # Extract the letter and title
                                    match = re.match(r'([A-Z])\.\s*[\'"]?(.+?)[\'"]?$', text)
                                    if match:
                                        section_id = match.group(1)
                                        heading = match.group(2).strip("'\"")
                                        # Create a numbered section (treat letter as number for consistency)
                                        builder.feed_text(f"{ord(section_id) - ord('A') + 1}. {heading}")
                                    else:
                                        builder.feed_text(text)
                                else:
                                    builder.feed_text(text)
                        
                        elif node.name in ['ul', 'ol']:
                            for li in node.find_all('li'):
                                li_text = self._normalize_text(li.get_text())
                                if li_text:
                                    builder.feed_list(li_text)
                        
                        elif node.name == 'table':
                            # Parse table as structured data and add to current section
                            try:
                                caption = ""
                                caption_element = node.find_previous('p', class_='oj-ti-table')
                                if caption_element:
                                    caption = self._normalize_text(caption_element.get_text())
                                
                                # Use pandas to parse the table
                                df = pd.read_html(StringIO(str(node)))[0]
                                
                                # Clean up NaN values - replace with empty strings
                                df = df.fillna("")
                                
                                rows = df.to_dict("records")
                                
                                table_data = {
                                    "caption": caption,
                                    "rows": rows
                                }
                                
                                # Add table to current section instead of processing as text
                                builder.add_table_to_current_section(table_data)
                                
                            except Exception as e:
                                print(f"Error parsing table in annex {annex_id}: {e}")
                                # Fallback: process as text if table parsing fails
                                for row in node.find_all('tr'):
                                    cells = row.find_all(['td', 'th'])
                                    if len(cells) >= 2:
                                        first_cell = self._normalize_text(cells[0].get_text())
                                        if first_cell and re.match(r'^\d+\.?$', first_cell.strip()):
                                            content = ' '.join(self._normalize_text(cell.get_text()) for cell in cells[1:])
                                            if content:
                                                builder.feed_text(f"{first_cell} {content}")
                        
                        elif node.name == 'div':
                            # Process paragraphs within div
                            for p in node.find_all('p'):
                                text = self._normalize_text(p.get_text())
                                if text:
                                    # Check for section headers in divs too
                                    if 'oj-ti-grseq-1' in p.get('class', []):
                                        match = re.match(r'([A-Z])\.\s*[\'"]?(.+?)[\'"]?$', text)
                                        if match:
                                            section_id = match.group(1)
                                            heading = match.group(2).strip("'\"")
                                            builder.feed_text(f"{ord(section_id) - ord('A') + 1}. {heading}")
                                        else:
                                            builder.feed_text(text)
                                    else:
                                        builder.feed_text(text)
                    
                    sections = builder.flush()
                    
                    # Step 7: Check for existing annex and merge or create new (no separate tables array)
                    if annex_id in annex_map:
                        # Merge into existing annex
                        existing = annex_map[annex_id]
                        _merge_sections(existing["sections"], sections)
                        print(f"Merged content into existing Annex {annex_id}")
                    else:
                        # Create new annex (without top-level tables array)
                        annex = {
                            "annex_id": annex_id,
                            "title": title,
                            "subtitle": subtitle if 'subtitle' in locals() else "",
                            "sections": sections,
                            "order_index": order_index
                        }
                        
                        # Validate annex
                        original_html = ''.join(str(node) for node in content_nodes)
                        if sections:  # Only validate if we have sections
                            try:
                                self._validate_annex(annex, original_html)
                            except ValueError as e:
                                print(f"Validation error for annex {annex_id}: {e}")
                        
                        annex_map[annex_id] = annex
                        
                        # Count tables in sections for logging
                        total_tables = sum(len(section.get("tables", [])) for section in sections)
                        print(f"Created Annex {annex_id}: {len(sections)} sections, {total_tables} tables")
                    
                except Exception as e:
                    print(f"Error processing annex: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Step 9: Convert map to sorted list and validate uniqueness
            annexes = list(annex_map.values())
            annexes.sort(key=lambda a: a["order_index"])
            
            # Step 10: Final validation
            self._validate_annex_uniqueness(annexes)
            
            return annexes
                    
        except Exception as e:
            print(f"Error in annex extraction: {e}")
            import traceback
            traceback.print_exc()
            return []
        

    def save_structured_data(self):
        """構造化データを保存"""
        if not self._download_content():
            print("HTMLデータのダウンロードに失敗しました。")
            return

        try:
            # 前文を抽出
            recitals = self._extract_recitals()
            print(f"\n前文数: {len(recitals)}")

            # チャプターを抽出
            chapters = self._extract_chapters()
            print(f"チャプター数: {len(chapters)}")

            # 条文を抽出
            articles = self._extract_articles()
            print(f"条文数: {len(articles)}")

            # 附属書を抽出
            annexes = self._extract_annexes()
            print(f"附属書数: {len(annexes)}")

            # データを保存（動的な値を使用）
            data = {
                'metadata': {
                    'title': self.regulation_data.get('name', 'Unknown Regulation'),
                    'extracted_at': datetime.now().isoformat()
                },
                'recitals': recitals,
                'chapters': chapters,
                'articles': articles,
                'annexes': annexes
            }

            # 保存先ディレクトリを作成（動的に）
            regulation_name = self.regulation_data.get('name', 'unknown').lower()
            output_dir = f'{regulation_name}_data'
            os.makedirs(output_dir, exist_ok=True)

            # JSONファイルに保存（動的に）
            output_path = os.path.join(output_dir, f'{regulation_name}_structured.json')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"データを {output_dir} に保存しました。")
            print(f"- 前文数: {len(recitals)}")
            print(f"- チャプター数: {len(chapters)}")
            print(f"- 条文数: {len(articles)}")
            print(f"- 附属書数: {len(annexes)}")

        except Exception as e:
            print(f"データの保存中にエラー: {e}")
            traceback.print_exc()

def main():
    import argparse

    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description='法令のHTMLから構造化データを抽出します。')
    parser.add_argument('--url', type=str, required=True,
                        help='EUR-Lexの法令URL')
    parser.add_argument('--name', type=str, required=True,
                        help='法令の短縮名（例: GDPR, DMAなど）')
    parser.add_argument('--definition-articles', type=int, nargs='+',
                        help='定義条項の条番号をスペース区切りで指定（例: 2 4）。指定がない場合は[2, 4]を使用')

    args = parser.parse_args()

    # メタデータの設定
    metadata = {
        "name": args.name,
        "official_title": args.name,
        "short_title": args.name,
        "jurisdiction_id": "EU",
        "document_date": datetime.now().strftime("%Y-%m-%d"),
        "version": "1.0",
        "status": "enacted",
        "metadata": {}
    }
    
    # アナライザーの初期化と実行
    analyzer = EURegulationAnalyzer(args.url, metadata, args.definition_articles)
    print(f"{metadata['name']}の構造化データを抽出中...")
    print(f"定義条項: {analyzer.definition_articles}")
    analyzer.save_structured_data()

if __name__ == "__main__":
    main()