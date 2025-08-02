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
            # チャプターセクションを特定
            chapter_elements = self.soup.find_all('p', class_='oj-ti-section-1', string=re.compile(r'^CHAPTER \w+'))
            
            for idx, element in enumerate(chapter_elements, 1):
                title = element.get_text().strip()
                chapter_number = re.search(r'CHAPTER (\w+)', title)
                if chapter_number:
                    # チャプタータイトルを取得
                    title_element = element.find_next_sibling('div', class_='eli-title')
                    subtitle = ""
                    if title_element:
                        subtitle_element = title_element.find('span', class_='oj-bold')
                        if subtitle_element:
                            subtitle = subtitle_element.get_text().strip()
                    
                    # チャプター番号とタイトルを分離
                    chapter_num = chapter_number.group(1)
                    
                    chapters.append({
                        "chapter_number": chapter_num,
                        "title": subtitle,  # サブタイトルのみを使用
                        "order_index": idx
                    })
        except Exception as e:
            print(f"チャプターの抽出中にエラー: {e}")

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

    def _parse_paragraph(self, paragraph_element, article_number: int=None):
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
        
        is_definition = article_number in self.definition_articles if article_number else False

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

    def _extract_paragraphs(self, article_element, article_number):
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
            if article_number in self.definition_articles:
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
                    current_paragraph = self._parse_paragraph(element, article_number)
                    if current_paragraph:
                        print(f"Created new paragraph with {len(current_paragraph.get('ordered_contents', []))} ordered contents")
                elif current_paragraph:
                    # 既存の段落に要素を追加
                    parsed = self._parse_paragraph(element, article_number)
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

            # 段落を抽出（Article 2も同じ処理で行う）
            paragraphs = self._extract_paragraphs(article_element, article_number)

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
                    'is_definitions': article_number in self.definition_articles,
                    'extracted_at': datetime.now().isoformat()
                }
            }
            articles.append(article)
            print(f"条文 {article_number} を処理中:")
            print(f"タイトル: {title}")
            if paragraphs:
                print(f"-> 条文を追加しました（段落数: {len(paragraphs)}）")

        return sorted(articles, key=lambda x: x['article_number'])

    def _extract_annexes(self) -> List[Dict[str, Any]]:
        """附属書の抽出"""
        return []

        # try:
        #     # 附属書セクションを特定
        #     annex_elements = self.soup.find_all('p', class_='oj-doc-ti', string=re.compile(r'^ANNEX'))
            
        #     for element in annex_elements:
        #         # 基本情報の取得
        #         title = element.get_text(strip=True)
        #         sections = []
        #         current_section = None
        #         current_subsection = None
                
        #         # 次の要素から処理開始
        #         next_element = element.find_next_sibling()
                
        #         while next_element and not (next_element.name == 'p' and 'oj-doc-ti' in next_element.get('class', [])):
        #             text = next_element.get_text(strip=True)
                    
        #             # セクション（A., B., C., など）の検出
        #             section_match = re.match(r'^([A-E])\.\s*[\'"]?(.*?)[\'"]?$', text)
        #             if section_match:
        #                 current_section = {
        #                     "section_id": section_match.group(1),
        #                     "title": self._normalize_text(section_match.group(2)),
        #                     "subsections": [],
        #                     "content": ""
        #                 }
        #                 sections.append(current_section)
                    
        #             # サブセクション（1., 2., など）の検出
        #             elif current_section and re.match(r'^\d+\.', text):
        #                 current_subsection = {
        #                     "number": re.match(r'^(\d+)\.', text).group(1),
        #                     "content": self._normalize_text(text[text.find('.')+1:]),
        #                     "items": []
        #                 }
        #                 current_section["subsections"].append(current_subsection)
                    
        #             # 項目（a., b., c., など）の検出
        #             elif current_subsection and re.match(r'^[a-z]\.\s', text):
        #                 item = {
        #                     "id": text[0],
        #                     "content": self._normalize_text(text[2:])
        #                 }
        #                 current_subsection["items"].append(item)
                    
        #             # 特別なケース：表形式データ（セクションE）
        #             elif next_element.name == 'table' and current_section and current_section["section_id"] == "E":
        #                 definitions = []
        #                 for row in next_element.find_all('tr'):
        #                     cells = row.find_all('td')
        #                     if len(cells) >= 3:  # 3列以上ある場合
        #                         service_type = cells[0].get_text(strip=True)
        #                         active_end_users = cells[1].get_text(strip=True)
        #                         active_business_users = cells[2].get_text(strip=True)
        #                         if service_type and (active_end_users or active_business_users):
        #                             definitions.append({
        #                                 "service_type": self._normalize_text(service_type),
        #                                 "active_end_users": self._normalize_text(active_end_users),
        #                                 "active_business_users": self._normalize_text(active_business_users)
        #                             })
        #                 current_section["definitions"] = definitions
                    
        #             # 通常のテキストコンテンツ
        #             elif text and not text.startswith('ANNEX'):
        #                 if current_subsection:
        #                     current_subsection["content"] += " " + self._normalize_text(text)
        #                 elif current_section:
        #                     current_section["content"] += " " + self._normalize_text(text)
                    
        #             next_element = next_element.find_next_sibling()
                
        #         # 構造化データの作成
        #         content = {
        #             "sections": sections,
        #             "metadata": {
        #                 "extracted_at": datetime.now().isoformat(),
        #                 "structure": {
        #                     "total_sections": len(sections),
        #                     "section_details": [
        #                         {
        #                             "section_id": section["section_id"],
        #                             "title": section["title"],
        #                             "subsection_count": len(section["subsections"]),
        #                             "has_definitions": "definitions" in section
        #                         }
        #                         for section in sections
        #                     ]
        #                 }
        #             }
        #         }
                
        #         # 附属書オブジェクトの作成
        #         annex = {
        #             "annex_number": "1",  # DMAには1つの附属書しかない
        #             "title": title,
        #             "content": content,  # JSONBとして保存される構造化データ
        #             "metadata": {
        #                 "id": element.get('id', ''),
        #                 "extracted_at": datetime.now().isoformat()
        #             }
        #         }
        #         annexes.append(annex)

        #         # 抽出結果の表示
        #         print(f"\n附属書の抽出: {len(annexes)}件")
        #         for annex in annexes:
        #             print(f"ANNEX {annex['annex_number']}: {annex['title']}")
        #             print(f"セクション数: {len(annex['content']['sections'])}")
        #             for section in annex['content']['sections']:
        #                 print(f"- セクション {section['section_id']}: {section['title']}")
        #                 print(f"  サブセクション数: {len(section['subsections'])}")
        #                 if 'definitions' in section:
        #                     print(f"  定義数: {len(section['definitions'])}")

        # except Exception as e:
        #     print(f"附属書の抽出中にエラー: {e}")
        #     import traceback
        #     traceback.print_exc()

        # return annexes
        

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

            # データを保存
            data = {
                'metadata': {
                    'title': 'Digital Markets Act',
                    'extracted_at': datetime.now().isoformat()
                },
                'recitals': recitals,
                'chapters': chapters,
                'articles': articles,
                'annexes': annexes
            }

            # 保存先ディレクトリを作成
            os.makedirs('dma_data', exist_ok=True)

            # JSONファイルに保存
            output_path = os.path.join('dma_data', 'dma_structured.json')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"データを dma_data に保存しました。")
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