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

class DMAStructureAnalyzer:
    def __init__(self):
        """初期化"""
        self.dma_url = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.soup = None
        self.regulation_data = {
            "name": "DMA",
            "official_title": "",
            "short_title": "Digital Markets Act",
            "jurisdiction_id": None,  # EUのIDを後で設定
            "document_date": "2022-09-14",  # DMAの制定日
            "effective_date": "2022-11-01",  # DMAの施行日
            "version": "1.0",
            "status": "enacted",
            "metadata": {}
        }

    def _download_content(self) -> bool:
        """HTMLコンテンツのダウンロード"""
        try:
            response = self.session.get(self.dma_url)
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
        親タイトルが"Definitions"の場合、定義として処理します。
        """
        subparagraphs = []
        current_definition = None
        current_items = []

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

            # 定義の種類を判定
            is_definition = parent_title == "Definitions"
            is_alphabetic = number.isalpha()
            is_numeric = number.isdigit()

            # 新しい定義の開始を検出（数字の場合）
            if is_numeric and is_definition:
                # 前の定義とその項目を保存
                if current_definition and current_items:
                    # 項目を定義のcontentに追加
                    full_content = current_definition["content"]
                    for item in current_items:
                        full_content += f"\n({item['id']}) {item['content']}"
                    current_definition["content"] = full_content
                    subparagraphs.append(current_definition)
                elif current_definition:
                    subparagraphs.append(current_definition)
                current_items = []

                # 新しい定義を作成
                current_definition = {
                    "subparagraph_id": number,
                    "content": content,
                    "type": "definition",
                    "order_index": len(subparagraphs) + 1
                }
            # 定義の項目を処理（アルファベットの場合）
            elif is_alphabetic and current_definition:
                current_items.append({
                    "id": number,
                    "content": content
                })
            # その他のサブパラグラフを処理
            elif not is_definition:
                subparagraphs.append({
                    "subparagraph_id": number,
                    "content": content,
                    "type": "alphabetic" if is_alphabetic else "numeric",
                    "order_index": len(subparagraphs) + 1
                })

        # 最後の定義とその項目を保存
        if current_definition and current_items:
            # 項目を定義のcontentに追加
            full_content = current_definition["content"]
            for item in current_items:
                full_content += f"\n({item['id']}) {item['content']}"
            current_definition["content"] = full_content
            subparagraphs.append(current_definition)
        elif current_definition:
            subparagraphs.append(current_definition)

        return subparagraphs

    def _parse_paragraph(self, paragraph_element):
        """
        パラグラフ要素を解析し、構造化されたデータを返します。
        HTML構造に基づいて、テーブル内の要素をサブパラグラフとして、
        テーブル外の要素をchapeauとして扱います。
        """
        print("\nParsing paragraph element...")
        ordered_contents = []
        current_order_index = 1
        processed_texts = set()  # 重複チェック用のセット
        
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
                        ordered_contents.append({
                            "type": "subparagraph",
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
            
            # Article 2（定義規定）の特別処理
            if article_number == 2:
                print("Processing Article 2 (Definitions)...")
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
                                'type': 'subparagraph',
                                'id': sp['subparagraph_id'],
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
                    current_paragraph = self._parse_paragraph(element)
                    if current_paragraph:
                        print(f"Created new paragraph with {len(current_paragraph.get('ordered_contents', []))} ordered contents")
                elif current_paragraph:
                    # 既存の段落に要素を追加
                    parsed = self._parse_paragraph(element)
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

    def _extract_articles(self, soup):
        """
        条文を抽出するメソッド。
        """
        articles = []
        article_elements = soup.find_all('div', class_='eli-subdivision', id=lambda x: x and x.startswith('art_'))
        
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

            article = {
                'article_number': article_number,
                'title': title,
                'paragraphs': paragraphs,
                'order_index': article_number,
                'metadata': {
                    'is_definitions': article_number == 2,
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
        annexes = []
        if not self.soup:
            return annexes

        try:
            # 附属書セクションを特定
            annex_elements = self.soup.find_all('p', class_='oj-doc-ti', string=re.compile(r'^ANNEX'))
            
            for element in annex_elements:
                # 基本情報の取得
                title = element.get_text(strip=True)
                sections = []
                current_section = None
                current_subsection = None
                
                # 次の要素から処理開始
                next_element = element.find_next_sibling()
                
                while next_element and not (next_element.name == 'p' and 'oj-doc-ti' in next_element.get('class', [])):
                    text = next_element.get_text(strip=True)
                    
                    # セクション（A., B., C., など）の検出
                    section_match = re.match(r'^([A-E])\.\s*[\'"]?(.*?)[\'"]?$', text)
                    if section_match:
                        current_section = {
                            "section_id": section_match.group(1),
                            "title": self._normalize_text(section_match.group(2)),
                            "subsections": [],
                            "content": ""
                        }
                        sections.append(current_section)
                    
                    # サブセクション（1., 2., など）の検出
                    elif current_section and re.match(r'^\d+\.', text):
                        current_subsection = {
                            "number": re.match(r'^(\d+)\.', text).group(1),
                            "content": self._normalize_text(text[text.find('.')+1:]),
                            "items": []
                        }
                        current_section["subsections"].append(current_subsection)
                    
                    # 項目（a., b., c., など）の検出
                    elif current_subsection and re.match(r'^[a-z]\.\s', text):
                        item = {
                            "id": text[0],
                            "content": self._normalize_text(text[2:])
                        }
                        current_subsection["items"].append(item)
                    
                    # 特別なケース：表形式データ（セクションE）
                    elif next_element.name == 'table' and current_section and current_section["section_id"] == "E":
                        definitions = []
                        for row in next_element.find_all('tr'):
                            cells = row.find_all('td')
                            if len(cells) >= 3:  # 3列以上ある場合
                                service_type = cells[0].get_text(strip=True)
                                active_end_users = cells[1].get_text(strip=True)
                                active_business_users = cells[2].get_text(strip=True)
                                if service_type and (active_end_users or active_business_users):
                                    definitions.append({
                                        "service_type": self._normalize_text(service_type),
                                        "active_end_users": self._normalize_text(active_end_users),
                                        "active_business_users": self._normalize_text(active_business_users)
                                    })
                        current_section["definitions"] = definitions
                    
                    # 通常のテキストコンテンツ
                    elif text and not text.startswith('ANNEX'):
                        if current_subsection:
                            current_subsection["content"] += " " + self._normalize_text(text)
                        elif current_section:
                            current_section["content"] += " " + self._normalize_text(text)
                    
                    next_element = next_element.find_next_sibling()
                
                # 構造化データの作成
                content = {
                    "sections": sections,
                    "metadata": {
                        "extracted_at": datetime.now().isoformat(),
                        "structure": {
                            "total_sections": len(sections),
                            "section_details": [
                                {
                                    "section_id": section["section_id"],
                                    "title": section["title"],
                                    "subsection_count": len(section["subsections"]),
                                    "has_definitions": "definitions" in section
                                }
                                for section in sections
                            ]
                        }
                    }
                }
                
                # 附属書オブジェクトの作成
                annex = {
                    "annex_number": "1",  # DMAには1つの附属書しかない
                    "title": title,
                    "content": content,  # JSONBとして保存される構造化データ
                    "metadata": {
                        "id": element.get('id', ''),
                        "extracted_at": datetime.now().isoformat()
                    }
                }
                annexes.append(annex)

                # 抽出結果の表示
                print(f"\n附属書の抽出: {len(annexes)}件")
                for annex in annexes:
                    print(f"ANNEX {annex['annex_number']}: {annex['title']}")
                    print(f"セクション数: {len(annex['content']['sections'])}")
                    for section in annex['content']['sections']:
                        print(f"- セクション {section['section_id']}: {section['title']}")
                        print(f"  サブセクション数: {len(section['subsections'])}")
                        if 'definitions' in section:
                            print(f"  定義数: {len(section['definitions'])}")

        except Exception as e:
            print(f"附属書の抽出中にエラー: {e}")
            import traceback
            traceback.print_exc()

        return annexes

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
            articles = self._extract_articles(self.soup)
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
    analyzer = DMAStructureAnalyzer()
    print("DMAの構造化データを抽出中...")
    analyzer.save_structured_data()

if __name__ == "__main__":
    main()