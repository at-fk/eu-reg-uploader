from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import re

class StructureAnalyzer:
    def __init__(self, soup: BeautifulSoup):
        self.soup = soup
        
    def analyze_structure(self) -> Dict[str, Any]:
        """Analyze the document structure including chapters, sections, and articles.
        
        Returns:
            Dict containing the hierarchical structure of the document with chapters,
            their sections, and corresponding articles.
        """
        chapters = []
        
        # Chapter level - only match exact chapter IDs (cpt_I, cpt_II, etc.)
        chapter_elements = self.soup.find_all('div', id=lambda x: x and re.match(r'^cpt_[IVXLCDM]+$', str(x)))
        chapter_order = 0
        
        for chapter_elem in chapter_elements:
            chapter_id = chapter_elem.get('id', '')
            # まずチャプター番号（CHAPTER II など）を取得
            chapter_header = chapter_elem.find('p', class_='oj-ti-section-1')
            if not chapter_header:
                continue
            
            # 次にタイトルを取得（Principles など）
            chapter_title_elem = chapter_elem.find('div', class_='eli-title')
            if not chapter_title_elem:
                continue
                
            title_text = chapter_title_elem.find('span', class_='oj-bold')
            if title_text and title_text.find('span', class_='oj-italic'):
                chapter_title = title_text.find('span', class_='oj-italic').get_text(strip=True)
            else:
                chapter_title = chapter_title_elem.get_text(strip=True)
            chapter_number = self._extract_roman_numeral(chapter_id)
            
            # Initialize chapter structure
            chapter = {
                "chapter_number": chapter_number,
                "title": chapter_title,
                "sections": [],
                "articles": [],
                "order_index": chapter_order + 1  # 1-based index
            }
            
            # Section level
            section_elements = chapter_elem.find_all('div', id=lambda x: x and '.sct_' in x)
            section_order = 0
            
            if section_elements:  # If chapter has sections
                for section_elem in section_elements:
                    section_id = section_elem.get('id', '')
                    section_title_elem = section_elem.find('div', class_='eli-title')
                    if not section_title_elem:
                        continue
                        
                    section_title = section_title_elem.get_text(strip=True)
                    section_number = str(section_order + 1)
                    
                    # Initialize section structure
                    section = {
                        "section_number": section_number,
                        "title": section_title,
                        "articles": []
                    }
                    
                    # Process articles in this section
                    articles = []
                    self._process_articles(section_elem, articles, chapter_number, section_number)
                    section["articles"] = articles
                    
                    chapter["sections"].append(section)
                    section_order += 1
            else:  # If chapter has no sections
                # Process articles directly under chapter
                articles = []
                self._process_articles(chapter_elem, articles, chapter_number, None)
                chapter["articles"] = articles
            
            chapters.append(chapter)
            chapter_order += 1
        
        # セクションを集約
        sections = []
        section_order = 0
        for chapter in chapters:
            for section in chapter.get('sections', []):
                section_order += 1
                sections.append({
                    "section_number": section["section_number"],
                    "title": section["title"],
                    "chapter_number": chapter["chapter_number"],
                    "order_index": section_order
                })
            
        return {
            "chapters": chapters,
            "sections": sections
        }
    
    def _process_articles(self, parent_elem: BeautifulSoup, articles: List[Dict[str, Any]], 
                         chapter_number: str, section_number: Optional[str]):
        """Process articles within a chapter or section.
        
        Args:
            parent_elem: BeautifulSoup element containing the articles (chapter or section)
            articles: List to append found articles to
            chapter_number: Number of the parent chapter
            section_number: Number of the parent section (None if article is directly under chapter)
        """
        # Find all article divs (class='eli-subdivision' with id starting with 'art_')
        article_elements = parent_elem.find_all('div', class_='eli-subdivision',
                                              id=lambda x: x and x.startswith('art_'))
        
        for article_elem in article_elements:
            # Extract article number from id (e.g., 'art_12' -> '12')
            article_id = article_elem.get('id', '')
            article_number = article_id.replace('art_', '')
            
            # Get article title
            title = ""
            title_elem = article_elem.find('p', class_='oj-ti-art')
            if title_elem:
                title = title_elem.get_text(strip=True)
            else:
                # Try finding title in eli-title div if not found in oj-ti-art
                title_div = article_elem.find('div', class_='eli-title')
                if title_div:
                    title_elem = title_div.find('p', class_='oj-sti-art')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
            chapter_num = str(chapter_number) if chapter_number else None

    
            # Create article object with proper chapter/section association
            article = {
                "article_number": article_number,
                "title": title,
                "chapter_number": chapter_number,
                "section_number": section_number,  # None if article is directly under chapter
                "order_index": int(article_number) if article_number.isdigit() else 0
            }
            articles.append(article)
    
    def _extract_roman_numeral(self, text: str) -> str:
        """Extract and convert Roman numeral from chapter ID to Arabic numeral.
        
        Args:
            text: Text containing a Roman numeral (expected format: 'cpt_X', 'cpt_IV', etc)
            
        Returns:
            String representation of the Arabic numeral, or empty string if no valid Roman numeral found
        """
        # Extract Roman numeral from chapter ID (e.g., 'cpt_IV' -> 'IV')
        match = re.match(r'^cpt_([IVXLCDM]+)$', text)
        if not match:
            return ""
        
        roman_numeral = match.group(1).upper()
        
        # Validate that the Roman numeral only contains valid characters
        if not all(c in 'IVXLCDM' for c in roman_numeral):
            return ""
        
        # Map for Roman numeral conversion
        roman_values = {
            'I': 1, 'V': 5, 'X': 10,
            'L': 50, 'C': 100, 'D': 500, 'M': 1000
        }
        
        # Convert to Arabic numeral
        try:
            arabic = 0
            prev_value = 0
            
            for char in reversed(roman_numeral):
                current_value = roman_values[char]
                if current_value >= prev_value:
                    arabic += current_value
                else:
                    arabic -= current_value
                prev_value = current_value
            
            # Validate the result (chapters shouldn't have extremely large numbers)
            if arabic < 1 or arabic > 100:
                return ""
                
            return str(arabic)
        except (KeyError, ValueError):
            return ""
