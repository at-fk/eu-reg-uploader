from typing import Dict, Any, List
from bs4 import BeautifulSoup
from eu_reg_html_analyzer import EURegulationAnalyzer

class EURegulationAnalyzerWrapper:
    def __init__(self, url: str, metadata: Dict[str, Any], definition_articles: List[int] = None):
        """
        Args:
            url: EUR-Lexの法令URL
            metadata: 法令のメタデータ（名前、施行日等）を含む辞書
            definition_articles: 定義条項の条番号のリスト。指定がない場合は[2, 4]を使用
        """
        self.analyzer = EURegulationAnalyzer(url, metadata, definition_articles)
    
    def analyze(self) -> Dict[str, Any]:
        """HTMLを解析して構造化データを返す"""
        if not self.analyzer._download_content():
            raise Exception("Failed to download content")
        
        result = {
            "recitals": [],
            "articles": [],
            "annexes": [],
            "sections": []  # セクションの配列を追加
        }
        
        # 前文の抽出
        try:
            result["recitals"] = self.analyzer._extract_recitals()
        except Exception as e:
            print(f"Warning: Error extracting recitals: {e}")
        
        # 条文の抽出
        try:
            result["articles"] = self.analyzer._extract_articles()
        except Exception as e:
            print(f"Warning: Error extracting articles: {e}")
        
        # 附属書の抽出
        try:
            result["annexes"] = self.analyzer._extract_annexes()
        except Exception as e:
            print(f"Warning: Error extracting annexes: {e}")
        
        return result, self.analyzer.soup
