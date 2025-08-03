import pytest
import sys
import os
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from eu_reg_html_analyzer import EURegulationAnalyzer

def run_analyzer():
    """GDPRのURLからアナライザーを実行して結果を返す"""
    # テスト用のメタデータ
    metadata = {
        "name": "GDPR",
        "official_title": "General Data Protection Regulation",
        "short_title": "GDPR",
        "jurisdiction_id": "EU",
        "document_date": "2016-04-27",
        "version": "1.0",
        "status": "enacted",
        "metadata": {}
    }
    
    # アナライザーを初期化
    analyzer = EURegulationAnalyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32016R0679", metadata)
    
    # HTMLコンテンツをダウンロード
    if not analyzer._download_content():
        pytest.skip("Failed to download HTML content")
    
    # チャプターを抽出
    chapters = analyzer._extract_chapters()
    
    return {
        "chapters": chapters
    }

def test_gdpr_has_11_chapters():
    """GDPRに11個のチャプターがあることを確認"""
    doc = run_analyzer()
    assert len(doc["chapters"]) == 11, f"Expected 11 chapters, got {len(doc['chapters'])}"

def test_chapter_structure():
    """チャプターの構造が正しいことを確認"""
    doc = run_analyzer()
    
    for chapter in doc["chapters"]:
        # 必要なフィールドが存在することを確認
        assert "chapter_number" in chapter
        assert "title" in chapter
        assert "article_numbers" in chapter
        assert "order_index" in chapter
        
        # データ型の確認
        assert isinstance(chapter["chapter_number"], int)
        assert isinstance(chapter["title"], str)
        assert isinstance(chapter["article_numbers"], list)
        assert isinstance(chapter["order_index"], int)
        
        # チャプター番号が1-11の範囲内であることを確認
        assert 1 <= chapter["chapter_number"] <= 11
        
        # 条文番号が正の整数であることを確認
        for art_num in chapter["article_numbers"]:
            assert isinstance(art_num, int)
            assert art_num > 0

def test_chapter_1_is_general_provisions():
    """チャプター1が「General provisions」であることを確認"""
    doc = run_analyzer()
    
    chapter_1 = None
    for chapter in doc["chapters"]:
        if chapter["chapter_number"] == 1:
            chapter_1 = chapter
            break
    
    assert chapter_1 is not None, "Chapter 1 not found"
    assert "general" in chapter_1["title"].lower(), f"Expected 'general' in title, got: {chapter_1['title']}"

def test_article_numbers_are_sequential():
    """条文番号が1-99の範囲内で連続していることを確認"""
    doc = run_analyzer()
    
    all_article_numbers = []
    for chapter in doc["chapters"]:
        all_article_numbers.extend(chapter["article_numbers"])
    
    # 重複を除去してソート
    all_article_numbers = sorted(set(all_article_numbers))
    
    # 1-99の範囲内であることを確認
    assert all(1 <= num <= 99 for num in all_article_numbers), f"Article numbers out of range: {all_article_numbers}"
    
    # 連続していることを確認（ギャップがない）
    expected_range = list(range(1, 100))  # 1-99
    missing_numbers = set(expected_range) - set(all_article_numbers)
    assert len(missing_numbers) == 0, f"Missing article numbers: {missing_numbers}"

def test_chapter_11_is_final_provisions():
    """チャプター11が「Final provisions」であることを確認"""
    doc = run_analyzer()
    
    chapter_11 = None
    for chapter in doc["chapters"]:
        if chapter["chapter_number"] == 11:
            chapter_11 = chapter
            break
    
    assert chapter_11 is not None, "Chapter 11 not found"
    assert "final" in chapter_11["title"].lower(), f"Expected 'final' in title, got: {chapter_11['title']}" 