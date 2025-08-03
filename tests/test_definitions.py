"""Tests for definition parsing functionality."""

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
    
    # 各コンポーネントを抽出
    recitals = analyzer._extract_recitals()
    chapters = analyzer._extract_chapters()
    articles = analyzer._extract_articles()
    annexes = analyzer._extract_annexes()
    
    # データ構造を返す
    return {
        'metadata': {
            'title': metadata.get('name', 'Unknown Regulation'),
        },
        'recitals': recitals,
        'chapters': chapters,
        'articles': articles,
        'annexes': annexes
    }


def test_definition_numbers():
    """Test that Article 4 definitions have proper subparagraph_id values and at least 26 main definitions."""
    doc = run_analyzer()
    defs = [p for a in doc["articles"] if a["article_number"] == 4
            for p in a["paragraphs"][0]["ordered_contents"] if p["type"] == "definition"]
    ids = [d["subparagraph_id"] for d in defs]
    
    # Check that all definitions have subparagraph_id (not None)
    assert all(id is not None for id in ids), "All definitions should have subparagraph_id"
    
    # Check that we have the main numbered definitions 1-26
    main_numbered_ids = [id for id in ids if id.isdigit()]
    expected_main_ids = [str(i) for i in range(1, 27)]
    assert set(main_numbered_ids) >= set(expected_main_ids), f"Missing main definition IDs. Expected: {expected_main_ids}, Got: {sorted(set(main_numbered_ids))}"


def test_automatic_definition_detection():
    """Test that definition articles are automatically detected based on title."""
    doc = run_analyzer()
    
    # Check that Article 4 is detected as a definition article
    article_4 = [a for a in doc["articles"] if a["article_number"] == 4][0]
    assert article_4["metadata"]["is_definitions"] == True, "Article 4 should be detected as a definition article"
    assert "definition" in article_4["title"].lower(), f"Article 4 title should contain 'definition': {article_4['title']}"
    
    # Check that other articles are not marked as definition articles
    non_definition_articles = [a for a in doc["articles"] if a["article_number"] in [1, 2, 3, 5] and not a["metadata"]["is_definitions"]]
    assert len(non_definition_articles) > 0, "Some non-definition articles should not be marked as definitions"