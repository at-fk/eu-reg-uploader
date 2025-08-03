"""Tests for annex extraction functionality."""

import pytest
import sys
import os
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from eu_reg_html_analyzer import EURegulationAnalyzer


def run_gdpr_analyzer():
    """GDPRのURLからアナライザーを実行して結果を返す"""
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
    
    analyzer = EURegulationAnalyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32016R0679", metadata)
    
    if not analyzer._download_content():
        pytest.skip("Failed to download GDPR HTML content")
    
    annexes = analyzer._extract_annexes()
    
    return {
        "annexes": annexes
    }


def run_dma_analyzer():
    """DMAのURLからアナライザーを実行して結果を返す"""
    metadata = {
        "name": "DMA",
        "official_title": "Digital Markets Act",
        "short_title": "DMA", 
        "jurisdiction_id": "EU",
        "document_date": "2022-03-25",
        "version": "1.0",
        "status": "enacted",
        "metadata": {}
    }
    
    analyzer = EURegulationAnalyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925", metadata)
    
    if not analyzer._download_content():
        pytest.skip("Failed to download DMA HTML content")
    
    annexes = analyzer._extract_annexes()
    
    return {
        "annexes": annexes
    }


def test_gdpr_has_no_annexes():
    """GDPRには附属書がないことを確認"""
    doc = run_gdpr_analyzer()
    assert len(doc["annexes"]) == 0, f"GDPR should have no annexes, got {len(doc['annexes'])}"


def test_dma_has_annexes():
    """DMAには少なくとも1つの附属書があることを確認"""
    doc = run_dma_analyzer()
    assert len(doc["annexes"]) > 0, f"DMA should have at least one annex, got {len(doc['annexes'])}"


def test_dma_annex_structure():
    """DMAの附属書の構造が正しいことを確認"""
    doc = run_dma_analyzer()
    
    for annex in doc["annexes"]:
        # 必要なフィールドが存在することを確認
        assert "annex_id" in annex
        assert "title" in annex
        assert "sections" in annex
        assert "order_index" in annex
        assert "metadata" in annex
        
        # データ型の確認
        assert isinstance(annex["annex_id"], str)
        assert isinstance(annex["title"], str)
        assert isinstance(annex["sections"], list)
        assert isinstance(annex["order_index"], int)
        assert isinstance(annex["metadata"], dict)
        
        # メタデータの確認
        assert "id" in annex["metadata"]
        assert "extracted_at" in annex["metadata"]
        
        # 少なくとも1つのセクションがあることを確認
        assert len(annex["sections"]) > 0, f"Annex {annex['annex_id']} should have at least one section"


def test_dma_annex_content():
    """DMAの附属書の内容が適切に抽出されていることを確認"""
    doc = run_dma_analyzer()
    
    # 少なくとも1つの附属書があることを確認
    assert len(doc["annexes"]) > 0
    
    # 最初の附属書をチェック
    first_annex = doc["annexes"][0]
    
    # タイトルが空でないことを確認
    assert first_annex["title"].strip() != "", "Annex title should not be empty"
    
    # セクションが空でないことを確認
    assert len(first_annex["sections"]) > 0, "Annex should have sections"
    
    # セクションの内容が空でないことを確認
    for section in first_annex["sections"]:
        assert isinstance(section, str)
        assert section.strip() != "", "Section content should not be empty"


def test_annex_sorting():
    """附属書がIDでソートされていることを確認"""
    doc = run_dma_analyzer()
    
    if len(doc["annexes"]) > 1:
        annex_ids = [annex["annex_id"] for annex in doc["annexes"]]
        sorted_ids = sorted(annex_ids)
        assert annex_ids == sorted_ids, f"Annexes should be sorted by ID. Got: {annex_ids}, Expected: {sorted_ids}"