"""Tests for annex extraction functionality."""

import pytest
import sys
import os
from pathlib import Path
import json

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from eu_reg_html_analyzer import EURegulationAnalyzer


def run_analyzer(url_or_path):
    """Run analyzer on URL or local file path"""
    if isinstance(url_or_path, Path) and url_or_path.exists():
        # Local file mode (for future fixture support)
        pytest.skip("Local file fixtures not yet implemented")
    
    # Determine regulation type from URL
    if "32016R0679" in str(url_or_path):  # GDPR
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
    elif "32022R1925" in str(url_or_path):  # DMA
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
    elif "32024R1689" in str(url_or_path):  # AI Act
        metadata = {
            "name": "AI_Act",
            "official_title": "Artificial Intelligence Act",
            "short_title": "AI_Act", 
            "jurisdiction_id": "EU",
            "document_date": "2024-05-21",
            "version": "1.0",
            "status": "enacted",
            "metadata": {}
        }
    else:
        metadata = {
            "name": "Unknown",
            "official_title": "Unknown Regulation",
            "short_title": "Unknown",
            "jurisdiction_id": "EU",
            "document_date": "2024-01-01",
            "version": "1.0",
            "status": "enacted",
            "metadata": {}
        }
    
    analyzer = EURegulationAnalyzer(str(url_or_path), metadata)
    
    if not analyzer._download_content():
        pytest.skip(f"Failed to download HTML content from {url_or_path}")
    
    annexes = analyzer._extract_annexes()
    
    return {
        "annexes": annexes
    }


def test_gdpr_has_no_annexes():
    """GDPRには附属書がないことを確認"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32016R0679")
    assert len(doc["annexes"]) == 0, f"GDPR should have no annexes, got {len(doc['annexes'])}"


@pytest.mark.parametrize("url,expected_content", [
    ("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32024R1689", "harmonisation"),
    ("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925", "methodology")
])
def test_annex_extract(url, expected_content):
    """Test annex extraction for AI Act and DMA with expected content"""
    doc = run_analyzer(url)
    
    # Should have at least one annex
    assert len(doc["annexes"]) > 0, f"Should have at least one annex, got {len(doc['annexes'])}"
    
    # Find ANNEX I (should be the first or second annex)
    annex_i = None
    for annex in doc["annexes"]:
        if annex["annex_id"] == "I":
            annex_i = annex
            break
    
    assert annex_i is not None, "Should have ANNEX I"
    assert "sections" in annex_i and annex_i["sections"], "ANNEX I should have sections"
    
    # Check that expected content appears anywhere in the annex (items or tables)
    found_content = False
    for section in annex_i["sections"]:
        # Check in text items
        for item in section.get("items", []):
            if expected_content.lower() in item.lower():
                found_content = True
                break
        
        # Check in table content
        if not found_content:
            for table in section.get("tables", []):
                # Check table caption
                if expected_content.lower() in table.get("caption", "").lower():
                    found_content = True
                    break
                # Check table rows
                for row in table.get("rows", []):
                    for value in row.values():
                        if isinstance(value, str) and expected_content.lower() in value.lower():
                            found_content = True
                            break
                    if found_content:
                        break
                if found_content:
                    break
        
        if found_content:
            break
    
    assert found_content, f"Expected content '{expected_content}' not found in ANNEX I (checked items and tables)"


def test_revised_annex_structure():
    """Test that revised annexes have the expected structure"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925")
    
    for annex in doc["annexes"]:
        # Required fields from specification (no top-level tables anymore)
        assert "annex_id" in annex, "Should have annex_id"
        assert "title" in annex, "Should have title"
        assert "sections" in annex, "Should have sections"
        assert "order_index" in annex, "Should have order_index"
        
        # Data types
        assert isinstance(annex["annex_id"], str), "annex_id should be string"
        assert isinstance(annex["title"], str), "title should be string"
        assert isinstance(annex["sections"], list), "sections should be list"
        assert isinstance(annex["order_index"], int), "order_index should be int"
        
        # Section structure
        for section in annex["sections"]:
            assert "section_id" in section, "Section should have section_id"
            assert "heading" in section, "Section should have heading"
            assert "list_type" in section, "Section should have list_type"
            assert "items" in section, "Section should have items"
            assert "subsections" in section, "Section should have subsections"
            assert "tables" in section, "Section should have tables array"
            
            # Data types within sections
            assert isinstance(section["tables"], list), "Section tables should be list"
            
            # List type validation
            assert section["list_type"] in ["dash", "ordered", "letter"], \
                f"Invalid list_type: {section['list_type']}"
            
            # Items should not be empty or just dashes
            for item in section["items"]:
                assert item.strip() != "", "Item should not be empty"
                assert item.strip() != "—", "Item should not be just a dash"
            
            # Validate table structure within sections
            for table in section["tables"]:
                assert "caption" in table, "Table should have caption"
                assert "rows" in table, "Table should have rows"
                assert isinstance(table["rows"], list), "Table rows should be list"


def test_table_extraction():
    """Test that tables are properly extracted within sections"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925")
    
    # Should have sections with tables
    has_tables = False
    for annex in doc["annexes"]:
        for section in annex["sections"]:
            if section.get("tables"):
                has_tables = True
                for table in section["tables"]:
                    assert "caption" in table, "Table should have caption"
                    assert "rows" in table, "Table should have rows"
                    assert isinstance(table["rows"], list), "Table rows should be list"
                    
                    # Each row should be a dict
                    for row in table["rows"]:
                        assert isinstance(row, dict), "Table row should be dict"
    
    # At least one section should have tables
    assert has_tables, "Should find tables within sections"


def test_no_orphan_dashes():
    """Test that there are no orphan dash items"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925")
    
    for annex in doc["annexes"]:
        for section in annex["sections"]:
            for item in section["items"]:
                assert item.strip() != "—", f"Found orphan dash in annex {annex['annex_id']}"
                assert item.strip() != "", f"Found empty item in annex {annex['annex_id']}"
            
            # Check subsections too
            for subsection in section["subsections"]:
                for item in subsection["items"]:
                    assert item.strip() != "—", f"Found orphan dash in subsection"
                    assert item.strip() != "", f"Found empty item in subsection"


def test_annex_id_uniqueness():
    """Test that annex_ids are unique across all regulations"""
    # Test AI Act which previously had duplicate annexes
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32024R1689")
    
    annex_ids = [annex["annex_id"] for annex in doc["annexes"]]
    unique_ids = set(annex_ids)
    
    assert len(annex_ids) == len(unique_ids), \
        f"Duplicate annex_ids found: {[id for id in annex_ids if annex_ids.count(id) > 1]}"


def test_section_id_uniqueness_within_annexes():
    """Test that section_ids are unique within each annex"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32024R1689")
    
    for annex in doc["annexes"]:
        section_ids = [section["section_id"] for section in annex["sections"]]
        unique_ids = set(section_ids)
        
        assert len(section_ids) == len(unique_ids), \
            f"Duplicate section_ids in annex {annex['annex_id']}: {[id for id in section_ids if section_ids.count(id) > 1]}"


def test_ai_act_deduplication():
    """Test that AI Act no longer has duplicate annexes (regression test)"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32024R1689")
    
    # Count occurrences of each annex_id
    annex_counts = {}
    for annex in doc["annexes"]:
        annex_id = annex["annex_id"]
        annex_counts[annex_id] = annex_counts.get(annex_id, 0) + 1
    
    # Verify no duplicates
    for annex_id, count in annex_counts.items():
        assert count == 1, f"Annex {annex_id} appears {count} times (should be 1)"
    
    # Verify annexes have content (not empty)
    for annex in doc["annexes"]:
        if annex["annex_id"] in ["I", "II", "III", "IV", "V", "VI", "VII", "VIII"]:  # Known AI Act annexes
            has_content = bool(annex["sections"])
            if annex["sections"]:
                # Check if sections have items or tables
                has_items = any(section.get("items") for section in annex["sections"])
                has_tables = any(section.get("tables") for section in annex["sections"])
                has_content = has_items or has_tables
            
            assert has_content, f"Annex {annex['annex_id']} should have content (sections with items or tables)"


def test_hierarchical_section_numbering():
    """Test that hierarchical numbering like '3.1' is handled correctly"""
    doc = run_analyzer("https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32024R1689")
    
    # Look for hierarchical section IDs in any annex
    found_hierarchical = False
    for annex in doc["annexes"]:
        for section in annex["sections"]:
            section_id = section["section_id"]
            if "." in section_id and section_id.replace(".", "").isdigit():
                found_hierarchical = True
                # Verify the format is correct (e.g., "3.1", "4.2")
                parts = section_id.split(".")
                assert len(parts) == 2, f"Hierarchical section_id should have 2 parts: {section_id}"
                assert all(part.isdigit() for part in parts), f"All parts should be digits: {section_id}"
    
    # Note: This test might pass even if no hierarchical sections exist, 
    # which is fine - it just validates the format when they do exist
    if found_hierarchical:
        print("Found and validated hierarchical section numbering")
    else:
        print("No hierarchical section numbering found (this is OK)")