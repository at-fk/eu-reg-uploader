"""Test JSON ingestion functionality."""

import json
import pytest
from pathlib import Path
from sqlmodel import Session, select

from eu_link_db.models_hierarchical import get_session, Regulation, Article
from eu_link_db.ingest_structured_json import ingest_structured_json_file


@pytest.fixture
def test_json_data():
    """Sample test JSON data."""
    return {
        "metadata": {
            "title": "Test Regulation",
            "celex_id": "32024R1689",
            "extraction_date": "2025-08-05"
        },
        "articles": [
            {
                "article_number": 1,
                "title": "Test Article",
                "content_full": "This is a test article for verification.",
                "order_index": 1,
                "metadata": {"is_definitions": False}
            }
        ]
    }


@pytest.fixture
def test_json_file(tmp_path, test_json_data):
    """Create temporary JSON file for testing."""
    json_file = tmp_path / "test_structured.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(test_json_data, f)
    return json_file


@pytest.fixture
def test_session(tmp_path):
    """Create test database session."""
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"
    return get_session(db_url)


def test_json_ingest_creates_regulation(test_session, test_json_file):
    """Test that JSON ingest creates regulation records."""
    # Ingest the test file
    result = ingest_structured_json_file(test_json_file, test_session)
    
    # Verify regulation was created
    assert result["regulation"] == 1
    
    # Check database
    regulations = test_session.exec(select(Regulation)).all()
    assert len(regulations) == 1
    assert regulations[0].celex_id == "32024R1689"


def test_json_ingest_creates_articles(test_session, test_json_file):
    """Test that JSON ingest creates article records."""
    # Ingest the test file
    result = ingest_structured_json_file(test_json_file, test_session)
    
    # Verify articles were created
    assert result["articles"] == 1
    
    # Check database
    articles = test_session.exec(select(Article)).all()
    assert len(articles) == 1
    assert articles[0].article_number == 1
    assert articles[0].title == "Test Article"


def test_no_dummy_data_functions_exist():
    """Test that no dummy data generation functions exist in the codebase."""
    import eu_link_db.ingest_structured_json as ingest_module
    
    # Check that no functions with 'dummy', 'fake', or 'generate' in the name exist
    module_functions = [name for name in dir(ingest_module) if callable(getattr(ingest_module, name))]
    
    forbidden_patterns = ['dummy', 'fake', 'generate_sample', 'create_sample']
    for func_name in module_functions:
        for pattern in forbidden_patterns:
            assert pattern not in func_name.lower(), f"Found forbidden function: {func_name}"


def test_ingest_count_greater_than_zero(test_session, test_json_file):
    """Test that ingestion creates records with count > 0."""
    result = ingest_structured_json_file(test_json_file, test_session)
    
    # Verify counts are positive
    assert result["regulation"] > 0
    assert result["articles"] > 0
    
    # All other counts should be 0 or positive
    for key, value in result.items():
        assert value >= 0, f"Negative count for {key}: {value}"