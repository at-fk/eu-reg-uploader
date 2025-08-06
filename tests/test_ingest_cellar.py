"""Test CELLAR ingestion functionality."""

import pytest
from unittest.mock import patch, MagicMock
from sqlmodel import Session, select

from eu_link_db.models_hierarchical import get_session, Caselaw, Citation
from eu_link_db.cellar_citation_ingester import CellarCitationIngester


@pytest.fixture
def test_session(tmp_path):
    """Create test database session."""
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"
    return get_session(db_url)


@pytest.fixture
def sample_rdf_xml():
    """Sample RDF/XML content for testing."""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:j.0="http://publications.europa.eu/ontology/cdm#"
         xmlns:j.2="http://publications.europa.eu/ontology/annotation#">
  <rdf:Description rdf:nodeID="N123">
    <owl:annotatedSource rdf:resource="https://publications.europa.eu/resource/celex/32016R0679"/>
    <owl:annotatedTarget rdf:resource="https://publications.europa.eu/resource/celex/ECLI:EU:C:2020:123"/>
    <j.2:fragment_citing_source>A6P1</j.2:fragment_citing_source>
    <j.2:fragment_cited_target>A6</j.2:fragment_cited_target>
  </rdf:Description>
</rdf:RDF>'''


def test_cellar_ingester_initialization(test_session):
    """Test that CellarCitationIngester initializes correctly."""
    ingester = CellarCitationIngester(test_session)
    assert ingester.session == test_session
    assert isinstance(ingester.namespaces, dict)
    assert 'rdf' in ingester.namespaces


def test_extract_celex_id():
    """Test CELEX ID extraction from URIs."""
    ingester = CellarCitationIngester(MagicMock())
    
    # Test various URI formats
    uri1 = "https://publications.europa.eu/resource/celex/32016R0679"
    assert ingester._extract_celex_id(uri1) == "32016R0679"
    
    uri2 = "https://publications.europa.eu/resource/celex/ECLI:EU:C:2020:123"
    assert ingester._extract_celex_id(uri2) == "ECLI:EU:C:2020:123"


def test_extract_ecli():
    """Test ECLI extraction from URIs."""
    ingester = CellarCitationIngester(MagicMock())
    
    uri = "https://publications.europa.eu/resource/ecli/ECLI:EU:C:2020:123"
    assert ingester._extract_ecli(uri) == "ECLI:EU:C:2020:123"


def test_parse_fragment_reference():
    """Test fragment reference parsing."""
    ingester = CellarCitationIngester(MagicMock())
    
    # Test article reference
    result = ingester._parse_fragment_reference("A6P1LB")
    assert result['type'] == 'article'
    assert result['article'] == 6
    assert result['paragraph'] == 1
    assert result['subparagraph'] == 'B'
    
    # Test chapter reference
    result = ingester._parse_fragment_reference("C108")
    assert result['type'] == 'chapter'
    assert result['numbers'] == [108]


@patch('eu_link_db.cellar_citation_ingester.requests.get')
def test_download_caselaw_metadata_success(mock_get, test_session):
    """Test successful caselaw metadata download."""
    # Mock successful response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'''<?xml version="1.0"?>
    <rdf:RDF xmlns:cdm="http://publications.europa.eu/ontology/cdm#"
             xmlns:dct="http://purl.org/dc/terms/"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        <cdm:resource_legal_title>Test Case Title</cdm:resource_legal_title>
        <cdm:work_date_document>2020-01-01</cdm:work_date_document>
        <dct:title>Test Court</dct:title>
        <cdm:case-law_ecli>ECLI:EU:C:2020:123</cdm:case-law_ecli>
    </rdf:RDF>'''
    mock_get.return_value = mock_response
    
    ingester = CellarCitationIngester(test_session)
    result = ingester._download_caselaw_metadata("62020CJ0123")
    
    assert result["title"] == "Test Case Title"
    assert result["court"] == "Test Court"
    assert result["ecli"] == "ECLI:EU:C:2020:123"


@patch('eu_link_db.cellar_citation_ingester.requests.get')
def test_download_caselaw_metadata_failure(mock_get, test_session):
    """Test caselaw metadata download failure returns empty dict."""
    # Mock failed response
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    
    ingester = CellarCitationIngester(test_session)
    result = ingester._download_caselaw_metadata("62020CJ0123")
    
    assert result == {}


def test_no_dummy_data_functions_exist():
    """Test that no dummy data generation functions exist in CELLAR ingester."""
    import eu_link_db.cellar_citation_ingester as cellar_module
    
    # Check that no functions with 'dummy', 'fake', or 'generate' in the name exist
    module_functions = [name for name in dir(cellar_module) if callable(getattr(cellar_module, name))]
    
    forbidden_patterns = ['dummy', 'fake', 'generate_sample', 'create_sample']
    for func_name in module_functions:
        for pattern in forbidden_patterns:
            assert pattern not in func_name.lower(), f"Found forbidden function: {func_name}"


def test_ingester_respects_no_dummy_data_policy(test_session):
    """Test that the ingester respects no dummy data policy."""
    ingester = CellarCitationIngester(test_session)
    
    # Test with invalid CELEX ID - should not create dummy records
    with patch.object(ingester, '_download_caselaw_metadata', return_value={}):
        result = ingester._ensure_caselaw_exists("ECLI:EU:C:2020:123", "invalid_celex")
        assert result is False
        
        # Verify no caselaw was created
        caselaw_records = test_session.exec(select(Caselaw)).all()
        assert len(caselaw_records) == 0


def test_get_ingestion_stats_structure(test_session):
    """Test that ingestion stats have expected structure."""
    ingester = CellarCitationIngester(test_session)
    stats = ingester.get_ingestion_stats()
    
    # Verify required keys exist
    required_keys = ['total_citations', 'total_caselaw', 'total_regulations', 'citations_by_type']
    for key in required_keys:
        assert key in stats
        
    # Verify citations_by_type structure
    citation_types = ['article', 'chapter', 'recital', 'paragraph', 'subparagraph', 'annex', 'regulation']
    for citation_type in citation_types:
        assert citation_type in stats['citations_by_type']