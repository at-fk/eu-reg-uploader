"""Hierarchical SQLModel schema optimized for structured JSON data from eu_reg_html_analyzer.py."""

from datetime import datetime
from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine


class Regulation(SQLModel, table=True):
    """EU regulation metadata table."""
    
    celex_id: str = Field(primary_key=True, max_length=20)
    title: str = Field(max_length=500)
    type: str = Field(max_length=50, default="Regulation")
    adoption_date: Optional[datetime] = Field(default=None)
    end_of_validity_date: Optional[datetime] = Field(default=None)
    consolidated_version_id: Optional[str] = Field(default=None, max_length=50)
    consolidated_as_of_date: Optional[datetime] = Field(default=None)
    extracted_at: datetime = Field(default_factory=datetime.utcnow)


class Chapter(SQLModel, table=True):
    """Regulation chapters (e.g., Chapter I - General Provisions)."""
    
    chapter_id: str = Field(primary_key=True, max_length=100)  # e.g., "32016R0679-Ch1"
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    chapter_number: int = Field(index=True)
    title: str = Field(max_length=500)
    order_index: int = Field(default=0)


class Recital(SQLModel, table=True):
    """Regulation recitals (preamble provisions)."""
    
    recital_id: str = Field(primary_key=True, max_length=100)  # e.g., "32016R0679-Rec1"
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    recital_number: int = Field(index=True)
    text: str
    order_index: int = Field(default=0)
    html_id: Optional[str] = Field(default=None, max_length=50)  # Original HTML id


class Article(SQLModel, table=True):
    """Regulation articles."""
    
    article_id: str = Field(primary_key=True, max_length=100)  # e.g., "32016R0679-Art6"
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    chapter_id: Optional[str] = Field(foreign_key="chapter.chapter_id", default=None, max_length=100)
    article_number: int = Field(index=True)
    title: str = Field(max_length=500)
    content_full: str  # Complete article text
    order_index: int = Field(default=0)
    is_definitions: bool = Field(default=False)


class Paragraph(SQLModel, table=True):
    """Article paragraphs (numbered sections within articles)."""
    
    paragraph_id: str = Field(primary_key=True, max_length=150)  # e.g., "32016R0679-Art6-Para1"
    article_id: str = Field(foreign_key="article.article_id", max_length=100, index=True)
    paragraph_number: str = Field(max_length=10)  # "1", "2", etc.
    content_full: str  # Complete paragraph text
    order_index: int = Field(default=0)
    chapeau_count: int = Field(default=0)
    subparagraph_count: int = Field(default=0)


class SubParagraph(SQLModel, table=True):
    """Sub-paragraphs within paragraphs (lettered items like (a), (b), etc.)."""
    
    subparagraph_id: str = Field(primary_key=True, max_length=200)  # e.g., "32016R0679-Art6-Para1-Sub_a"
    paragraph_id: str = Field(foreign_key="paragraph.paragraph_id", max_length=150, index=True)
    element_id: str = Field(max_length=10)  # "a", "b", "c", etc.
    content: str
    order_index: int = Field(default=0)
    content_type: str = Field(default="subparagraph")  # subparagraph, chapeau, definition


class Annex(SQLModel, table=True):
    """Regulation annexes."""
    
    annex_id: str = Field(primary_key=True, max_length=100)  # e.g., "32016R0679-AnnexI"
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    annex_number: str = Field(max_length=20)  # "I", "II", "III", etc.
    title: str = Field(max_length=500)
    subtitle: Optional[str] = Field(default=None, max_length=1000)
    order_index: int = Field(default=0)


class AnnexSection(SQLModel, table=True):
    """Sections within annexes."""
    
    section_id: str = Field(primary_key=True, max_length=150)  # e.g., "32024R1689-AnnexI-Sec1"
    annex_id: str = Field(foreign_key="annex.annex_id", max_length=100, index=True)
    section_number: str = Field(max_length=20)  # "1", "2", "A", "B", etc.
    heading: str = Field(max_length=1000)
    list_type: Optional[str] = Field(default=None, max_length=50)  # "ordered", "unordered", etc.
    order_index: int = Field(default=0)


class AnnexSectionItem(SQLModel, table=True):
    """Items within annex sections (text content)."""
    
    item_id: str = Field(primary_key=True, max_length=200)  # e.g., "32024R1689-AnnexI-Sec1-Item1"
    section_id: str = Field(foreign_key="annexsection.section_id", max_length=150, index=True)
    content: str  # Text content of the item
    order_index: int = Field(default=0)


class AnnexTable(SQLModel, table=True):
    """Tables within annex sections."""
    
    table_id: str = Field(primary_key=True, max_length=200)  # e.g., "32024R1689-AnnexI-Sec1-Tab1"
    section_id: str = Field(foreign_key="annexsection.section_id", max_length=150, index=True)
    caption: Optional[str] = Field(default=None, max_length=1000)
    order_index: int = Field(default=0)


class AnnexTableRow(SQLModel, table=True):
    """Rows within annex tables."""
    
    row_id: str = Field(primary_key=True, max_length=250)  # e.g., "32024R1689-AnnexI-Sec1-Tab1-Row1"
    table_id: str = Field(foreign_key="annextable.table_id", max_length=200, index=True)
    row_data: str  # JSON string of row data (e.g., '{"0": "", "1": "1.0", "2": "Directive text..."}')
    order_index: int = Field(default=0)


class AmendmentHistory(SQLModel, table=True):
    """Amendment history tracking for regulations."""
    
    amendment_id: Optional[int] = Field(default=None, primary_key=True)
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    amending_act_celex: Optional[str] = Field(default=None, max_length=20)
    amending_act_eli: Optional[str] = Field(default=None, max_length=200)
    amendment_type: str = Field(max_length=50)  # "amended", "corrected", "repealed", "consolidated"
    amendment_date: Optional[datetime] = Field(default=None)
    article_reference: Optional[str] = Field(default=None, max_length=200)  # e.g., "AR 58 PA 2 PTA (g)"
    oj_reference: Optional[str] = Field(default=None, max_length=100)  # Official Journal reference
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ConsolidatedVersion(SQLModel, table=True):
    """Consolidated version metadata for regulations."""
    
    version_id: str = Field(primary_key=True, max_length=50)  # e.g., "01995L0046-20180525"
    base_celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    consolidated_date: datetime
    version_uri: Optional[str] = Field(default=None, max_length=300)
    is_current: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class StagedImplementation(SQLModel, table=True):
    """Staged implementation schedules for regulations with phased entry into force."""
    
    implementation_id: Optional[int] = Field(default=None, primary_key=True)
    celex_id: str = Field(foreign_key="regulation.celex_id", max_length=20, index=True)
    effective_date: datetime = Field(index=True)
    implementation_type: str = Field(max_length=50)  # e.g., "entry_into_force", "application", "deadline"
    scope_description: str = Field(max_length=500)  # Description of what becomes effective
    article_references: Optional[str] = Field(default=None, max_length=200)  # Legal basis (e.g., "Article 113(a)", "Article 99")
    affected_articles: Optional[str] = Field(default=None, max_length=200)  # Articles that become effective (e.g., "Article 3", "Articles 5-15")
    comment: Optional[str] = Field(default=None, max_length=1000)  # Additional comments from XML
    xml_type_code: Optional[str] = Field(default=None, max_length=50)  # Original XML type code
    is_main_application: bool = Field(default=False)  # Mark the primary application date
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Citation(SQLModel, table=True):
    """Links between case law and any provision type."""
    
    citation_id: Optional[int] = Field(default=None, primary_key=True)
    ecli: str = Field(foreign_key="caselaw.ecli", max_length=100, index=True)
    
    # Flexible foreign keys - one will be populated based on provision type
    regulation_id: Optional[str] = Field(foreign_key="regulation.celex_id", default=None, max_length=20)
    chapter_id: Optional[str] = Field(foreign_key="chapter.chapter_id", default=None, max_length=100)
    recital_id: Optional[str] = Field(foreign_key="recital.recital_id", default=None, max_length=100)
    article_id: Optional[str] = Field(foreign_key="article.article_id", default=None, max_length=100)
    paragraph_id: Optional[str] = Field(foreign_key="paragraph.paragraph_id", default=None, max_length=150)
    subparagraph_id: Optional[str] = Field(foreign_key="subparagraph.subparagraph_id", default=None, max_length=200)
    annex_id: Optional[str] = Field(foreign_key="annex.annex_id", default=None, max_length=100)
    
    quote_text: str = Field(max_length=1000)
    confidence: float = Field(default=1.0)
    first_seen_at: datetime = Field(default_factory=datetime.utcnow)


class Caselaw(SQLModel, table=True):
    """EU case law decisions table."""
    
    ecli: str = Field(primary_key=True, max_length=100)
    court: str = Field(max_length=100)
    decision_date: Optional[datetime] = Field(default=None)
    title: str = Field(max_length=1000)
    summary_text: str
    source_url: Optional[str] = Field(default=None, max_length=500)


def get_session(db_url: str = "sqlite:///eu_hierarchical.db") -> Session:
    """
    Create database session for hierarchical schema.
    
    Args:
        db_url: Database connection URL, defaults to SQLite
        
    Returns:
        Session: SQLModel database session
    """
    engine = create_engine(db_url, echo=False)
    SQLModel.metadata.create_all(engine)
    
    return Session(engine)