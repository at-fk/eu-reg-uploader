"""CLI for hierarchical EU law database operations."""

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlmodel import Session, select
from pathlib import Path
import json
import logging

from .models_hierarchical import get_session
from .ingest_structured_json import ingest_structured_json_file
# from .fetch_comprehensive_cases import create_comprehensive_gdpr_cases, ingest_cases_to_database
# from .link_citations_hierarchical import link_all_gdpr_citations, show_citation_stats
from .cellar_citation_ingester import CellarCitationIngester
from .eurlex_notice_parser import EurLexNoticeParser
from .batch_processor import EURegulationBatchProcessor

import sys
sys.path.append('..')
from edpb_guideline_collector import EDPBGuidelineCollector

console = Console()

@click.group()
@click.version_option()
def cli():
    """Hierarchical EU Law Database - Ingest structured JSON files"""
    pass

@cli.command()
@click.argument("json_file", type=click.Path(exists=True))
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def ingest(json_file, db_url):
    """Ingest structured JSON file into hierarchical database."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    with get_session(database_url) as session:
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Ingesting JSON file...", total=None)
                
                result = ingest_structured_json_file(Path(json_file), session)
                
                progress.update(task, description="✅ Ingestion complete!")
            
            console.print(f"✅ Successfully ingested {result['articles']} articles")
            console.print(f"📊 Statistics: {result}")
            
        except Exception as e:
            console.print(f"❌ Error during ingestion: {e}", style="red")
            raise

@cli.command()
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def status(db_url):
    """Show hierarchical database status."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        with get_session(database_url) as session:
            from .models_hierarchical import (
                Regulation, Chapter, Recital, Article, Paragraph,
                SubParagraph, Annex, AnnexSection, AnnexSectionItem,
                AnnexTable, AnnexTableRow, Citation, Caselaw
            )
            
            # Count records
            regulations = len(session.exec(select(Regulation)).all())
            chapters = len(session.exec(select(Chapter)).all())
            recitals = len(session.exec(select(Recital)).all())
            articles = len(session.exec(select(Article)).all())
            paragraphs = len(session.exec(select(Paragraph)).all())
            subparagraphs = len(session.exec(select(SubParagraph)).all())
            annexes = len(session.exec(select(Annex)).all())
            annex_sections = len(session.exec(select(AnnexSection)).all())
            annex_items = len(session.exec(select(AnnexSectionItem)).all())
            annex_tables = len(session.exec(select(AnnexTable)).all())
            annex_table_rows = len(session.exec(select(AnnexTableRow)).all())
            citations = len(session.exec(select(Citation)).all())
            caselaw = len(session.exec(select(Caselaw)).all())
            
            table = Table(title="Hierarchical Database Status")
            table.add_column("Table", style="cyan")
            table.add_column("Count", style="magenta")
            
            table.add_row("Regulations", str(regulations))
            table.add_row("Chapters", str(chapters))
            table.add_row("Recitals", str(recitals))
            table.add_row("Articles", str(articles))
            table.add_row("Paragraphs", str(paragraphs))
            table.add_row("Subparagraphs", str(subparagraphs))
            table.add_row("Annexes", str(annexes))
            table.add_row("Annex Sections", str(annex_sections))
            table.add_row("Annex Items", str(annex_items))
            table.add_row("Annex Tables", str(annex_tables))
            table.add_row("Annex Table Rows", str(annex_table_rows))
            table.add_row("Citations", str(citations))
            table.add_row("Caselaw", str(caselaw))
            
            console.print(table)
            
    except Exception as e:
        console.print(f"❌ Error accessing database: {e}", style="red")

@cli.command()
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def list_regulations(db_url):
    """List all regulations in the database."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        with get_session(database_url) as session:
            from .models_hierarchical import Regulation
            
            regulations = session.exec(select(Regulation)).all()
            
            if not regulations:
                console.print("No regulations found in database.", style="yellow")
                return
            
            table = Table(title="Regulations in Database")
            table.add_column("CELEX ID", style="cyan")
            table.add_column("Title", style="green")
            table.add_column("Type", style="blue")
            table.add_column("Adoption Date", style="yellow")
            
            for reg in regulations:
                try:
                    adoption_date = reg.adoption_date.strftime("%Y-%m-%d") if reg.adoption_date else "N/A"
                except AttributeError:
                    adoption_date = "N/A"
                table.add_row(reg.celex_id, reg.title, reg.type, adoption_date)
            
            console.print(table)
            
    except Exception as e:
        console.print(f"❌ Error accessing database: {e}", style="red")

@cli.command()
@click.argument("celex_id")
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def show_regulation(celex_id, db_url):
    """Show detailed information about a specific regulation."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        with get_session(database_url) as session:
            from .models_hierarchical import (
                Regulation, Chapter, Recital, Article, Paragraph,
                SubParagraph, Annex, AnnexSection, AnnexSectionItem,
                AnnexTable, AnnexTableRow, Citation, Caselaw
            )
            
            # Get regulation
            regulation = session.exec(
                select(Regulation).where(Regulation.celex_id == celex_id)
            ).first()
            
            if not regulation:
                console.print(f"❌ Regulation {celex_id} not found", style="red")
                return
            
            console.print(f"📋 [bold]Regulation: {regulation.celex_id}[/bold]")
            console.print(f"Title: {regulation.title}")
            console.print(f"Type: {regulation.type}")
            try:
                if regulation.adoption_date:
                    console.print(f"Adoption Date: {regulation.adoption_date.strftime('%Y-%m-%d')}")
            except AttributeError:
                pass
            console.print()
            
            # Count related records
            chapters = len(session.exec(
                select(Chapter).where(Chapter.celex_id == celex_id)
            ).all())
            recitals = len(session.exec(
                select(Recital).where(Recital.celex_id == celex_id)
            ).all())
            articles = len(session.exec(
                select(Article).where(Article.celex_id == celex_id)
            ).all())
            annexes = len(session.exec(
                select(Annex).where(Annex.celex_id == celex_id)
            ).all())
            citations = len(session.exec(
                select(Citation).where(Citation.regulation_id == celex_id)
            ).all())
            
            table = Table(title=f"Structure of {celex_id}")
            table.add_column("Component", style="cyan")
            table.add_column("Count", style="magenta")
            
            table.add_row("Chapters", str(chapters))
            table.add_row("Recitals", str(recitals))
            table.add_row("Articles", str(articles))
            table.add_row("Annexes", str(annexes))
            table.add_row("Citations", str(citations))
            
            console.print(table)
            
    except Exception as e:
        console.print(f"❌ Error accessing database: {e}", style="red")

# Temporarily disabled - missing dependencies
# @cli.command()
# @click.option(
#     "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
# )
# @click.option("--skip-ingest", is_flag=True, help="Skip database ingestion, only generate cases")
# def create_gdpr_cases(db_url, skip_ingest):
#     """Create comprehensive GDPR case law database."""
#     pass

# @cli.command()
# @click.option(
#     "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
# )
# def link_citations(db_url):
#     """Link all GDPR citations in the database."""
#     pass

# @cli.command()
# @click.option(
#     "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
# )
# def citation_stats(db_url):
#     """Show citation statistics."""
#     pass

@cli.command()
@click.argument("xml_file", type=click.Path(exists=True))
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def ingest_cellar(xml_file, db_url):
    """Ingest CELLAR REST API XML data into the database."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        console.print(f"🔄 Ingesting CELLAR data from {xml_file}...", style="blue")
        
        # Read XML file
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        with get_session(database_url) as session:
            ingester = CellarCitationIngester(session)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Processing CELLAR XML...", total=None)
                
                result = ingester.ingest_cellar_data(xml_content)
                
                progress.update(task, description="✅ CELLAR ingestion complete!")
            
            console.print(f"✅ CELLAR ingestion complete!", style="green")
            console.print(f"📊 Statistics: {result}")
            
            # Show ingestion stats
            stats = ingester.get_ingestion_stats()
            console.print(f"📈 Database stats after ingestion: {stats}")
            
    except Exception as e:
        console.print(f"❌ Error during CELLAR ingestion: {e}", style="red")
        raise

@cli.command()
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def cellar_stats(db_url):
    """Show CELLAR citation statistics."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        with get_session(database_url) as session:
            ingester = CellarCitationIngester(session)
            stats = ingester.get_ingestion_stats()
            
            table = Table(title="CELLAR Citation Statistics")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", style="magenta")
            
            table.add_row("Total Citations", str(stats['total_citations']))
            table.add_row("Total Caselaw", str(stats['total_caselaw']))
            table.add_row("Total Regulations", str(stats['total_regulations']))
            
            console.print(table)
            
            # Citations by type
            type_table = Table(title="Citations by Provision Type")
            type_table.add_column("Provision Type", style="cyan")
            type_table.add_column("Count", style="magenta")
            
            for provision_type, count in stats['citations_by_type'].items():
                type_table.add_row(provision_type.title(), str(count))
            
            console.print(type_table)
            
    except Exception as e:
        console.print(f"❌ Error showing CELLAR stats: {e}", style="red")

@cli.command()
@click.argument("xml_file", type=click.Path(exists=True))
@click.argument("regulation_celex_id")
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def ingest_eurlex(xml_file, regulation_celex_id, db_url):
    """Ingest EUR-Lex NOTICE format XML data into the database."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    
    try:
        console.print(f"🔄 Ingesting EUR-Lex NOTICE data from {xml_file}...", style="blue")
        
        # Read XML file
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        with get_session(database_url) as session:
            parser = EurLexNoticeParser(session)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Processing EUR-Lex NOTICE XML...", total=None)
                
                result = parser.ingest_eurlex_notice_data(xml_content, regulation_celex_id)
                
                progress.update(task, description="✅ EUR-Lex ingestion complete!")
            
            console.print(f"✅ EUR-Lex ingestion complete!", style="green")
            console.print(f"📊 Statistics: {result}")
            
            # Show ingestion stats
            stats = parser.get_ingestion_stats()
            console.print(f"📈 Database stats after ingestion: {stats}")
            
    except Exception as e:
        console.print(f"❌ Error during EUR-Lex ingestion: {e}", style="red")
        raise

@cli.command()
@click.argument("data_dir", type=click.Path(exists=True))
@click.option(
    "--db-url", help="Database URL (default: sqlite:///eu_hierarchical.db)"
)
def batch_process(data_dir, db_url):
    """Process all regulation files (JSON + XML) in a directory."""
    database_url = db_url or "sqlite:///eu_hierarchical.db"
    data_path = Path(data_dir)
    
    try:
        processor = EURegulationBatchProcessor(database_url)
        results = processor.process_batch(data_path)
        processor.print_batch_summary(results)
        
    except Exception as e:
        console.print(f"❌ Error during batch processing: {e}", style="red")
        raise

@cli.command()
@click.option('--page', default=0, help='Page number to scrape (default: 0)')
@click.option('--download-dir', default='edpb_guidelines', help='Directory to save PDFs (default: edpb_guidelines)')
def collect_edpb_guidelines(page, download_dir):
    """Collect EDPB GDPR guidelines from public consultations."""
    
    collector = EDPBGuidelineCollector(download_dir)
    
    console.print(f"🔄 Starting EDPB guideline collection for page {page}")
    console.print(f"📁 Download directory: {download_dir}")
    
    try:
        downloaded_files = collector.collect_guidelines(page)
        
        if downloaded_files:
            console.print(f"\n✅ Successfully downloaded {len(downloaded_files)} guidelines:")
            for file_info in downloaded_files:
                console.print(f"  • {file_info['title']}")
                console.print(f"    📄 {file_info['file_path']}")
        else:
            console.print(f"\n⚠️  No guidelines downloaded from page {page}")
            
    except Exception as e:
        console.print(f"\n❌ Error during collection: {e}")
        raise

if __name__ == "__main__":
    cli()