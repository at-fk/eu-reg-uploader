"""Batch processor for multiple regulations and their CELLAR data."""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from .models_hierarchical import get_session
from .ingest_structured_json import ingest_structured_json_file
from .cellar_citation_ingester import CellarCitationIngester
from .eurlex_notice_parser import EurLexNoticeParser

logger = logging.getLogger(__name__)
console = Console()


class EURegulationBatchProcessor:
    """Batch processor for EU regulations and their citation data."""
    
    def __init__(self, db_url: str = "sqlite:///eu_hierarchical.db"):
        self.db_url = db_url
        self.session = get_session(db_url)
        
    def discover_files(self, data_dir: Path) -> Dict[str, Dict[str, Optional[Path]]]:
        """Discover structured JSON and XML files for regulations."""
        files = {}
        
        # Look for structured JSON files
        for json_file in data_dir.glob("*_structured.json"):
            regulation_name = json_file.stem.replace("_structured", "")
            # Normalize regulation names
            normalized_name = self._normalize_regulation_name(regulation_name)
            if normalized_name not in files:
                files[normalized_name] = {"json": None, "xml": None}
            files[normalized_name]["json"] = json_file
            
        # Look for XML files
        for xml_file in data_dir.glob("*.xml"):
            regulation_name = xml_file.stem
            # Normalize regulation names
            normalized_name = self._normalize_regulation_name(regulation_name)
            if normalized_name not in files:
                files[normalized_name] = {"json": None, "xml": None}
            files[normalized_name]["xml"] = xml_file
            
        return files
    
    def _normalize_regulation_name(self, name: str) -> str:
        """Normalize regulation names for consistent matching."""
        # Convert to lowercase and replace spaces/underscores
        normalized = name.lower().replace(' ', '_').replace('-', '_')
        
        # Handle specific mappings
        if normalized in ['ai_act', 'aiact']:
            return 'ai_act'
        elif normalized == 'gdpr':
            return 'gdpr'
        
        return normalized
    
    def _get_celex_id(self, regulation_name: str) -> Optional[str]:
        """Get CELEX ID for a regulation name."""
        name_mapping = {
            'gdpr': '32016R0679',
            'ai_act': '32024R1689',
        }
        return name_mapping.get(regulation_name)
    
    def process_single_regulation(self, regulation_name: str, json_file: Path, xml_file: Optional[Path] = None) -> Dict[str, int]:
        """Process a single regulation with its JSON and optional XML data."""
        results = {"json_success": 0, "cellar_success": 0, "total_citations": 0, "total_caselaw": 0}
        
        try:
            # Process structured JSON
            if json_file and json_file.exists():
                console.print(f"📊 Processing JSON: {json_file.name}")
                json_result = ingest_structured_json_file(json_file, self.session)
                results["json_success"] = 1
                console.print(f"✅ JSON ingested: {json_result.get('articles', 0)} articles, {json_result.get('recitals', 0)} recitals")
            
            # Process XML if available (detect format and use appropriate parser)
            if xml_file and xml_file.exists():
                console.print(f"⚖️  Processing XML: {xml_file.name}")
                with open(xml_file, 'r', encoding='utf-8') as f:
                    xml_content = f.read()
                
                # Detect XML format
                if '<rdf:RDF' in xml_content:
                    # RDF/XML format - use CELLAR ingester
                    console.print("🔍 Detected RDF/XML format")
                    cellar_ingester = CellarCitationIngester(self.session)
                    cellar_result = cellar_ingester.ingest_cellar_data(xml_content)
                    
                    results["cellar_success"] = 1
                    results["total_citations"] = cellar_result.get("successful", 0)
                    results["total_caselaw"] = cellar_result.get("caselaw_created", 0)
                    
                    console.print(f"✅ RDF/XML ingested: {results['total_citations']} citations, {results['total_caselaw']} caselaw")
                    
                elif '<NOTICE' in xml_content:
                    # NOTICE format - use EUR-Lex parser
                    console.print("🔍 Detected EUR-Lex NOTICE format")
                    
                    # Extract regulation CELEX ID from normalized regulation name
                    regulation_celex = self._get_celex_id(regulation_name)
                    
                    if regulation_celex:
                        eurlex_parser = EurLexNoticeParser(self.session)
                        eurlex_result = eurlex_parser.ingest_eurlex_notice_data(xml_content, regulation_celex)
                        
                        results["cellar_success"] = 1
                        results["total_citations"] = eurlex_result.get("citations_created", 0)
                        results["total_caselaw"] = eurlex_result.get("caselaw_created", 0)
                        
                        console.print(f"✅ EUR-Lex NOTICE ingested: {results['total_citations']} citations, {results['total_caselaw']} caselaw")
                    else:
                        console.print(f"⚠️  Could not determine regulation CELEX ID for {regulation_name}")
                        
                else:
                    console.print(f"⚠️  Unknown XML format in {xml_file.name}")
            
        except Exception as e:
            console.print(f"❌ Error processing {regulation_name}: {e}", style="red")
            logger.error(f"Failed to process {regulation_name}: {e}")
            
        return results
    
    def process_batch(self, data_dir: Path) -> Dict[str, Dict[str, int]]:
        """Process all regulations found in the data directory."""
        console.print(f"🔍 Discovering files in {data_dir}")
        
        discovered_files = self.discover_files(data_dir)
        
        if not discovered_files:
            console.print("❌ No regulation files found", style="red")
            return {}
        
        console.print(f"📋 Found {len(discovered_files)} regulations:")
        for name, files in discovered_files.items():
            json_status = "✅" if files["json"] else "❌"
            xml_status = "✅" if files["xml"] else "❌"
            console.print(f"  • {name}: JSON {json_status}, XML {xml_status}")
        
        results = {}
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing regulations...", total=len(discovered_files))
            
            for regulation_name, files in discovered_files.items():
                progress.update(task, description=f"Processing {regulation_name}...")
                
                result = self.process_single_regulation(
                    regulation_name, 
                    files["json"], 
                    files["xml"]
                )
                results[regulation_name] = result
                
                progress.advance(task)
        
        return results
    
    def print_batch_summary(self, results: Dict[str, Dict[str, int]]):
        """Print a summary of batch processing results."""
        total_regulations = len(results)
        total_json_success = sum(1 for r in results.values() if r["json_success"])
        total_cellar_success = sum(1 for r in results.values() if r["cellar_success"])
        total_citations = sum(r["total_citations"] for r in results.values())
        total_caselaw = sum(r["total_caselaw"] for r in results.values())
        
        console.print("\n📊 [bold]Batch Processing Summary[/bold]")
        console.print(f"  • Regulations processed: {total_regulations}")
        console.print(f"  • JSON files ingested: {total_json_success}")
        console.print(f"  • CELLAR files ingested: {total_cellar_success}")
        console.print(f"  • Total citations created: {total_citations}")
        console.print(f"  • Total caselaw records created: {total_caselaw}")
        
        # Show detailed results
        console.print("\n📋 [bold]Detailed Results[/bold]")
        for name, result in results.items():
            status_json = "✅" if result["json_success"] else "❌"
            status_cellar = "✅" if result["cellar_success"] else "❌"
            console.print(f"  • {name}: JSON {status_json}, CELLAR {status_cellar} "
                         f"({result['total_citations']} citations, {result['total_caselaw']} caselaw)")


def main():
    """CLI entry point for batch processing."""
    import sys
    
    if len(sys.argv) < 2:
        console.print("Usage: python -m eu_link_db.batch_processor <data_directory>", style="red")
        sys.exit(1)
    
    data_dir = Path(sys.argv[1])
    if not data_dir.exists():
        console.print(f"❌ Directory not found: {data_dir}", style="red")
        sys.exit(1)
    
    processor = EURegulationBatchProcessor()
    results = processor.process_batch(data_dir)
    processor.print_batch_summary(results)


if __name__ == "__main__":
    main()