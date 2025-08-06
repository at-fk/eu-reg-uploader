"""Update regulation metadata in database from XML files."""

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from sqlmodel import Session, select
from eu_link_db.models_hierarchical import Regulation, get_session
from eu_link_db.amendment_parser import AmendmentParser


class RegulationXMLUpdater:
    """Update regulation metadata from XML files to database."""
    
    def __init__(self, session: Session):
        self.session = session
        self.amendment_parser = AmendmentParser(session)

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

    def _extract_celex_from_xml(self, xml_content: str) -> Optional[str]:
        """Extract CELEX ID from XML content."""
        try:
            root = ET.fromstring(xml_content)
            
            # Look for CELEX identifier
            celex_elem = root.find('.//IDENTIFIER[../TYPE[text()="celex"]]')
            if celex_elem is not None:
                return celex_elem.text
            
            # Alternative: look in VALUE with celex pattern
            value_elems = root.findall('.//VALUE')
            for elem in value_elems:
                if elem.text and 'celex/' in elem.text:
                    return elem.text.split('celex/')[-1].split('?')[0]
            
            return None
            
        except Exception as e:
            print(f"Error extracting CELEX from XML: {e}")
            return None

    def _extract_basic_metadata(self, xml_content: str, celex_id: str) -> Dict[str, Any]:
        """Extract basic metadata from XML."""
        try:
            root = ET.fromstring(xml_content)
            metadata = {}
            
            # Extract title
            title_elem = root.find('.//RESOURCE_LEGAL_TITLE/VALUE')
            if title_elem is not None and title_elem.text:
                metadata['title'] = title_elem.text
            
            # Extract adoption date (signature date)
            adoption_elem = root.find('.//RESOURCE_LEGAL_DATE_SIGNATURE/VALUE')
            if adoption_elem is not None:
                metadata['adoption_date'] = self._parse_date(adoption_elem.text)
            else:
                # Fallback to document date
                adoption_elem = root.find('.//WORK_DATE_DOCUMENT/VALUE')
                if adoption_elem is not None:
                    metadata['adoption_date'] = self._parse_date(adoption_elem.text)
            
            # Extract entry into force date 
            entry_force_elems = root.findall('.//RESOURCE_LEGAL_DATE_ENTRY-INTO-FORCE/VALUE')
            if entry_force_elems:
                # For AI Act, prefer the main application date (2026-08-02)
                # For GDPR, prefer the application date (2018-05-25)
                if celex_id == '32024R1689':  # AI Act
                    # Look for the main application date
                    for elem in entry_force_elems:
                        if elem.text == '2026-08-02':
                            metadata['entry_into_force_date'] = self._parse_date(elem.text)
                            break
                    # If not found, use the first partial entry date
                    if 'entry_into_force_date' not in metadata and entry_force_elems:
                        metadata['entry_into_force_date'] = self._parse_date(entry_force_elems[0].text)
                elif celex_id == '32016R0679':  # GDPR
                    # Skip the "20 days after publication" date for GDPR
                    for elem in entry_force_elems:
                        if elem.text and elem.text not in ['2016-05-24']:
                            metadata['entry_into_force_date'] = self._parse_date(elem.text)
                            break
                else:
                    # Default: use first entry into force date
                    metadata['entry_into_force_date'] = self._parse_date(entry_force_elems[0].text)
            
            # Extract application date
            application_elem = root.find('.//RESOURCE_LEGAL_DATE_APPLICATION/VALUE')
            if application_elem is not None:
                metadata['application_date'] = self._parse_date(application_elem.text)
            
            # Extract end of validity date
            end_validity_elem = root.find('.//RESOURCE_LEGAL_DATE_END-OF-VALIDITY/VALUE')
            if end_validity_elem is not None:
                metadata['end_of_validity_date'] = self._parse_date(end_validity_elem.text)
            
            # Extract consolidated version info
            consolidated_elem = root.find('.//RESOURCE_LEGAL_CONSOLIDATED_BY_ACT_CONSOLIDATED/VALUE')
            if consolidated_elem is not None and consolidated_elem.text:
                # Extract version ID from URI
                uri_text = consolidated_elem.text
                if '-' in uri_text and uri_text.split('-')[-1].isdigit():
                    version_id = uri_text.split('/')[-1]
                    metadata['consolidated_version_id'] = version_id
                    
                    # Extract date from version ID (last 8 digits YYYYMMDD)
                    date_part = version_id.split('-')[-1]
                    if len(date_part) == 8 and date_part.isdigit():
                        try:
                            consolidated_date = datetime.strptime(date_part, "%Y%m%d")
                            metadata['consolidated_as_of_date'] = consolidated_date
                        except ValueError:
                            pass
            
            return metadata
            
        except Exception as e:
            print(f"Error extracting metadata from XML: {e}")
            return {}

    def update_regulation_from_xml(self, xml_content: str, celex_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Update regulation metadata in database from XML content.
        
        Args:
            xml_content: XML content string
            celex_id: Optional CELEX ID (extracted from XML if not provided)
            
        Returns:
            Dictionary with update results
        """
        # Extract CELEX ID if not provided
        if not celex_id:
            celex_id = self._extract_celex_from_xml(xml_content)
            if not celex_id:
                return {'error': 'Could not extract CELEX ID from XML', 'updated': False}
        
        # Get existing regulation
        regulation = self.session.exec(
            select(Regulation).where(Regulation.celex_id == celex_id)
        ).first()
        
        if not regulation:
            return {'error': f'Regulation {celex_id} not found in database', 'updated': False}
        
        # Extract metadata from XML
        metadata = self._extract_basic_metadata(xml_content, celex_id)
        
        # Update regulation fields
        updated_fields = []
        
        if 'title' in metadata and metadata['title'] != regulation.title:
            regulation.title = metadata['title']
            updated_fields.append('title')
        
        if 'adoption_date' in metadata and metadata['adoption_date'] != regulation.adoption_date:
            regulation.adoption_date = metadata['adoption_date']
            updated_fields.append('adoption_date')
        
        if 'entry_into_force_date' in metadata:
            regulation.entry_into_force_date = metadata['entry_into_force_date']
            updated_fields.append('entry_into_force_date')
        
        if 'application_date' in metadata:
            regulation.application_date = metadata['application_date']
            updated_fields.append('application_date')
        
        if 'end_of_validity_date' in metadata:
            regulation.end_of_validity_date = metadata['end_of_validity_date']
            updated_fields.append('end_of_validity_date')
        
        if 'consolidated_version_id' in metadata:
            regulation.consolidated_version_id = metadata['consolidated_version_id']
            updated_fields.append('consolidated_version_id')
        
        if 'consolidated_as_of_date' in metadata:
            regulation.consolidated_as_of_date = metadata['consolidated_as_of_date']
            updated_fields.append('consolidated_as_of_date')
        
        # Commit changes
        if updated_fields:
            try:
                self.session.commit()
                print(f"Updated regulation {celex_id}: {', '.join(updated_fields)}")
            except Exception as e:
                self.session.rollback()
                return {'error': f'Failed to update regulation: {e}', 'updated': False}
        
        # Process amendment history
        amendment_results = self.amendment_parser.extract_amendment_history(xml_content, celex_id)
        
        return {
            'celex_id': celex_id,
            'updated': True,
            'updated_fields': updated_fields,
            'amendments_created': amendment_results.get('amendments_created', 0),
            'versions_created': amendment_results.get('versions_created', 0)
        }

    def update_regulation_from_xml_file(self, xml_path: Path, celex_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Update regulation metadata from XML file.
        
        Args:
            xml_path: Path to XML file
            celex_id: Optional CELEX ID
            
        Returns:
            Dictionary with update results
        """
        if not xml_path.exists():
            return {'error': f'XML file not found: {xml_path}', 'updated': False}
        
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            return self.update_regulation_from_xml(xml_content, celex_id)
            
        except Exception as e:
            return {'error': f'Failed to read XML file: {e}', 'updated': False}


def update_all_regulations_from_xml_directory(xml_directory: Path) -> Dict[str, Any]:
    """
    Update all regulations from XML files in directory.
    
    Args:
        xml_directory: Directory containing XML files
        
    Returns:
        Dictionary with processing results
    """
    results = {
        'files_processed': 0,
        'regulations_updated': 0,
        'total_amendments': 0,
        'total_versions': 0,
        'errors': []
    }
    
    xml_files = list(xml_directory.glob("*.xml"))
    
    with get_session() as session:
        updater = RegulationXMLUpdater(session)
        
        for xml_file in xml_files:
            try:
                print(f"Processing {xml_file.name}...")
                result = updater.update_regulation_from_xml_file(xml_file)
                
                results['files_processed'] += 1
                
                if result.get('updated'):
                    results['regulations_updated'] += 1
                    results['total_amendments'] += result.get('amendments_created', 0)
                    results['total_versions'] += result.get('versions_created', 0)
                    
                    print(f"✓ Updated regulation {result['celex_id']}")
                    if result['updated_fields']:
                        print(f"  Fields: {', '.join(result['updated_fields'])}")
                    if result.get('amendments_created', 0) > 0:
                        print(f"  Amendments: {result['amendments_created']}")
                    if result.get('versions_created', 0) > 0:
                        print(f"  Versions: {result['versions_created']}")
                else:
                    error_msg = result.get('error', 'Unknown error')
                    print(f"✗ {xml_file.name}: {error_msg}")
                    results['errors'].append(f"{xml_file.name}: {error_msg}")
            
            except Exception as e:
                error_msg = f"Failed to process {xml_file.name}: {str(e)}"
                results['errors'].append(error_msg)
                print(f"✗ {error_msg}")
    
    return results


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python xml_to_db_updater.py <xml_directory_or_file> [celex_id]")
        print("Examples:")
        print("  python xml_to_db_updater.py eu_link_db/")
        print("  python xml_to_db_updater.py eu_link_db/gdpr.xml 32016R0679")
        sys.exit(1)
    
    path = Path(sys.argv[1])
    celex_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not path.exists():
        print(f"Path not found: {path}")
        sys.exit(1)
    
    if path.is_file():
        # Process single XML file
        with get_session() as session:
            updater = RegulationXMLUpdater(session)
            result = updater.update_regulation_from_xml_file(path, celex_id)
            
            if result.get('updated'):
                print(f"✓ Updated regulation {result['celex_id']}")
                print(f"Fields updated: {', '.join(result['updated_fields'])}")
                print(f"Amendments created: {result.get('amendments_created', 0)}")
                print(f"Versions created: {result.get('versions_created', 0)}")
            else:
                print(f"✗ Failed: {result.get('error', 'Unknown error')}")
    
    elif path.is_dir():
        # Process directory
        print(f"Updating regulations from XML files in: {path}")
        results = update_all_regulations_from_xml_directory(path)
        
        print(f"\nResults:")
        print(f"Files processed: {results['files_processed']}")
        print(f"Regulations updated: {results['regulations_updated']}")
        print(f"Total amendments created: {results['total_amendments']}")
        print(f"Total versions created: {results['total_versions']}")
        
        if results['errors']:
            print(f"Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"  - {error}")
    
    else:
        print(f"Invalid path: {path}")
        sys.exit(1)