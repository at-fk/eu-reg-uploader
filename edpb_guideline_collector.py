"""EDPB GDPR Guideline Collector"""

import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import click
import json
from datetime import datetime

console = Console()

class EDPBGuidelineCollector:
    """Collects GDPR-related guidelines from EDPB website."""
    
    BASE_URL = "https://www.edpb.europa.eu"
    GUIDELINES_URL = "https://www.edpb.europa.eu/our-work-tools/general-guidance/guidelines-recommendations-best-practices_en"
    
    def __init__(self, download_dir: str = "edpb_guidelines"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.downloaded_files = set()  # Track downloaded filenames to avoid duplicates
    
    def fetch_guidelines_page(self, page_num: int = 0):
        """Fetch the guidelines page HTML content."""
        url = f"{self.GUIDELINES_URL}?page={page_num}"
        console.print(f"Fetching page {page_num}: {url}")
        
        response = self.session.get(url)
        response.raise_for_status()
        return response.text
    
    def extract_all_document_links(self, html_content):
        """Extract both public consultation and final/direct download links."""
        soup = BeautifulSoup(html_content, 'html.parser')
        documents = []
        
        # Find all views-row divs that contain guideline entries
        rows = soup.find_all('div', class_='views-row')
        
        # If no views-row divs found, look for alternative structures
        if not rows:
            # Alternative structure: look for document containers with different classes
            rows.extend(soup.find_all('div', class_=re.compile(r'node.*article', re.IGNORECASE)))
            rows.extend(soup.find_all('article', class_=re.compile(r'node', re.IGNORECASE)))
            rows.extend(soup.find_all('div', class_=re.compile(r'item.*document', re.IGNORECASE)))
            
            # Look for list items that may contain documents
            list_items = soup.find_all('li')
            for li in list_items:
                # Check if list item contains document-like content
                if li.find('a', href=re.compile(r'/our-work-tools/', re.IGNORECASE)):
                    rows.append(li)
        
        for row in rows:
            # Check if this entry is obsolete
            if self._is_obsolete_entry(row):
                console.print(f"⚠️  Skipping obsolete entry", style="yellow")
                continue
            
            # Extract title with multiple fallback strategies
            title = None
            title_elem = row.find('h4', class_='node__title')
            
            if title_elem:
                title_span = title_elem.find('span', class_='field--name-title')
                if title_span:
                    title = title_span.get_text(strip=True)
                else:
                    title = title_elem.get_text(strip=True)
            else:
                # Alternative title extraction methods
                # Try different heading levels
                for heading_tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    title_elem = row.find(heading_tag)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        break
                
                # Try looking for links that might contain titles
                if not title:
                    link_elem = row.find('a', href=re.compile(r'/our-work-tools/', re.IGNORECASE))
                    if link_elem:
                        title = link_elem.get_text(strip=True)
                
                # Try looking for span or div with title-like content
                if not title:
                    for title_elem in row.find_all(['span', 'div'], class_=re.compile(r'title|name', re.IGNORECASE)):
                        potential_title = title_elem.get_text(strip=True)
                        if len(potential_title) > 10:  # Reasonable title length
                            title = potential_title
                            break
            
            if not title:
                continue
            
            doc_type = self._get_document_type(title)
            if not title or not doc_type:
                continue
            
            # Look for different types of links
            document_entry = {
                'title': title,
                'doc_type': doc_type,
                'consultation_url': None,
                'final_url': None,
                'direct_pdf_url': None
            }
            
            # 1. Look for Public consultation links
            consultation_links = []
            consultation_links.extend(row.find_all('a', string=re.compile(r'Public consultation', re.IGNORECASE)))
            consultation_links.extend(row.find_all('a', href=re.compile(r'consultation', re.IGNORECASE)))
            consultation_links.extend(row.find_all('a', href=re.compile(r'/public-consultations/', re.IGNORECASE)))
            
            for consultation_link in consultation_links:
                href = consultation_link.get('href')
                if href and 'consultation' in href.lower():
                    document_entry['consultation_url'] = urljoin(self.BASE_URL, href)
                    break
            
            # 2. Look for final/adopted version links
            final_links = row.find_all('a', href=re.compile(r'/our-work-tools/our-documents/', re.IGNORECASE))
            for final_link in final_links:
                href = final_link.get('href')
                if href:
                    document_entry['final_url'] = urljoin(self.BASE_URL, href)
                    break
            
            # 3. Look for direct PDF download links
            pdf_links = row.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            for pdf_link in pdf_links:
                href = pdf_link.get('href')
                if href and '_en.pdf' in href.lower():  # Prioritize English
                    document_entry['direct_pdf_url'] = urljoin(self.BASE_URL, href)
                    break
            if not document_entry['direct_pdf_url'] and pdf_links:
                # Fallback to first PDF if no English version found
                href = pdf_links[0].get('href')
                if href:
                    document_entry['direct_pdf_url'] = urljoin(self.BASE_URL, href)
            
            # 4. Look for other document patterns (like Transparency, DPO, DPIA)
            # These may not have traditional Guidelines numbering
            if not any([document_entry['consultation_url'], document_entry['final_url'], document_entry['direct_pdf_url']]):
                # Check for simple title-based documents
                other_links = row.find_all('a', href=re.compile(r'/our-work-tools/our-documents/', re.IGNORECASE))
                for link in other_links:
                    href = link.get('href')
                    link_text = link.get_text(strip=True).lower()
                    # Look for standalone documents like "Transparency", "Data Protection Officer", etc.
                    if any(keyword in link_text for keyword in ['transparency', 'data protection officer', 'dpo', 'dpia', 'impact assessment', 'portability']):
                        document_entry['final_url'] = urljoin(self.BASE_URL, href)
                        break
            
            # Only add if we have at least one URL
            if any([document_entry['consultation_url'], document_entry['final_url'], document_entry['direct_pdf_url']]):
                documents.append(document_entry)
        
        # Remove duplicates based on title
        seen_titles = set()
        unique_documents = []
        for doc in documents:
            if doc['title'] not in seen_titles:
                seen_titles.add(doc['title'])
                unique_documents.append(doc)
        
        return unique_documents
    
    def _is_obsolete_entry(self, row):
        """Check if a document entry is marked as obsolete."""
        # Look for "Obsolete" text in the row
        obsolete_text = row.find(string=re.compile(r'Obsolete', re.IGNORECASE))
        if obsolete_text:
            return True
        
        # Note: WP (Working Party) documents are important and should be downloaded
        # Only skip if explicitly marked as obsolete
        return False
    
    def _get_document_type(self, title):
        """Determine if document is Guidelines or Recommendations."""
        title_lower = title.lower()
        if 'guideline' in title_lower:
            return 'Guidelines'
        elif 'recommendation' in title_lower:
            return 'Recommendations'
        # Include other EDPB document types
        elif any(keyword in title_lower for keyword in [
            'transparency', 'data protection officer', 'dpo', 'dpia', 
            'impact assessment', 'portability', 'guidance', 'automated',
            'decision-making', 'profiling', 'position paper', 'derogation',
            'records of processing', 'opinion'
        ]):
            return 'Guidelines'  # Treat these as Guidelines for categorization
        else:
            return None
    
    def find_pdf_download_link(self, page_url):
        """Find the PDF download link from a page (EDPB or external)."""
        console.print(f"Checking page: {page_url}")
        
        try:
            response = self.session.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if this is an Article 29 external link
            if 'ec.europa.eu' in page_url and 'article29' in page_url:
                # Look for external links to ec.europa.eu for Article 29 documents
                external_links = soup.find_all('a', href=re.compile(r'ec\.europa\.eu.*article29', re.IGNORECASE))
                for link in external_links:
                    href = link.get('href')
                    if href and ('item-detail' in href or 'document.cfm' in href):
                        # This is an Article 29 document link
                        article29_pdf = self.find_article29_pdf_link(href)
                        if article29_pdf:
                            return article29_pdf
            
            # Check for external links to Article 29 documents
            if 'edpb.europa.eu' in page_url:
                external_links = soup.find_all('a', href=re.compile(r'ec\.europa\.eu.*article29', re.IGNORECASE))
                for link in external_links:
                    href = link.get('href')
                    if href:
                        console.print(f"Found Article 29 external link: {href}")
                        article29_pdf = self.find_article29_pdf_link(href)
                        if article29_pdf:
                            return article29_pdf
            
            # Look for PDF download links with multiple patterns
            pdf_links = []
            
            # Pattern 1: Direct PDF links
            pdf_links.extend(soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE)))
            
            # Pattern 2: Links containing "download" text
            download_links = soup.find_all('a', string=re.compile(r'download', re.IGNORECASE))
            for link in download_links:
                href = link.get('href')
                if href and '.pdf' in href.lower():
                    pdf_links.append(link)
            
            # Pattern 3: Links in download sections or with download classes
            download_sections = soup.find_all(['div', 'section'], class_=re.compile(r'download', re.IGNORECASE))
            for section in download_sections:
                pdf_links.extend(section.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE)))
            
            # Pattern 4: File links (common path patterns)
            file_links = soup.find_all('a', href=re.compile(r'/system/files/', re.IGNORECASE))
            for link in file_links:
                href = link.get('href')
                if href and '.pdf' in href.lower():
                    pdf_links.append(link)
            
            # Pattern 5: Look for simple PDF download links on EDPB pages  
            # (for documents like Transparency that have direct PDFs)
            simple_pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            for link in simple_pdf_links:
                href = link.get('href')
                if href:
                    pdf_links.append(link)
            
            # Prioritize English PDF files
            for link in pdf_links:
                href = link.get('href')
                if href and '_en.pdf' in href.lower():
                    return urljoin(page_url, href)
            
            # Prioritize links with download-related text
            for link in pdf_links:
                href = link.get('href')
                link_text = link.get_text(strip=True).lower()
                
                if any(word in link_text for word in ['download', 'pdf', 'document', 'file']):
                    return urljoin(page_url, href)
            
            # If no specific download link found, return the first PDF link
            if pdf_links:
                return urljoin(page_url, pdf_links[0].get('href'))
                
        except Exception as e:
            console.print(f"Error accessing page {page_url}: {e}")
            
        return None
    
    def find_article29_pdf_link(self, page_url):
        """Find PDF links from Article 29 Working Party external pages."""
        try:
            response = self.session.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Pattern 1: Direct PDF links on ec.europa.eu
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            
            for link in pdf_links:
                href = link.get('href')
                if href:
                    # Prioritize English versions
                    if '_en.pdf' in href.lower():
                        if not href.startswith('http'):
                            href = urljoin(page_url, href)
                        return href
            
            # Pattern 2: Redirection/document links
            redirect_links = soup.find_all('a', href=re.compile(r'(redirection|document)', re.IGNORECASE))
            for link in redirect_links:
                href = link.get('href')
                if href and not href.startswith('http'):
                    href = urljoin(page_url, href)
                if href:
                    return href
            
            # Pattern 3: Any PDF file reference
            if pdf_links:
                href = pdf_links[0].get('href')
                if href and not href.startswith('http'):
                    href = urljoin(page_url, href)
                return href
                    
        except Exception as e:
            console.print(f"Error accessing Article 29 page {page_url}: {e}")
            
        return None
    
    def get_article29_title(self, page_url):
        """Extract Article 29 Working Party document title from external page."""
        try:
            response = self.session.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for Article 29 specific title patterns
            # Pattern 1: h1 or h2 tags with document titles
            for heading_tag in ['h1', 'h2', 'h3']:
                headings = soup.find_all(heading_tag)
                for heading in headings:
                    heading_text = heading.get_text(strip=True)
                    if any(keyword in heading_text.lower() for keyword in [
                        'working party', 'guidelines', 'opinion', 'recommendation', 'wp'
                    ]):
                        # Simplify Article 29 titles
                        simplified_title = self._simplify_article29_title(heading_text)
                        if simplified_title:
                            return simplified_title
            
            # Pattern 2: Look for div or span with document titles
            title_containers = soup.find_all(['div', 'span'], class_=re.compile(r'title|header', re.IGNORECASE))
            for container in title_containers:
                title_text = container.get_text(strip=True)
                if len(title_text) > 20 and any(keyword in title_text.lower() for keyword in [
                    'working party', 'guidelines', 'opinion', 'wp'
                ]):
                    simplified_title = self._simplify_article29_title(title_text)
                    if simplified_title:
                        return simplified_title
            
            # Pattern 3: Look for strong/b tags that might contain titles
            strong_tags = soup.find_all(['strong', 'b'])
            for strong in strong_tags:
                strong_text = strong.get_text(strip=True)
                if len(strong_text) > 20 and any(keyword in strong_text.lower() for keyword in [
                    'working party', 'guidelines', 'opinion'
                ]):
                    simplified_title = self._simplify_article29_title(strong_text)
                    if simplified_title:
                        return simplified_title
                        
        except Exception as e:
            console.print(f"Error extracting Article 29 title from {page_url}: {e}")
            
        return None
    
    def _simplify_article29_title(self, title):
        """Simplify Article 29 Working Party document titles."""
        if not title or len(title) < 10:
            return None
            
        title_lower = title.lower()
        
        # Skip if it's not an Article 29/WP document
        if not any(keyword in title_lower for keyword in [
            'article 29', 'working party', 'wp', 'guidelines', 'opinion'
        ]):
            return None
        
        # Clean up common patterns and create simplified titles
        simplified = title.strip()
        
        # Remove "Article 29 Working Party - " prefix
        simplified = re.sub(r'^Article 29 Working Party[\s\-]*', '', simplified, flags=re.IGNORECASE)
        
        # Replace "Guidelines on" with "Guidelines on"
        simplified = re.sub(r'^Guidelines on\s*', 'Guidelines on ', simplified, flags=re.IGNORECASE)
        
        # Replace "Opinion" with "Opinion on"
        if simplified.lower().startswith('opinion') and ' on ' not in simplified.lower():
            simplified = re.sub(r'^Opinion\s*', 'Opinion on ', simplified, flags=re.IGNORECASE)
        
        # Clean up quotes and parentheses
        simplified = re.sub(r'"([^"]*)"', r'\1', simplified)  # Remove quotes around words
        simplified = re.sub(r'\(([^)]*)\)', r'(\1)', simplified)  # Clean up parentheses
        
        # Add WP29 prefix for clarity
        if not simplified.lower().startswith('wp'):
            simplified = f"WP29 - {simplified}"
        
        # Clean up WP references
        simplified = re.sub(r'\(wp\d+.*?\)', '', simplified, flags=re.IGNORECASE)  # Remove (wp242rev.01) style references
        
        # Limit length to avoid overly long filenames
        if len(simplified) > 80:
            # Try to truncate at a word boundary
            truncated = simplified[:77]
            last_space = truncated.rfind(' ')
            if last_space > 50:
                simplified = truncated[:last_space] + '...'
            else:
                simplified = truncated + '...'
        
        # Final cleanup
        simplified = simplified.strip()
        
        return simplified
    
    def find_best_pdf_url(self, document_entry):
        """Find the best PDF URL from various sources."""
        # Priority order: direct_pdf_url > consultation PDF > final page PDF (including Article 29)
        
        # 1. Direct PDF URL (highest priority)
        if document_entry.get('direct_pdf_url'):
            return document_entry['direct_pdf_url'], 'direct'
        
        # 2. PDF from consultation page
        if document_entry.get('consultation_url'):
            pdf_url = self.find_pdf_download_link(document_entry['consultation_url'])
            if pdf_url:
                source_type = 'article29' if 'ec.europa.eu' in pdf_url else 'consultation'
                return pdf_url, source_type
        
        # 3. PDF from final/adopted version page (may include Article 29 external links)
        if document_entry.get('final_url'):
            pdf_url = self.find_pdf_download_link(document_entry['final_url'])
            if pdf_url:
                source_type = 'article29' if 'ec.europa.eu' in pdf_url else 'final'
                return pdf_url, source_type
        
        return None, None
    
    def download_pdf(self, pdf_url, title, source_type=None):
        """Download PDF file with sanitized title as filename, avoiding duplicates."""
        if not pdf_url:
            return None
        
        # For Article 29 documents, try to get better title from the source
        final_title = title
        if source_type == 'article29' and 'ec.europa.eu' in pdf_url:
            # Try to extract better title from Article 29 page
            article29_title = self.get_article29_title(pdf_url)
            if article29_title:
                final_title = article29_title
                console.print(f"📋 Using Article 29 title: {final_title}")
        
        # Sanitize filename
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', final_title)
        safe_filename = re.sub(r'_+', '_', safe_filename)
        safe_filename = safe_filename.strip('_')[:100]  # Limit length
        
        if not safe_filename.endswith('.pdf'):
            safe_filename += '.pdf'
        
        # Handle filename duplicates
        original_filename = safe_filename
        counter = 1
        while safe_filename in self.downloaded_files:
            # Extract filename without extension
            name_without_ext = original_filename[:-4] if original_filename.endswith('.pdf') else original_filename
            safe_filename = f"{name_without_ext} ({counter}).pdf"
            counter += 1
        
        # Add to tracking set
        self.downloaded_files.add(safe_filename)
        
        file_path = self.download_dir / safe_filename
        
        try:
            console.print(f"Downloading: {pdf_url}")
            response = self.session.get(pdf_url, stream=True)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            console.print(f"✅ Downloaded: {file_path}")
            return file_path
            
        except Exception as e:
            console.print(f"❌ Error downloading {pdf_url}: {e}")
            return None
    
    def collect_guidelines(self, page_num: int = 0):
        """Main method to collect all guidelines from a specific page."""
        try:
            # Fetch the page
            html_content = self.fetch_guidelines_page(page_num)
            
            # Extract all document links (consultation + final/direct)
            documents = self.extract_all_document_links(html_content)
            
            if not documents:
                console.print(f"No documents found on page {page_num}")
                return []
            
            # Separate by document type
            guidelines = [d for d in documents if d.get('doc_type') == 'Guidelines']
            recommendations = [d for d in documents if d.get('doc_type') == 'Recommendations']
            
            console.print(f"Found {len(documents)} documents on page {page_num}")
            console.print(f"  - Guidelines: {len(guidelines)}")
            console.print(f"  - Recommendations: {len(recommendations)}")
            
            # Show link type statistics
            consultation_count = len([d for d in documents if d.get('consultation_url')])
            final_count = len([d for d in documents if d.get('final_url')])
            direct_pdf_count = len([d for d in documents if d.get('direct_pdf_url')])
            
            console.print(f"  - With consultation links: {consultation_count}")
            console.print(f"  - With final/adopted links: {final_count}")
            console.print(f"  - With direct PDF links: {direct_pdf_count}")
            
            downloaded_files = []
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                
                for i, document in enumerate(documents):
                    task = progress.add_task(
                        f"Processing {document.get('doc_type', 'Unknown')}: {document['title'][:40]}...", 
                        total=None
                    )
                    
                    # Find best PDF URL
                    pdf_url, source_type = self.find_best_pdf_url(document)
                    
                    if pdf_url:
                        # For Article 29 documents, try to get better title
                        display_title = document['title']
                        if source_type == 'article29' and 'ec.europa.eu' in pdf_url:
                            article29_title = self.get_article29_title(pdf_url)
                            if article29_title:
                                display_title = article29_title
                        
                        # Download the PDF
                        file_path = self.download_pdf(pdf_url, document['title'], source_type)
                        if file_path:
                            downloaded_files.append({
                                'title': display_title,  # Use improved title for Article 29 docs
                                'original_title': document['title'],  # Keep original for reference
                                'consultation_url': document.get('consultation_url'),
                                'final_url': document.get('final_url'),
                                'direct_pdf_url': document.get('direct_pdf_url'),
                                'pdf_url': pdf_url,
                                'source_type': source_type,
                                'file_path': file_path,
                                'doc_type': document.get('doc_type', 'Unknown')
                            })
                    else:
                        console.print(f"⚠️  No PDF found for: {document['title']}")
                    
                    progress.update(task, description="✅ Complete")
            
            return downloaded_files
            
        except Exception as e:
            console.print(f"❌ Error collecting guidelines: {e}")
            raise
    
    def save_results_to_file(self, results, filename_prefix="edpb_collection_results"):
        """Save collection results to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        
        # Prepare data for JSON serialization
        json_data = {
            'collection_timestamp': datetime.now().isoformat(),
            'total_documents': len(results),
            'guidelines_count': len([r for r in results if r.get('doc_type') == 'Guidelines']),
            'recommendations_count': len([r for r in results if r.get('doc_type') == 'Recommendations']),
            'documents': []
        }
        
        for result in results:
            json_data['documents'].append({
                'title': result['title'],
                'doc_type': result.get('doc_type', 'Unknown'),
                'consultation_url': result.get('consultation_url'),
                'final_url': result.get('final_url'),
                'direct_pdf_url': result.get('direct_pdf_url'),
                'pdf_url': result['pdf_url'],
                'source_type': result.get('source_type', 'unknown'),
                'file_path': str(result['file_path']),
                'file_size_bytes': result['file_path'].stat().st_size if result['file_path'].exists() else 0
            })
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        console.print(f"📊 Results saved to: {filename}")
        return filename

@click.command()
@click.option('--page', default=0, help='Page number to scrape (default: 0)')
@click.option('--download-dir', default='edpb_guidelines', help='Directory to save PDFs (default: edpb_guidelines)')
def main(page, download_dir):
    """Collect EDPB GDPR guidelines from public consultations."""
    
    collector = EDPBGuidelineCollector(download_dir)
    
    console.print(f"🔄 Starting EDPB guideline collection for page {page}")
    console.print(f"📁 Download directory: {download_dir}")
    
    try:
        downloaded_files = collector.collect_guidelines(page)
        
        if downloaded_files:
            # Separate by document type for summary
            guidelines = [f for f in downloaded_files if f.get('doc_type') == 'Guidelines']
            recommendations = [f for f in downloaded_files if f.get('doc_type') == 'Recommendations']
            
            console.print(f"\n✅ Successfully downloaded {len(downloaded_files)} documents:")
            console.print(f"  📋 Guidelines: {len(guidelines)}")
            console.print(f"  📋 Recommendations: {len(recommendations)}")
            console.print()
            
            for file_info in downloaded_files:
                doc_type_icon = "📋" if file_info.get('doc_type') == 'Guidelines' else "📑"
                console.print(f"  {doc_type_icon} {file_info['title']}")
                console.print(f"    📄 {file_info['file_path']}")
        else:
            console.print(f"\n⚠️  No documents downloaded from page {page}")
            
    except Exception as e:
        console.print(f"\n❌ Error during collection: {e}")
        raise

if __name__ == "__main__":
    main()