#!/usr/bin/env python3
"""
Debug script to test DMA annex extraction
"""

import requests
from bs4 import BeautifulSoup
import re

def main():
    # Fetch DMA HTML
    url = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:32022R1925"
    
    print("Fetching DMA HTML...")
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    print(f"HTML fetched successfully, length: {len(response.text)} characters")
    
    # Test different selectors for annex headers
    selectors_to_test = [
        'p.oj-doc-ti-annex',
        'p.oj-ti-annex', 
        'p.oj-doc-ti',
        '.oj-doc-ti',
        '.oj-ti-annex',
        '.oj-doc-ti-annex'
    ]
    
    print("\n=== Testing CSS Selectors ===")
    for selector in selectors_to_test:
        elements = soup.select(selector)
        print(f"\nSelector '{selector}': Found {len(elements)} elements")
        
        for i, elem in enumerate(elements):
            text = elem.get_text().strip()
            if 'ANNEX' in text.upper():
                print(f"  [{i}] MATCH: {text}")
                print(f"      ID: {elem.get('id', 'None')}")
                print(f"      Classes: {elem.get('class', [])}")
                print(f"      Parent: {elem.parent.name if elem.parent else 'None'}")
                if elem.parent:
                    print(f"      Parent ID: {elem.parent.get('id', 'None')}")
                    print(f"      Parent Classes: {elem.parent.get('class', [])}")
            else:
                print(f"  [{i}] No match: {text[:50]}...")
    
    # Test for text containing "ANNEX"
    print("\n=== Searching for 'ANNEX' text ===")
    all_p_tags = soup.find_all('p')
    annex_elements = []
    
    for p in all_p_tags:
        text = p.get_text().strip()
        if 'ANNEX' in text.upper():
            annex_elements.append(p)
            print(f"Found ANNEX text: '{text}'")
            print(f"  Classes: {p.get('class', [])}")
            print(f"  ID: {p.get('id', 'None')}")
            print(f"  Parent: {p.parent.name if p.parent else 'None'}")
            if p.parent:
                print(f"  Parent ID: {p.parent.get('id', 'None')}")
                print(f"  Parent Classes: {p.parent.get('class', [])}")
            print()
    
    # Look for div containers with annex IDs
    print("\n=== Looking for annex containers ===")
    annex_containers = soup.find_all('div', id=re.compile(r'anx_'))
    print(f"Found {len(annex_containers)} annex containers")
    
    for container in annex_containers:
        print(f"Container ID: {container.get('id')}")
        print(f"Container classes: {container.get('class', [])}")
        
        # Look for title elements within
        title_elements = container.find_all('p', class_='oj-doc-ti')
        for title in title_elements:
            print(f"  Title: '{title.get_text().strip()}'")
        
        # Look for section headers
        section_headers = container.find_all('p', class_='oj-ti-grseq-1')
        for header in section_headers:
            print(f"  Section: '{header.get_text().strip()}'")
        print()
    
    # Test the current algorithm from the code
    print("\n=== Testing Current Algorithm ===")
    # Step 1: Un-truncate hidden text
    for span in soup.select('[style*="display:none"]'):
        span.replace_with(span.get_text())
    
    # Step 2: Find annex headers
    header_q = 'p.oj-doc-ti-annex, p.oj-ti-annex, p.oj-doc-ti'
    headers = []
    
    for header in soup.select(header_q):
        text = header.get_text().strip()
        if 'ANNEX' in text.upper():
            headers.append(header)
    
    print(f"Current algorithm found {len(headers)} annex headers")
    
    for i, header in enumerate(headers):
        text = header.get_text().strip()
        print(f"Header {i+1}: '{text}'")
        
        # Try to extract annex ID
        annex_match = re.search(r'ANNEX\s+([IVXLC]+|[A-Z])', text)
        if annex_match:
            annex_id = annex_match.group(1)
            print(f"  Annex ID: {annex_id}")
        else:
            print(f"  No annex ID found in: {text}")
        
        # Look for content after this header
        current = header.next_sibling
        content_count = 0
        while current and content_count < 5:  # Just look at first few siblings
            if current.name and current.name in ['p', 'div', 'table', 'ul', 'ol']:
                # Check if this is another annex header
                if (current.name == 'p' and 
                    any(cls in current.get('class', []) for cls in ['oj-doc-ti-annex', 'oj-ti-annex', 'oj-doc-ti']) and
                    'ANNEX' in current.get_text().upper()):
                    print(f"  Found next annex header, stopping")
                    break
                
                content_text = current.get_text().strip()[:100]
                print(f"  Content {content_count}: {current.name} - {content_text}...")
                content_count += 1
            current = current.next_sibling
        print()

if __name__ == "__main__":
    main()