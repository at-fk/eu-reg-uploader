#!/usr/bin/env python3
"""Final Complete EDPB Collection - Including Article 29 WP documents"""

import sys
import os
from pathlib import Path
from rich.console import Console
from edpb_guideline_collector import EDPBGuidelineCollector

console = Console()

def main():
    """Collect all EDPB documents including Article 29 WP documents."""
    
    download_dir = "edpb_final_complete_collection"
    collector = EDPBGuidelineCollector(download_dir)
    
    console.print(f"🔄 Starting FINAL complete EDPB collection")
    console.print(f"📁 Download directory: {download_dir}")
    console.print(f"🎯 Target: EDPB + Article 29 Working Party documents")
    console.print()
    
    all_results = []
    page_summaries = []
    
    try:
        for page_num in range(7):  # pages 0-6
            console.print(f"\n📄 Processing page {page_num}...")
            
            try:
                page_results = collector.collect_guidelines(page_num)
                
                if page_results:
                    all_results.extend(page_results)
                    
                    guidelines = [r for r in page_results if r.get('doc_type') == 'Guidelines']
                    recommendations = [r for r in page_results if r.get('doc_type') == 'Recommendations']
                    
                    # Count source types including Article 29
                    direct_count = len([r for r in page_results if r.get('source_type') == 'direct'])
                    consultation_count = len([r for r in page_results if r.get('source_type') == 'consultation'])
                    final_count = len([r for r in page_results if r.get('source_type') == 'final'])
                    article29_count = len([r for r in page_results if r.get('source_type') == 'article29'])
                    
                    page_summary = {
                        'page': page_num,
                        'total': len(page_results),
                        'guidelines': len(guidelines),
                        'recommendations': len(recommendations),
                        'source_types': {
                            'direct': direct_count,
                            'consultation': consultation_count,
                            'final': final_count,
                            'article29': article29_count
                        },
                        'documents': page_results
                    }
                    page_summaries.append(page_summary)
                    
                    console.print(f"✅ Page {page_num}: {len(page_results)} documents")
                    console.print(f"   📋 {len(guidelines)} Guidelines, 📑 {len(recommendations)} Recommendations")
                    console.print(f"   🔗 Sources: {direct_count} direct, {consultation_count} consultation, {final_count} final, {article29_count} Article29")
                else:
                    console.print(f"⚠️  Page {page_num}: No documents found")
                    page_summaries.append({
                        'page': page_num,
                        'total': 0,
                        'guidelines': 0,
                        'recommendations': 0,
                        'source_types': {'direct': 0, 'consultation': 0, 'final': 0, 'article29': 0},
                        'documents': []
                    })
                    
            except Exception as e:
                console.print(f"❌ Error processing page {page_num}: {e}")
                continue
        
        # Final comprehensive summary
        console.print(f"\n🎉 FINAL Complete Collection Summary!")
        console.print(f"Total documents downloaded: {len(all_results)}")
        
        total_guidelines = len([r for r in all_results if r.get('doc_type') == 'Guidelines'])
        total_recommendations = len([r for r in all_results if r.get('doc_type') == 'Recommendations'])
        
        console.print(f"  📋 Guidelines: {total_guidelines}")
        console.print(f"  📑 Recommendations: {total_recommendations}")
        
        # Enhanced source type breakdown
        total_direct = len([r for r in all_results if r.get('source_type') == 'direct'])
        total_consultation = len([r for r in all_results if r.get('source_type') == 'consultation'])
        total_final = len([r for r in all_results if r.get('source_type') == 'final'])
        total_article29 = len([r for r in all_results if r.get('source_type') == 'article29'])
        
        console.print(f"\n🔗 Source breakdown:")
        console.print(f"  📎 Direct PDF links: {total_direct}")
        console.print(f"  💬 Consultation pages: {total_consultation}")
        console.print(f"  📋 Final/adopted pages: {total_final}")
        console.print(f"  🏛️  Article 29 WP documents: {total_article29}")
        
        # Article 29 document details
        if total_article29 > 0:
            console.print(f"\n🏛️  Article 29 Working Party documents retrieved:")
            article29_docs = [r for r in all_results if r.get('source_type') == 'article29']
            for doc in article29_docs:
                console.print(f"    • {doc['title']}")
        
        # Page-by-page breakdown
        console.print(f"\n📄 Page-by-page summary:")
        for summary in page_summaries:
            if summary['total'] > 0:
                sources = summary['source_types']
                console.print(f"  Page {summary['page']}: {summary['total']} docs ({summary['guidelines']} G, {summary['recommendations']} R) | D:{sources['direct']} C:{sources['consultation']} F:{sources['final']} A29:{sources['article29']}")
            else:
                console.print(f"  Page {summary['page']}: No documents")
        
        # Save comprehensive results
        if all_results:
            result_file = collector.save_results_to_file(all_results, "edpb_final_complete_collection")
            console.print(f"\n💾 All results saved to: {result_file}")
        
        # Save enhanced page summaries
        import json
        from datetime import datetime
        
        # Convert to JSON serializable format
        json_page_summaries = []
        for summary in page_summaries:
            json_summary = {
                'page': summary['page'],
                'total': summary['total'],
                'guidelines': summary['guidelines'],
                'recommendations': summary['recommendations'],
                'source_types': summary['source_types'],
                'documents': []
            }
            for doc in summary['documents']:
                json_summary['documents'].append({
                    'title': doc['title'],
                    'doc_type': doc.get('doc_type', 'Unknown'),
                    'consultation_url': doc.get('consultation_url'),
                    'final_url': doc.get('final_url'),
                    'direct_pdf_url': doc.get('direct_pdf_url'),
                    'pdf_url': doc['pdf_url'],
                    'source_type': doc.get('source_type', 'unknown'),
                    'file_path': str(doc['file_path'])
                })
            json_page_summaries.append(json_summary)
        
        summary_file = f"edpb_final_complete_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump({
                'collection_timestamp': datetime.now().isoformat(),
                'collection_version': 'v3_final_with_article29',
                'total_pages': 7,
                'total_documents': len(all_results),
                'total_guidelines': total_guidelines,
                'total_recommendations': total_recommendations,
                'source_breakdown': {
                    'direct': total_direct,
                    'consultation': total_consultation,
                    'final': total_final,
                    'article29': total_article29
                },
                'page_summaries': json_page_summaries
            }, f, indent=2, ensure_ascii=False)
        
        console.print(f"📊 Final summary saved to: {summary_file}")
        
        console.print(f"\n🎯 FINAL Collection Achievement!")
        console.print(f"Previous collection: 51 documents")
        console.print(f"Final collection: {len(all_results)} documents")
        console.print(f"Improvement: +{len(all_results)-51} documents")
        
        if total_article29 > 0:
            console.print(f"🏛️  Article 29 WP documents successfully integrated: {total_article29}")
        
        console.print(f"✅ Complete GDPR interpretation document collection achieved!")
        
    except KeyboardInterrupt:
        console.print(f"\n⚠️  Collection interrupted by user")
        if all_results:
            result_file = collector.save_results_to_file(all_results, "edpb_interrupted_final")
            console.print(f"💾 Partial results saved to: {result_file}")
    except Exception as e:
        console.print(f"\n❌ Error during collection: {e}")
        raise

if __name__ == "__main__":
    main()