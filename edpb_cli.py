#!/usr/bin/env python3
"""
EDPB Guidelines Processor CLI

EDPBガイドラインPDFの処理用CLIツール
- メタデータ抽出
- サマリー生成 (Gemini 2.5 Pro)
- 全文抽出
- チャンク分割・埋め込み生成 (gemini-embedding-001, 768次元)
- eu_hierarchical.dbへの保存
"""

import click
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from edpb_processor import EDPBProcessor
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

# 環境変数読み込み
load_dotenv()

console = Console()

def setup_logging(verbose: bool = False):
    """ログ設定"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('edpb_processing.log')
        ]
    )

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='詳細ログ出力を有効にする')
@click.pass_context
def cli(ctx, verbose):
    """EDPB Guidelines Processor CLI Tool"""
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    setup_logging(verbose)

@cli.command()
@click.argument('pdf_path', type=click.Path(exists=True, path_type=Path))
@click.option('--db-path', default='eu_hierarchical.db', help='データベースファイルパス')
@click.option('--chunk-size', default=1000, help='チャンクサイズ')
@click.option('--chunk-overlap', default=100, help='チャンクオーバーラップ')
@click.pass_context
def process_single(ctx, pdf_path: Path, db_path: str, chunk_size: int, chunk_overlap: int):
    """単一PDFファイルを処理する"""
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        console.print("[red]Error: GEMINI_API_KEY environment variable not set[/red]")
        sys.exit(1)
    
    try:
        processor = EDPBProcessor(
            gemini_api_key=gemini_api_key,
            db_path=db_path,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Processing {pdf_path.name}...", total=None)
            
            success = processor.process_pdf(pdf_path)
            
            if success:
                console.print(f"[green]✓ Successfully processed {pdf_path.name}[/green]")
            else:
                console.print(f"[red]✗ Failed to process {pdf_path.name}[/red]")
                sys.exit(1)
                
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

@cli.command()
@click.argument('directory_path', type=click.Path(exists=True, path_type=Path))
@click.option('--db-path', default='eu_hierarchical.db', help='データベースファイルパス')
@click.option('--chunk-size', default=1000, help='チャンクサイズ')
@click.option('--chunk-overlap', default=100, help='チャンクオーバーラップ')
@click.option('--continue-on-error', is_flag=True, help='エラー時も処理を継続する')
@click.pass_context
def process_batch(ctx, directory_path: Path, db_path: str, chunk_size: int, chunk_overlap: int, continue_on_error: bool):
    """ディレクトリ内のすべてのPDFファイルをバッチ処理する"""
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        console.print("[red]Error: GEMINI_API_KEY environment variable not set[/red]")
        sys.exit(1)
    
    try:
        processor = EDPBProcessor(
            gemini_api_key=gemini_api_key,
            db_path=db_path,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        
        # PDFファイル一覧取得
        pdf_files = list(directory_path.glob("*.pdf"))
        if not pdf_files:
            console.print(f"[yellow]No PDF files found in {directory_path}[/yellow]")
            return
        
        console.print(f"[blue]Found {len(pdf_files)} PDF files to process[/blue]")
        
        # バッチ処理実行
        with Progress(console=console) as progress:
            main_task = progress.add_task("Processing PDFs...", total=len(pdf_files))
            
            results = {"success": 0, "failed": 0, "total": len(pdf_files)}
            failed_files = []
            
            for pdf_file in pdf_files:
                progress.update(main_task, description=f"Processing {pdf_file.name}")
                
                try:
                    success = processor.process_pdf(pdf_file)
                    if success:
                        results["success"] += 1
                        console.print(f"[green]✓ {pdf_file.name}[/green]")
                    else:
                        results["failed"] += 1
                        failed_files.append(pdf_file.name)
                        console.print(f"[red]✗ {pdf_file.name}[/red]")
                        
                        if not continue_on_error:
                            console.print("[red]Processing stopped due to error. Use --continue-on-error to continue.[/red]")
                            break
                            
                except Exception as e:
                    results["failed"] += 1
                    failed_files.append(pdf_file.name)
                    console.print(f"[red]✗ {pdf_file.name}: {str(e)}[/red]")
                    
                    if not continue_on_error:
                        console.print("[red]Processing stopped due to error. Use --continue-on-error to continue.[/red]")
                        break
                
                progress.advance(main_task)
        
        # 結果サマリー表示
        display_results_summary(results, failed_files)
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

@cli.command()
@click.option('--db-path', default='eu_hierarchical.db', help='データベースファイルパス')
def status(db_path: str):
    """処理状況を確認する"""
    import sqlite3
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # ガイドライン処理状況
        cursor = conn.execute("""
            SELECT processing_status, COUNT(*) as count
            FROM edpb_guidelines
            GROUP BY processing_status
            ORDER BY processing_status
        """)
        status_counts = dict(cursor.fetchall())
        
        # 最近の処理ログ
        cursor = conn.execute("""
            SELECT g.filename, l.processing_step, l.status, l.created_at, l.error_message
            FROM edpb_processing_log l
            JOIN edpb_guidelines g ON l.guideline_id = g.guideline_id
            ORDER BY l.created_at DESC
            LIMIT 10
        """)
        recent_logs = cursor.fetchall()
        
        conn.close()
        
        # 状況表示
        table = Table(title="EDPB Guidelines Processing Status")
        table.add_column("Status", style="cyan")
        table.add_column("Count", style="magenta", justify="right")
        
        for status, count in status_counts.items():
            color = "green" if status == "completed" else "red" if status == "failed" else "yellow"
            table.add_row(f"[{color}]{status}[/{color}]", str(count))
        
        console.print(table)
        
        if recent_logs:
            console.print("\n[blue]Recent Processing Activity:[/blue]")
            for log in recent_logs:
                status_color = "green" if log['status'] == "completed" else "red"
                console.print(f"[{status_color}]{log['status']}[/{status_color}] {log['filename']} - {log['processing_step']} ({log['created_at']})")
                if log['error_message']:
                    console.print(f"  [red]Error: {log['error_message']}[/red]")
        
    except sqlite3.Error as e:
        console.print(f"[red]Database error: {str(e)}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

@cli.command()
@click.option('--db-path', default='eu_hierarchical.db', help='データベースファイルパス')
def list_guidelines(db_path: str):
    """保存されているガイドライン一覧を表示する"""
    import sqlite3
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT guideline_id, filename, title, document_type, version, 
                   adoption_date, processing_status, created_at
            FROM edpb_guidelines
            ORDER BY created_at DESC
        """)
        guidelines = cursor.fetchall()
        conn.close()
        
        if not guidelines:
            console.print("[yellow]No guidelines found in database[/yellow]")
            return
        
        table = Table(title=f"EDPB Guidelines Database ({len(guidelines)} records)")
        table.add_column("ID", style="cyan", width=4)
        table.add_column("Filename", style="blue", width=40)
        table.add_column("Type", style="green", width=12)
        table.add_column("Version", style="yellow", width=8)
        table.add_column("Date", style="magenta", width=12)
        table.add_column("Status", style="red", width=10)
        
        for guideline in guidelines:
            status_color = "green" if guideline['processing_status'] == "completed" else "red" if guideline['processing_status'] == "failed" else "yellow"
            table.add_row(
                str(guideline['guideline_id']),
                guideline['filename'][:37] + "..." if len(guideline['filename']) > 40 else guideline['filename'],
                guideline['document_type'],
                guideline['version'] or "N/A",
                guideline['adoption_date'][:10] if guideline['adoption_date'] else "N/A",
                f"[{status_color}]{guideline['processing_status']}[/{status_color}]"
            )
        
        console.print(table)
        
    except sqlite3.Error as e:
        console.print(f"[red]Database error: {str(e)}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

@cli.command()
@click.argument('guideline_id', type=int)
@click.option('--db-path', default='eu_hierarchical.db', help='データベースファイルパス')
def show_detail(guideline_id: int, db_path: str):
    """特定のガイドラインの詳細情報を表示する"""
    import sqlite3
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # ガイドライン基本情報
        cursor = conn.execute("""
            SELECT * FROM edpb_guidelines WHERE guideline_id = ?
        """, (guideline_id,))
        guideline = cursor.fetchone()
        
        if not guideline:
            console.print(f"[red]Guideline ID {guideline_id} not found[/red]")
            return
        
        # チャンク情報
        cursor = conn.execute("""
            SELECT COUNT(*) as chunk_count, 
                   SUM(CASE WHEN embedding_status = 'completed' THEN 1 ELSE 0 END) as completed_chunks
            FROM edpb_chunks WHERE guideline_id = ?
        """, (guideline_id,))
        chunk_info = cursor.fetchone()
        
        # 処理ログ
        cursor = conn.execute("""
            SELECT processing_step, status, processing_time_seconds, created_at, error_message
            FROM edpb_processing_log 
            WHERE guideline_id = ?
            ORDER BY created_at
        """, (guideline_id,))
        logs = cursor.fetchall()
        
        conn.close()
        
        # 詳細表示
        panel_content = f"""
[bold blue]Filename:[/bold blue] {guideline['filename']}
[bold blue]Title:[/bold blue] {guideline['title']}
[bold blue]Document Type:[/bold blue] {guideline['document_type']}
[bold blue]Version:[/bold blue] {guideline['version'] or 'N/A'}
[bold blue]Adoption Date:[/bold blue] {guideline['adoption_date'] or 'N/A'}
[bold blue]Page Count:[/bold blue] {guideline['page_count']}
[bold blue]File Size:[/bold blue] {guideline['file_size_bytes']:,} bytes
[bold blue]Processing Status:[/bold blue] {guideline['processing_status']}
[bold blue]Chunks:[/bold blue] {chunk_info['completed_chunks']}/{chunk_info['chunk_count']} completed
[bold blue]Created:[/bold blue] {guideline['created_at']}
[bold blue]Updated:[/bold blue] {guideline['updated_at']}
        """
        
        if guideline['subject_matter']:
            panel_content += f"\n[bold blue]Subject Matter:[/bold blue] {guideline['subject_matter']}"
        
        if guideline['related_articles']:
            panel_content += f"\n[bold blue]Related Articles:[/bold blue] {guideline['related_articles']}"
        
        console.print(Panel(panel_content.strip(), title=f"Guideline Details (ID: {guideline_id})"))
        
        # サマリー表示
        if guideline['summary']:
            console.print(Panel(guideline['summary'], title="Summary"))
        
        # 処理ログ表示
        if logs:
            log_table = Table(title="Processing Log")
            log_table.add_column("Step", style="cyan")
            log_table.add_column("Status", style="green")
            log_table.add_column("Time (s)", style="yellow", justify="right")
            log_table.add_column("Timestamp", style="magenta")
            
            for log in logs:
                status_color = "green" if log['status'] == "completed" else "red"
                processing_time = f"{log['processing_time_seconds']:.2f}" if log['processing_time_seconds'] else "N/A"
                log_table.add_row(
                    log['processing_step'],
                    f"[{status_color}]{log['status']}[/{status_color}]",
                    processing_time,
                    log['created_at']
                )
                if log['error_message']:
                    console.print(f"[red]Error in {log['processing_step']}: {log['error_message']}[/red]")
            
            console.print(log_table)
        
    except sqlite3.Error as e:
        console.print(f"[red]Database error: {str(e)}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

def display_results_summary(results: dict, failed_files: list):
    """処理結果サマリーを表示"""
    total = results['total']
    success = results['success']
    failed = results['failed']
    
    # サマリーテーブル
    table = Table(title="Processing Results Summary")
    table.add_column("Status", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    table.add_column("Percentage", style="yellow", justify="right")
    
    table.add_row("[green]Success[/green]", str(success), f"{success/total*100:.1f}%")
    table.add_row("[red]Failed[/red]", str(failed), f"{failed/total*100:.1f}%")
    table.add_row("[blue]Total[/blue]", str(total), "100.0%")
    
    console.print(table)
    
    # 失敗ファイル一覧
    if failed_files:
        console.print("\n[red]Failed Files:[/red]")
        for file in failed_files:
            console.print(f"  [red]✗ {file}[/red]")

if __name__ == '__main__':
    cli()