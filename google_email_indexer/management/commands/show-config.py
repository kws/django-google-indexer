import os
from django.core.management.base import BaseCommand
from django.conf import settings
from rich.console import Console
from rich.table import Table

console = Console()


class Command(BaseCommand):
    help = "Show current Google API configuration"

    def handle(self, *args, **kwargs):
        console.print("[bold blue]Google API Configuration[/bold blue]")
        console.print("=" * 40)
        
        # Create configuration table
        table = Table(title="Current Settings")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")
        table.add_column("Status", style="yellow")
        
        # Check credentials file
        creds_file = settings.GOOGLE_CREDENTIALS_FILE
        creds_exists = os.path.exists(creds_file)
        creds_status = "✓ Found" if creds_exists else "✗ Missing"
        
        # Check token file  
        token_file = settings.GOOGLE_TOKEN_FILE
        token_exists = os.path.exists(token_file)
        token_status = "✓ Found" if token_exists else "✗ Not created yet"
        
        table.add_row("GOOGLE_CREDENTIALS_FILE", creds_file, creds_status)
        table.add_row("GOOGLE_TOKEN_FILE", token_file, token_status)
        
        console.print(table)
        
        # Show file paths
        console.print(f"\n[bold]File Paths:[/bold]")
        console.print(f"  Credentials: {os.path.abspath(creds_file)}")
        console.print(f"  Token: {os.path.abspath(token_file)}")
        
        # Show status and next steps
        if not creds_exists:
            console.print(f"\n[bold red]⚠️  Credentials file not found![/bold red]")
            console.print("To fix this:")
            console.print("1. Download OAuth credentials from Google Cloud Console")
            console.print(f"2. Save as: {creds_file}")
            console.print("3. Or update GOOGLE_CREDENTIALS_FILE in your .env file")
        else:
            console.print(f"\n[bold green]✓ Credentials file found![/bold green]")
            if not token_exists:
                console.print("Token file will be created on first authentication.")
            else:
                console.print("✓ Token file exists - authentication should work.")
        
        # Show environment info
        console.print(f"\n[dim]Environment variables can be set in .env file[/dim]")
        console.print(f"[dim]See CONFIGURATION.md for detailed setup instructions[/dim]") 