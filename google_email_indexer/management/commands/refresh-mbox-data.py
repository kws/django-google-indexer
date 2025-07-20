from django.core.management.base import BaseCommand
from rich.console import Console
from google_email_indexer.models import GoogleMailMessage
from tqdm import tqdm

console = Console()


class Command(BaseCommand):
    help = "Refresh mbox data with optional full field update"

    def add_arguments(self, parser):
        parser.add_argument(
            "-a", "--all",
            action="store_true",
            help="Update all fields (not just changed ones)"
        )

    def handle(self, *args, **kwargs):
        update_all = kwargs["all"]
        
        console.print("[bold blue]Mbox Data Refresh[/bold blue]")
        console.print("=" * 30)
        
        if update_all:
            console.print("[yellow]Mode:[/yellow] Full update (all fields)")
            # Get all messages
            messages = GoogleMailMessage.objects.all()
        else:
            console.print("[yellow]Mode:[/yellow] Incremental update (changed fields only)")
            # Get only messages with missing values
            messages = GoogleMailMessage.objects.filter(original_message_id=None)
        
        updated_count = 0
        for message in tqdm(messages):
            mbox_message = message.mbox
            message.original_message_id = mbox_message.get("Message-ID")
            updated_count += 1
        
        # Use bulk_update with the list of updated model instances
        if updated_count > 0:
            GoogleMailMessage.objects.bulk_update(messages, fields=['original_message_id'])
            console.print(f"[green]✓ Updated {updated_count} messages[/green]")
        else:
            console.print("[yellow]No messages to update[/yellow]")
        
        console.print("[green]✓ Command completed successfully[/green]")
    
