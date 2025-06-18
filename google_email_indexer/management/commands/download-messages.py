from django.core.management.base import BaseCommand
from rich import print
from rich.console import Console
from rich.table import Table

from google_email_indexer.models import GoogleMailMessage
from google_email_indexer.message_sync_service import MessageSyncService

console = Console()


class Command(BaseCommand):
    help = "Efficiently sync Gmail messages using incremental updates when possible"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sync_service = None  # Lazy initialization

    @property
    def sync_service(self):
        """Lazy initialization of the sync service."""
        if self._sync_service is None:
            self._sync_service = MessageSyncService()
        return self._sync_service

    def add_arguments(self, parser):
        # Gmail account email override
        parser.add_argument(
            "--email", "--account-email",
            type=str,
            help="Gmail account email to sync (overrides GOOGLE_USER_EMAIL setting)"
        )
        
        # Max results to return for full sync
        parser.add_argument(
            "--max-results", "-m", 
            type=int, 
            default=100, 
            help="Max results to download (only used for full sync)"
        )
        
        # Force full sync instead of incremental
        parser.add_argument(
            "--force-full", "-f", 
            action="store_true", 
            help="Force full sync instead of incremental sync"
        )
        
        # Specific message ID to re-sync
        parser.add_argument(
            "--message-id", 
            type=str, 
            help="Resync a specific message by ID"
        )
        
        # Show detailed statistics
        parser.add_argument(
            "--verbose", "--detailed", 
            action="store_true", 
            help="Show detailed sync statistics"
        )
        
        # Label filtering options
        parser.add_argument(
            "--label-ids", 
            nargs="+", 
            help="Filter by Gmail label IDs (space-separated list)"
        )
        
        parser.add_argument(
            "--label-names", 
            nargs="+", 
            help="Filter by Gmail label names (space-separated list, case-insensitive)"
        )
        
        # List available labels
        parser.add_argument(
            "--list-labels", 
            action="store_true", 
            help="List all available labels and exit"
        )

    def handle(self, *args, **kwargs):
        account_email = kwargs["email"]
        max_results = kwargs["max_results"]
        force_full = kwargs["force_full"]
        message_id = kwargs["message_id"]
        verbose = kwargs["verbose"]
        label_ids = kwargs["label_ids"]
        label_names = kwargs["label_names"]
        list_labels = kwargs["list_labels"]
        
        console.print("[bold blue]Gmail Message Sync Service[/bold blue]")
        console.print("=" * 50)
        
        # Create sync service with account email override if provided
        if account_email:
            from google_email_indexer.service import GoogleEmailService
            gmail_service = GoogleEmailService(user_email=account_email)
            sync_service = MessageSyncService(gmail_service=gmail_service, account_email_override=account_email)
            console.print(f"[dim]Using account: {account_email}[/dim]")
        else:
            sync_service = self.sync_service
        
        # Handle list labels command
        if list_labels:
            return self._handle_list_labels(sync_service)
        
        # Handle specific message resync
        if message_id:
            return self._handle_single_message_sync(message_id, sync_service)
        
        # Show label filter info
        if label_ids or label_names:
            self._display_label_filter_info(label_ids, label_names)
        
        # Perform regular sync
        try:
            sync_result = sync_service.sync_messages(
                max_results=max_results,
                force_full_sync=force_full,
                label_ids=label_ids,
                label_names=label_names
            )
            
            self._display_sync_results(sync_result, verbose)
            
        except Exception as e:
            console.print(f"[bold red]Sync failed:[/bold red] {e}")
            self.stderr.write(f"Error: {e}")

    def _handle_single_message_sync(self, message_id: str, sync_service):
        """Handle resyncing a specific message."""
        console.print(f"[yellow]Resyncing message:[/yellow] {message_id}")
        
        try:
            message = sync_service.force_resync_message(message_id)
            
            console.print(f"[green]✓ Successfully synced message:[/green]")
            console.print(f"  ID: {message.message_id}")
            console.print(f"  From: {message.header_from}")
            console.print(f"  Subject: {message.header_subject or '(No Subject)'}")
            console.print(f"  Date: {message.internal_date}")
            console.print(f"  Labels: {message.label_ids}")
            
        except Exception as e:
            console.print(f"[bold red]Failed to sync message {message_id}:[/bold red] {e}")

    def _display_sync_results(self, sync_result: dict, verbose: bool):
        """Display sync results in a formatted way."""
        sync_type = sync_result.get('sync_type', 'unknown')
        label_filter = sync_result.get('label_filter')
        
        # Create title with label filter info
        title = f"Sync Results - {sync_type.title()} Sync"
        if label_filter:
            title += f" (Filtered by labels)"
        
        # Create summary table
        table = Table(title=title)
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        
        if sync_type == 'full':
            table.add_row("Total Found", str(sync_result.get('total_found', 0)))
            table.add_row("New Messages", str(sync_result.get('new_messages', 0)))
            table.add_row("Updated Messages", str(sync_result.get('updated_messages', 0)))
        else:  # incremental
            table.add_row("History Records", str(sync_result.get('history_records', 0)))
            table.add_row("Messages Added", str(sync_result.get('messages_added', 0)))
            table.add_row("Messages Deleted", str(sync_result.get('messages_deleted', 0)))
            table.add_row("Labels Modified", str(sync_result.get('labels_modified', 0)))
        
        # Add common metrics
        errors = sync_result.get('errors', [])
        table.add_row("Errors", str(len(errors)), style="red" if errors else "green")
        
        console.print(table)
        
        # Show label filter details
        if label_filter:
            console.print(f"[dim]Applied label filter: {', '.join(label_filter)}[/dim]")
        
        # Show history ID info
        history_id = sync_result.get('history_id')
        if history_id:
            console.print(f"[dim]Latest History ID: {history_id}[/dim]")
        
        # Show errors if any
        if errors and verbose:
            console.print("\n[bold red]Errors:[/bold red]")
            for error in errors[:10]:  # Limit to first 10 errors
                console.print(f"  • {error}")
            if len(errors) > 10:
                console.print(f"  ... and {len(errors) - 10} more errors")
        
        # Show database summary
        total_messages = GoogleMailMessage.objects.count()
        console.print(f"\n[dim]Total messages in database: {total_messages}[/dim]")
        
        # Display recent messages summary
        self._display_recent_messages()

    def _display_recent_messages(self):
        """Display a summary of recent messages."""
        recent_messages = GoogleMailMessage.objects.order_by('-internal_date')[:5]
        
        if recent_messages:
            console.print("\n[bold]Recent Messages:[/bold]")
            
            for msg in recent_messages:
                # Format the date
                try:
                    from datetime import datetime
                    from django.utils import timezone
                    date_str = timezone.make_aware(datetime.fromtimestamp(int(msg.internal_date) / 1000)).strftime("%Y-%m-%d %H:%M")
                except:
                    date_str = "Unknown"
                
                # Truncate subject
                subject = msg.header_subject or "(No Subject)"
                if len(subject) > 50:
                    subject = subject[:47] + "..."
                
                # Format sender
                sender = str(msg.header_from)
                if len(sender) > 30:
                    sender = sender[:27] + "..."
                
                console.print(f"  • {date_str} | {sender} | {subject}")
                
                # Show labels if verbose
                if msg.label_ids:
                    important_labels = [l for l in msg.label_ids if l in ['STARRED', 'IMPORTANT', 'UNREAD']]
                    if important_labels:
                        console.print(f"    [dim]Labels: {', '.join(important_labels)}[/dim]")

    def _handle_list_labels(self, sync_service):
        """Handle the list-labels command."""
        console.print("[bold]Available Labels:[/bold]")
        
        try:
            labels = sync_service.list_available_labels()
            
            # Create table for labels
            table = Table(title="Gmail Labels")
            table.add_column("Name", style="cyan")
            table.add_column("ID", style="dim")
            table.add_column("Type", style="green")
            table.add_column("Messages", justify="right", style="yellow")
            table.add_column("Unread", justify="right", style="red")
            
            # Sort labels: system labels first, then user labels alphabetically
            system_labels = [l for l in labels if l.get('category') == 'System']
            user_labels = [l for l in labels if l.get('category') == 'User']
            system_labels.sort(key=lambda x: x.get('name', ''))
            user_labels.sort(key=lambda x: x.get('name', ''))
            
            for label in system_labels + user_labels:
                name = label.get('name', 'Unknown')
                label_id = label.get('id', 'Unknown')
                category = label.get('category', 'Unknown')
                
                # Message counts (may not be available for all labels)
                messages_total = label.get('messagesTotal', 'N/A')
                messages_unread = label.get('messagesUnread', 'N/A')
                
                table.add_row(
                    name,
                    label_id,
                    category,
                    str(messages_total),
                    str(messages_unread)
                )
            
            console.print(table)
            
            # Show usage examples
            console.print("\n[bold]Usage Examples:[/bold]")
            console.print("  # Sync only starred messages:")
            console.print("  ./manage.py download-messages --label-names STARRED")
            console.print("  # Sync unread messages in inbox:")
            console.print("  ./manage.py download-messages --label-names INBOX UNREAD")
            console.print("  # Sync by label ID:")
            console.print("  ./manage.py download-messages --label-ids Label_123")
            
        except Exception as e:
            console.print(f"[bold red]Failed to list labels:[/bold red] {e}")

    def _display_label_filter_info(self, label_ids, label_names):
        """Display label filter information."""
        console.print("[bold]Label Filtering:[/bold]")
        if label_ids:
            console.print(f"  - Filtering by label IDs: {', '.join(label_ids)}")
        if label_names:
            console.print(f"  - Filtering by label names: {', '.join(label_names)}")
        console.print()  # Add spacing
