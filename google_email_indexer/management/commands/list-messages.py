from django.core.management.base import BaseCommand
from rich import print
from rich.console import Console
from rich.table import Table
from datetime import datetime
from django.utils import timezone

from google_email_indexer.models import GoogleMailMessage

console = Console()


class Command(BaseCommand):
    help = "Display the last 'n' messages in a nice rich table format"

    def add_arguments(self, parser):
        parser.add_argument(
            "--count", "-n",
            type=int,
            default=10,
            help="Number of messages to display (default: 10)"
        )
        
        parser.add_argument(
            "--account-email",
            type=str,
            help="Filter by specific Gmail account email"
        )
        
        parser.add_argument(
            "--unread-only",
            action="store_true",
            help="Show only unread messages"
        )
        
        parser.add_argument(
            "--starred-only",
            action="store_true",
            help="Show only starred messages"
        )

    def handle(self, *args, **kwargs):
        count = kwargs["count"]
        account_email = kwargs["account_email"]
        unread_only = kwargs["unread_only"]
        starred_only = kwargs["starred_only"]
        
        console.print(f"[bold blue]Last {count} Messages[/bold blue]")
        console.print("=" * 60)
        
        # Build the queryset
        queryset = GoogleMailMessage.objects.all()
        
        # Apply filters
        if account_email:
            queryset = queryset.filter(account_email=account_email)
            console.print(f"[dim]Filtered by account: {account_email}[/dim]")
        
        if unread_only:
            queryset = queryset.filter(is_read=False)
            console.print("[dim]Showing unread messages only[/dim]")
        
        if starred_only:
            queryset = queryset.filter(is_starred=True)
            console.print("[dim]Showing starred messages only[/dim]")
        
        # Get the messages ordered by date (newest first)
        messages = queryset.order_by('-internal_date')[:count]
        
        if not messages:
            console.print("[yellow]No messages found matching the criteria.[/yellow]")
            return
        
        # Create the table
        table = Table(title=f"Last {len(messages)} Messages")
        table.add_column("Date", style="cyan", width=20)
        table.add_column("From", style="green", width=25)
        table.add_column("To", style="blue", width=25)
        table.add_column("CC", style="magenta", width=20)
        table.add_column("Subject", style="white", width=40)
        
        # Add rows to the table
        for msg in messages:
            # Format the date
            try:
                date_obj = timezone.make_aware(
                    datetime.fromtimestamp(int(msg.internal_date) / 1000)
                )
                date_str = date_obj.strftime("%Y-%m-%d %H:%M")
            except:
                date_str = "Unknown"
            
            # Format the from field
            from_str = self._format_email_field(msg.header_from)
            
            # Format the to field
            to_str = self._format_email_list(msg.header_to)
            
            # Format the cc field
            cc_str = self._format_email_list(msg.header_cc)
            
            # Format the subject
            subject = msg.header_subject or "(No Subject)"
            if len(subject) > 37:
                subject = subject[:34] + "..."
            
            # Add status indicators
            status_indicators = []
            if not msg.is_read:
                status_indicators.append("📧")
            if msg.is_starred:
                status_indicators.append("⭐")
            if msg.is_important:
                status_indicators.append("🔥")
            
            status_prefix = " ".join(status_indicators) + " " if status_indicators else ""
            
            table.add_row(
                date_str,
                from_str,
                to_str,
                cc_str,
                status_prefix + subject
            )
        
        console.print(table)
        
        # Show summary
        total_messages = GoogleMailMessage.objects.count()
        console.print(f"\n[dim]Total messages in database: {total_messages}[/dim]")
        
        if account_email:
            account_total = GoogleMailMessage.objects.filter(account_email=account_email).count()
            console.print(f"[dim]Messages for {account_email}: {account_total}[/dim]")

    def _format_email_field(self, email_obj):
        """Format a single email address object."""
        if not email_obj:
            return ""
        
        try:
            if hasattr(email_obj, 'name') and email_obj.name:
                return f"{email_obj.name} <{email_obj.email}>"
            else:
                return email_obj.email
        except:
            return str(email_obj)

    def _format_email_list(self, email_list):
        """Format a list of email addresses."""
        if not email_list:
            return ""
        
        try:
            formatted_emails = []
            for email_obj in email_list:
                formatted = self._format_email_field(email_obj)
                if formatted:
                    formatted_emails.append(formatted)
            
            result = ", ".join(formatted_emails)
            if len(result) > 22:
                return result[:19] + "..."
            return result
        except:
            return str(email_list) 