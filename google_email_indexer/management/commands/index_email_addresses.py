import logging
from django.core.management.base import BaseCommand, CommandError
from google_email_indexer.models import GoogleMailMessage, IndexedEmailAddress, MessageEmailAddress
from google_email_indexer.email_indexing_service import EmailIndexingService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Index email addresses from all Gmail messages'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of messages to process in each batch (default: 1000)'
        )
        parser.add_argument(
            '--account-email',
            type=str,
            help='Only process messages for this specific account email'
        )
        parser.add_argument(
            '--reindex',
            action='store_true',
            help='Clear existing email index and rebuild from scratch'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose output'
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        account_email = options.get('account_email')
        reindex = options['reindex']
        verbose = options['verbose']

        if verbose:
            logging.basicConfig(level=logging.INFO)

        self.stdout.write(
            self.style.SUCCESS('Starting email address indexing...')
        )

        # Clear existing index if reindexing
        if reindex:
            self.stdout.write('Clearing existing email index...')
            EmailIndexingService.clear_index()
            self.stdout.write(
                self.style.SUCCESS('Cleared existing email index')
            )

        # Get queryset of messages to process
        queryset = GoogleMailMessage.objects.all()
        if account_email:
            queryset = queryset.filter(account_email=account_email)

        total_messages = queryset.count()
        self.stdout.write(f'Found {total_messages} messages to process')

        if total_messages == 0:
            self.stdout.write(
                self.style.WARNING('No messages found to process')
            )
            return

        # Define progress callback for verbose output
        def progress_callback(batch_start, batch_end, total):
            self.stdout.write(f'Processing batch {batch_start + 1}-{batch_end} of {total}')

        # Use the service to process messages
        processed_count, error_count = EmailIndexingService.bulk_index_messages(
            queryset,
            batch_size=batch_size,
            progress_callback=progress_callback if verbose else None
        )

        # Display results
        total_indexed_emails = IndexedEmailAddress.objects.count()
        total_relationships = MessageEmailAddress.objects.count()
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\nEmail indexing completed!\n'
                f'Processed: {processed_count} messages\n'
                f'Errors: {error_count} messages\n'
                f'Indexed emails: {total_indexed_emails} unique addresses\n'
                f'Total relationships: {total_relationships}'
            )
        )

 