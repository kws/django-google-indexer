import logging
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q, Count, F
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
            '--validate',
            action='store_true',
            help='Check for missing or outdated index entries without fixing them'
        )
        parser.add_argument(
            '--fix-missing',
            action='store_true',
            help='Index only messages that are missing from the index'
        )
        parser.add_argument(
            '--cleanup',
            action='store_true',
            help='Remove orphaned email addresses that have no associated messages'
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
        validate = options['validate']
        fix_missing = options['fix_missing']
        cleanup = options['cleanup']
        verbose = options['verbose']

        if verbose:
            logging.basicConfig(level=logging.INFO)

        # Validate index if requested
        if validate:
            self._validate_index(account_email, verbose)
            return

        # Fix missing entries if requested
        if fix_missing:
            self._fix_missing_entries(account_email, batch_size, verbose)
            return

        # Cleanup orphaned entries if requested
        if cleanup:
            self._cleanup_orphaned_entries(verbose)
            return

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

    def _validate_index(self, account_email, verbose):
        """Check for missing or outdated index entries"""
        self.stdout.write('Validating email index...')
        
        # Get base queryset
        messages_queryset = GoogleMailMessage.objects.all()
        if account_email:
            messages_queryset = messages_queryset.filter(account_email=account_email)
        
        total_messages = messages_queryset.count()
        self.stdout.write(f'Total messages in database: {total_messages}')
        
        # Find messages that have no email relationships
        messages_without_index = messages_queryset.exclude(
            id__in=MessageEmailAddress.objects.values('message_id')
        )
        
        missing_count = messages_without_index.count()
        self.stdout.write(f'Messages missing from index: {missing_count}')
        
        if missing_count > 0 and verbose:
            self.stdout.write('\nSample messages missing from index:')
            for msg in messages_without_index[:10]:
                self.stdout.write(f'  - {msg.message_id} ({msg.account_email})')
            if missing_count > 10:
                self.stdout.write(f'  ... and {missing_count - 10} more')
        
        # Find orphaned index entries (email addresses with no messages)
        orphaned_emails = IndexedEmailAddress.objects.annotate(
            message_count_actual=Count('messages')
        ).filter(message_count_actual=0)
        
        orphaned_count = orphaned_emails.count()
        self.stdout.write(f'Orphaned email addresses: {orphaned_count}')
        
        if orphaned_count > 0 and verbose:
            self.stdout.write('\nSample orphaned email addresses:')
            for email in orphaned_emails[:10]:
                self.stdout.write(f'  - {email.email}')
            if orphaned_count > 10:
                self.stdout.write(f'  ... and {orphaned_count - 10} more')
        
        # Check for inconsistent message counts
        inconsistent_emails = IndexedEmailAddress.objects.annotate(
            message_count_actual=Count('messages')
        ).filter(~Q(message_count=F('message_count_actual')))
        
        inconsistent_count = inconsistent_emails.count()
        self.stdout.write(f'Email addresses with inconsistent counts: {inconsistent_count}')
        
        if inconsistent_count > 0 and verbose:
            self.stdout.write('\nSample inconsistent email addresses:')
            for email in inconsistent_emails[:10]:
                actual_count = email.messages.count()
                self.stdout.write(f'  - {email.email}: stored={email.message_count}, actual={actual_count}')
            if inconsistent_count > 10:
                self.stdout.write(f'  ... and {inconsistent_count - 10} more')
        
        # Summary
        total_issues = missing_count + orphaned_count + inconsistent_count
        if total_issues == 0:
            self.stdout.write(
                self.style.SUCCESS('\n✅ Index validation passed - no issues found!')
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    f'\n⚠️  Index validation found {total_issues} issues:\n'
                    f'  - {missing_count} messages missing from index\n'
                    f'  - {orphaned_count} orphaned email addresses\n'
                    f'  - {inconsistent_count} inconsistent message counts\n\n'
                    f'Run with --fix-missing to repair missing entries.'
                )
            )

    def _fix_missing_entries(self, account_email, batch_size, verbose):
        """Index only messages that are missing from the index"""
        self.stdout.write('Fixing missing index entries...')
        
        # Get base queryset
        messages_queryset = GoogleMailMessage.objects.all()
        if account_email:
            messages_queryset = messages_queryset.filter(account_email=account_email)
        
        # Find messages that have no email relationships
        missing_messages = messages_queryset.exclude(
            id__in=MessageEmailAddress.objects.values('message_id')
        )
        
        missing_count = missing_messages.count()
        self.stdout.write(f'Found {missing_count} messages missing from index')
        
        if missing_count == 0:
            self.stdout.write(
                self.style.SUCCESS('No missing entries to fix!')
            )
            return
        
        # Define progress callback for verbose output
        def progress_callback(batch_start, batch_end, total):
            self.stdout.write(f'Processing batch {batch_start + 1}-{batch_end} of {total}')
        
        # Use the service to process only missing messages
        processed_count, error_count = EmailIndexingService.bulk_index_messages(
            missing_messages,
            batch_size=batch_size,
            progress_callback=progress_callback if verbose else None
        )
        
        # Update message counts for all email addresses to fix inconsistencies
        if verbose:
            self.stdout.write('Updating message counts...')
        EmailIndexingService.update_all_message_counts()
        
        # Display results
        self.stdout.write(
            self.style.SUCCESS(
                f'\nMissing entries fixed!\n'
                f'Processed: {processed_count} messages\n'
                f'Errors: {error_count} messages'
            )
        )

    def _cleanup_orphaned_entries(self, verbose):
        """Remove orphaned email addresses that have no associated messages"""
        self.stdout.write('Cleaning up orphaned email addresses...')
        
        # Find orphaned index entries (email addresses with no messages)
        orphaned_emails = IndexedEmailAddress.objects.annotate(
            message_count_actual=Count('messages')
        ).filter(message_count_actual=0)
        
        orphaned_count = orphaned_emails.count()
        self.stdout.write(f'Found {orphaned_count} orphaned email addresses')
        
        if orphaned_count > 0 and verbose:
            self.stdout.write('\nSample orphaned email addresses:')
            for email in orphaned_emails[:10]:
                self.stdout.write(f'  - {email.email}')
            if orphaned_count > 10:
                self.stdout.write(f'  ... and {orphaned_count - 10} more')
        
        # Remove orphaned email addresses
        orphaned_emails.delete()
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\nOrphaned email addresses cleaned up!\n'
                f'Removed: {orphaned_count} orphaned email addresses'
            )
        )

 