import logging
from django.core.management.base import BaseCommand
from google_email_indexer.email_indexing_service import EmailIndexingService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Perform periodic maintenance on the email index'

    def add_arguments(self, parser):
        parser.add_argument(
            '--validate-only',
            action='store_true',
            help='Only validate the index without making any changes'
        )
        parser.add_argument(
            '--fix-missing',
            action='store_true',
            help='Fix missing index entries'
        )
        parser.add_argument(
            '--cleanup',
            action='store_true',
            help='Clean up orphaned email addresses'
        )
        parser.add_argument(
            '--full-maintenance',
            action='store_true',
            help='Perform all maintenance operations (validate, fix, cleanup)'
        )
        parser.add_argument(
            '--account-email',
            type=str,
            help='Only process messages for this specific account email'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of messages to process in each batch (default: 1000)'
        )
        parser.add_argument(
            '--quiet',
            action='store_true',
            help='Suppress output (useful for cron jobs)'
        )

    def handle(self, *args, **options):
        validate_only = options['validate_only']
        fix_missing = options['fix_missing']
        cleanup = options['cleanup']
        full_maintenance = options['full_maintenance']
        account_email = options.get('account_email')
        batch_size = options['batch_size']
        quiet = options['quiet']

        if not any([validate_only, fix_missing, cleanup, full_maintenance]):
            self.stdout.write(
                self.style.ERROR(
                    'Please specify at least one operation: --validate-only, --fix-missing, '
                    '--cleanup, or --full-maintenance'
                )
            )
            return

        # Set up logging level based on quiet flag
        if quiet:
            logging.getLogger().setLevel(logging.ERROR)
        else:
            logging.getLogger().setLevel(logging.INFO)

        results = {}

        # Validate index
        if validate_only or full_maintenance:
            if not quiet:
                self.stdout.write('Validating email index...')
            
            validation_results = EmailIndexingService.validate_index(account_email)
            results['validation'] = validation_results
            
            if not quiet:
                self._print_validation_results(validation_results)

        # Fix missing entries
        if fix_missing or full_maintenance:
            if not quiet:
                self.stdout.write('Fixing missing index entries...')
            
            fix_results = EmailIndexingService.fix_missing_entries(
                account_email=account_email,
                batch_size=batch_size
            )
            results['fix_missing'] = fix_results
            
            if not quiet:
                self._print_fix_results(fix_results)

        # Cleanup orphaned entries
        if cleanup or full_maintenance:
            if not quiet:
                self.stdout.write('Cleaning up orphaned email addresses...')
            
            cleanup_results = EmailIndexingService.maintenance_cleanup()
            results['cleanup'] = cleanup_results
            
            if not quiet:
                self._print_cleanup_results(cleanup_results)

        # Get final statistics
        if not quiet:
            self.stdout.write('Getting final index statistics...')
        
        stats = EmailIndexingService.get_index_statistics(account_email)
        results['statistics'] = stats
        
        if not quiet:
            self._print_statistics(stats)

        # Summary
        if not quiet:
            self._print_summary(results)

    def _print_validation_results(self, results):
        """Print validation results"""
        self.stdout.write(f'Total messages: {results["total_messages"]}')
        self.stdout.write(f'Missing messages: {results["missing_messages"]}')
        self.stdout.write(f'Orphaned emails: {results["orphaned_emails"]}')
        self.stdout.write(f'Inconsistent counts: {results["inconsistent_counts"]}')
        
        if results['is_valid']:
            self.stdout.write(
                self.style.SUCCESS('✅ Index validation passed - no issues found!')
            )
        else:
            self.stdout.write(
                self.style.WARNING(
                    f'⚠️  Index validation found {results["total_issues"]} issues'
                )
            )

    def _print_fix_results(self, results):
        """Print fix missing results"""
        if results['missing_count'] == 0:
            self.stdout.write(
                self.style.SUCCESS('No missing entries to fix!')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f'Fixed {results["processed_count"]} messages '
                    f'({results["error_count"]} errors)'
                )
            )

    def _print_cleanup_results(self, results):
        """Print cleanup results"""
        self.stdout.write(
            self.style.SUCCESS(
                f'Removed {results["orphaned_emails_removed"]} orphaned email addresses'
            )
        )

    def _print_statistics(self, stats):
        """Print index statistics"""
        self.stdout.write('\nIndex Statistics:')
        self.stdout.write(f'  Total messages: {stats["total_messages"]}')
        self.stdout.write(f'  Indexed emails: {stats["total_indexed_emails"]}')
        self.stdout.write(f'  Total relationships: {stats["total_relationships"]}')
        
        if stats['field_distribution']:
            self.stdout.write('  Field distribution:')
            for field, count in stats['field_distribution'].items():
                self.stdout.write(f'    {field}: {count}')
        
        if stats['top_email_addresses']:
            self.stdout.write('  Top email addresses:')
            for email_info in stats['top_email_addresses'][:5]:  # Show top 5
                self.stdout.write(
                    f'    {email_info["email"]}: {email_info["message_count"]} messages'
                )

    def _print_summary(self, results):
        """Print maintenance summary"""
        self.stdout.write('\n' + '='*50)
        self.stdout.write('MAINTENANCE SUMMARY')
        self.stdout.write('='*50)
        
        if 'validation' in results:
            validation = results['validation']
            self.stdout.write(f'Validation: {validation["total_issues"]} issues found')
        
        if 'fix_missing' in results:
            fix = results['fix_missing']
            self.stdout.write(f'Fixed: {fix["processed_count"]} messages processed')
        
        if 'cleanup' in results:
            cleanup = results['cleanup']
            self.stdout.write(f'Cleanup: {cleanup["orphaned_emails_removed"]} orphaned emails removed')
        
        if 'statistics' in results:
            stats = results['statistics']
            self.stdout.write(f'Final state: {stats["total_messages"]} messages, {stats["total_indexed_emails"]} emails')
        
        self.stdout.write('='*50) 