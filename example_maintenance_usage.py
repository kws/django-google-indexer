#!/usr/bin/env python
"""
Example script demonstrating how to use EmailIndexingService for periodic maintenance.

This script can be used as a template for setting up automated maintenance tasks
via cron jobs, Celery tasks, or other scheduling mechanisms.
"""

import os
import sys
import django
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'your_project.settings')
django.setup()

from google_email_indexer.email_indexing_service import EmailIndexingService


def run_daily_maintenance(account_email=None, verbose=True):
    """
    Run daily maintenance tasks on the email index.
    
    Args:
        account_email: Optional account email to limit operations to
        verbose: Whether to print detailed output
    """
    if verbose:
        print(f"Starting daily maintenance at {datetime.now()}")
    
    try:
        # Step 1: Validate the index
        if verbose:
            print("1. Validating email index...")
        
        validation_results = EmailIndexingService.validate_index(account_email)
        
        if verbose:
            print(f"   Total messages: {validation_results['total_messages']}")
            print(f"   Missing messages: {validation_results['missing_messages']}")
            print(f"   Orphaned emails: {validation_results['orphaned_emails']}")
            print(f"   Inconsistent counts: {validation_results['inconsistent_counts']}")
        
        # Step 2: Fix missing entries if any found
        if validation_results['missing_messages'] > 0:
            if verbose:
                print(f"2. Fixing {validation_results['missing_messages']} missing entries...")
            
            fix_results = EmailIndexingService.fix_missing_entries(
                account_email=account_email,
                batch_size=1000
            )
            
            if verbose:
                print(f"   Processed: {fix_results['processed_count']} messages")
                print(f"   Errors: {fix_results['error_count']} messages")
        else:
            if verbose:
                print("2. No missing entries to fix")
        
        # Step 3: Clean up orphaned entries
        if validation_results['orphaned_emails'] > 0:
            if verbose:
                print(f"3. Cleaning up {validation_results['orphaned_emails']} orphaned emails...")
            
            cleanup_results = EmailIndexingService.maintenance_cleanup()
            
            if verbose:
                print(f"   Removed: {cleanup_results['orphaned_emails_removed']} orphaned emails")
        else:
            if verbose:
                print("3. No orphaned emails to clean up")
        
        # Step 4: Get final statistics
        if verbose:
            print("4. Getting final statistics...")
        
        stats = EmailIndexingService.get_index_statistics(account_email)
        
        if verbose:
            print(f"   Final state: {stats['total_messages']} messages, {stats['total_indexed_emails']} emails")
            print("   Maintenance completed successfully!")
        
        return {
            'success': True,
            'validation': validation_results,
            'statistics': stats
        }
        
    except Exception as e:
        if verbose:
            print(f"Error during maintenance: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def run_weekly_maintenance(account_email=None, verbose=True):
    """
    Run weekly maintenance tasks (more comprehensive than daily).
    
    Args:
        account_email: Optional account email to limit operations to
        verbose: Whether to print detailed output
    """
    if verbose:
        print(f"Starting weekly maintenance at {datetime.now()}")
    
    try:
        # Run full maintenance cleanup
        if verbose:
            print("Running comprehensive maintenance cleanup...")
        
        cleanup_results = EmailIndexingService.maintenance_cleanup()
        
        if verbose:
            print(f"   Orphaned emails removed: {cleanup_results['orphaned_emails_removed']}")
            print(f"   Message counts updated: {cleanup_results['message_counts_updated']}")
        
        # Get comprehensive statistics
        stats = EmailIndexingService.get_index_statistics(account_email)
        
        if verbose:
            print("\nWeekly Statistics:")
            print(f"   Total messages: {stats['total_messages']}")
            print(f"   Indexed emails: {stats['total_indexed_emails']}")
            print(f"   Total relationships: {stats['total_relationships']}")
            
            if stats['field_distribution']:
                print("   Field distribution:")
                for field, count in stats['field_distribution'].items():
                    print(f"     {field}: {count}")
            
            if stats['top_email_addresses']:
                print("   Top 5 email addresses:")
                for email_info in stats['top_email_addresses'][:5]:
                    print(f"     {email_info['email']}: {email_info['message_count']} messages")
        
        return {
            'success': True,
            'cleanup': cleanup_results,
            'statistics': stats
        }
        
    except Exception as e:
        if verbose:
            print(f"Error during weekly maintenance: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def check_index_health(account_email=None):
    """
    Quick health check of the email index.
    
    Args:
        account_email: Optional account email to limit check to
        
    Returns:
        Dictionary with health status and recommendations
    """
    try:
        validation_results = EmailIndexingService.validate_index(account_email)
        stats = EmailIndexingService.get_index_statistics(account_email)
        
        # Determine health status
        if validation_results['total_issues'] == 0:
            health_status = 'healthy'
        elif validation_results['total_issues'] < 10:
            health_status = 'warning'
        else:
            health_status = 'critical'
        
        # Generate recommendations
        recommendations = []
        if validation_results['missing_messages'] > 0:
            recommendations.append(f"Run fix_missing_entries to repair {validation_results['missing_messages']} missing entries")
        if validation_results['orphaned_emails'] > 0:
            recommendations.append(f"Run maintenance_cleanup to remove {validation_results['orphaned_emails']} orphaned emails")
        if validation_results['inconsistent_counts'] > 0:
            recommendations.append(f"Run maintenance_cleanup to fix {validation_results['inconsistent_counts']} inconsistent counts")
        
        return {
            'health_status': health_status,
            'total_issues': validation_results['total_issues'],
            'total_messages': stats['total_messages'],
            'total_emails': stats['total_indexed_emails'],
            'recommendations': recommendations
        }
        
    except Exception as e:
        return {
            'health_status': 'error',
            'error': str(e)
        }


if __name__ == '__main__':
    # Example usage
    
    # Quick health check
    print("=== Email Index Health Check ===")
    health = check_index_health()
    print(f"Health Status: {health['health_status']}")
    if 'total_issues' in health:
        print(f"Total Issues: {health['total_issues']}")
        print(f"Total Messages: {health['total_messages']}")
        print(f"Total Emails: {health['total_emails']}")
        if health['recommendations']:
            print("Recommendations:")
            for rec in health['recommendations']:
                print(f"  - {rec}")
    
    print("\n" + "="*50 + "\n")
    
    # Run daily maintenance
    print("=== Running Daily Maintenance ===")
    daily_results = run_daily_maintenance(verbose=True)
    
    if daily_results['success']:
        print("Daily maintenance completed successfully!")
    else:
        print(f"Daily maintenance failed: {daily_results['error']}")
    
    print("\n" + "="*50 + "\n")
    
    # Run weekly maintenance (uncomment to run)
    # print("=== Running Weekly Maintenance ===")
    # weekly_results = run_weekly_maintenance(verbose=True)
    # 
    # if weekly_results['success']:
    #     print("Weekly maintenance completed successfully!")
    # else:
    #     print(f"Weekly maintenance failed: {weekly_results['error']}") 