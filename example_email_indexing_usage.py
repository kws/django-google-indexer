#!/usr/bin/env python
"""
Example script demonstrating how to use the email indexing features.

This script shows various ways to interact with the email index:
- Index messages automatically
- Search for email addresses
- Find messages by email address
- Get contact statistics

Run this script from your Django project directory after running migrations
and indexing your emails.
"""

import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'your_project.settings')
django.setup()

from google_email_indexer.models import GoogleMailMessage, IndexedEmailAddress
from google_email_indexer.email_indexing_service import EmailIndexingService


def demo_email_indexing():
    """Demonstrate email indexing functionality"""
    
    print("=== Email Indexing Demo ===\n")
    
    # 1. Get some statistics about the current index
    total_messages = GoogleMailMessage.objects.count()
    indexed_emails = IndexedEmailAddress.objects.count()
    
    print(f"Total messages in database: {total_messages}")
    print(f"Indexed email addresses: {indexed_emails}\n")
    
    if total_messages == 0:
        print("No messages found. Please import some messages first using:")
        print("python manage.py download-messages")
        return
    
    # 2. Index a single message (if not already indexed)
    sample_message = GoogleMailMessage.objects.first()
    if sample_message:
        print(f"Indexing sample message: {sample_message.message_id}")
        relationships_created = EmailIndexingService.index_message_emails(sample_message)
        print(f"Created {relationships_created} email relationships\n")
    
    # 3. Search for email addresses
    print("=== Email Address Search ===")
    search_patterns = ['gmail.com', 'example.com', '@']
    
    for pattern in search_patterns:
        results = EmailIndexingService.get_emails_for_contact(pattern, limit=5)
        print(f"Emails matching '{pattern}': {len(results)} found")
        for email in results[:3]:  # Show first 3
            print(f"  - {email.email} ({email.message_count} messages)")
        if len(results) > 3:
            print(f"  ... and {len(results) - 3} more")
        print()
    
    # 4. Get messages for a specific email address
    if indexed_emails > 0:
        print("=== Messages by Email Address ===")
        top_email = IndexedEmailAddress.objects.order_by('-message_count').first()
        if top_email:
            print(f"Getting messages for: {top_email.email}")
            
            # Get all messages
            all_messages = EmailIndexingService.get_messages_for_email(
                top_email.email, limit=5
            )
            print(f"Total messages: {len(all_messages)}")
            
            # Get only messages where this email is in the 'from' field
            from_messages = EmailIndexingService.get_messages_for_email(
                top_email.email, field_types=['from'], limit=5
            )
            print(f"Messages from this email: {len(from_messages)}")
            
            # Get only messages where this email is in the 'to' field
            to_messages = EmailIndexingService.get_messages_for_email(
                top_email.email, field_types=['to'], limit=5
            )
            print(f"Messages to this email: {len(to_messages)}")
            print()
    
    # 5. Get contact statistics
    if indexed_emails > 0:
        print("=== Contact Statistics ===")
        top_emails = IndexedEmailAddress.objects.order_by('-message_count')[:3]
        
        for email in top_emails:
            stats = EmailIndexingService.get_contact_statistics(email.email)
            print(f"Email: {stats['email']}")
            print(f"Display Name: {stats['display_name'] or 'N/A'}")
            print(f"Total Messages: {stats['total_messages']}")
            print(f"Field Distribution: {stats['field_counts']}")
            print(f"First Seen: {stats['first_seen']}")
            print(f"Last Seen: {stats['last_seen']}")
            print("-" * 50)
    
    # 6. Demonstrate bulk indexing (commented out for safety)
    print("=== Bulk Indexing Example ===")
    print("To index all messages, run:")
    print("python manage.py index_email_addresses")
    print("python manage.py index_email_addresses --account-email user@example.com")
    print("python manage.py index_email_addresses --reindex --verbose")


def demo_filtering_by_email():
    """Demonstrate how to filter messages by email addresses"""
    
    print("\n=== Advanced Filtering Examples ===\n")
    
    # Find all messages from a specific domain
    domain = 'gmail.com'
    domain_emails = IndexedEmailAddress.objects.filter(email__endswith=f'@{domain}')
    print(f"Email addresses from {domain}: {domain_emails.count()}")
    
    # Find messages where specific emails appear in different fields
    if domain_emails.exists():
        sample_email = domain_emails.first()
        
        # Messages where this email is the sender
        from_messages = sample_email.messages.filter(
            messageemailaddress__field='from'
        ).distinct()
        print(f"Messages from {sample_email.email}: {from_messages.count()}")
        
        # Messages where this email is a recipient
        to_messages = sample_email.messages.filter(
            messageemailaddress__field__in=['to', 'cc']
        ).distinct()
        print(f"Messages to {sample_email.email}: {to_messages.count()}")
    
    # Find the most active email addresses
    print(f"\nTop 5 most active email addresses:")
    top_emails = IndexedEmailAddress.objects.order_by('-message_count')[:5]
    for i, email in enumerate(top_emails, 1):
        print(f"{i}. {email.email} - {email.message_count} messages")


if __name__ == '__main__':
    try:
        demo_email_indexing()
        demo_filtering_by_email()
        
        print("\n=== Getting Started ===")
        print("1. Run migrations: python manage.py makemigrations && python manage.py migrate")
        print("2. Index your emails: python manage.py index_email_addresses")
        print("3. Use the EmailIndexingService in your code for advanced queries")
        print("4. Check the Django admin for a web interface to browse indexed emails")
        
    except Exception as e:
        print(f"Error running demo: {e}")
        print("Make sure you have run migrations and have some messages in your database.")
        sys.exit(1) 