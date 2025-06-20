import logging
from typing import List, Optional, Tuple
from django.db import transaction
from django.utils import timezone
from django.db.models import Count
from .models import GoogleMailMessage, IndexedEmailAddress, MessageEmailAddress
from email.header import decode_header
from datetime import datetime
from django.db.models import Q, F

logger = logging.getLogger(__name__)


class EmailIndexingService:
    """Service class for managing email address indexing operations"""

    @staticmethod
    def index_message_emails(message: GoogleMailMessage, update_counts: bool = True) -> int:
        """
        Index email addresses for a single message.
        
        Args:
            message: The GoogleMailMessage to index
            update_counts: Whether to update message counts for affected email addresses
            
        Returns:
            Number of email relationships created
        """
        try:
            with transaction.atomic():
                # Clear existing relationships for this message
                MessageEmailAddress.objects.filter(message=message).delete()
                
                relationships_created = 0
                
                # Map email fields to their values
                email_field_mapping = [
                    ('from', [message.header_from] if message.header_from else []),
                    ('to', message.header_to or []),
                    ('cc', message.header_cc or []),
                    # Note: Add bcc, reply_to etc. when available
                ]
                
                for field_name, email_list in email_field_mapping:
                    # Ensure email_list is always a list
                    if not isinstance(email_list, list):
                        email_list = [email_list] if email_list else []
                        
                    for email_addr in email_list:
                        if email_addr and hasattr(email_addr, 'email') and email_addr.email:
                            created = EmailIndexingService._create_email_relationship(
                                message, email_addr, field_name
                            )
                            if created:
                                relationships_created += 1
                
                # Update message counts for affected email addresses
                if update_counts:
                    EmailIndexingService._update_message_counts_for_message(message)
                
                return relationships_created
                
        except Exception as e:
            logger.error(f'Error indexing emails for message {message.message_id}: {e}')
            raise

    @staticmethod
    def decode_name(name_value):
        """Decode MIME-encoded names to readable text"""
        if not name_value:
            return ""
        
        try:
            # Decode the name if it's MIME encoded
            decoded_parts = decode_header(name_value)
            decoded_name = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_name += part.decode(encoding or 'utf-8', errors='replace')
                else:
                    decoded_name += part
            return decoded_name
        except Exception:
            # Fallback to original string if decoding fails
            return str(name_value)

    @staticmethod
    def _create_email_relationship(
        message: GoogleMailMessage, 
        email_addr, 
        field_name: str
    ) -> bool:
        """
        Create email address relationship for a message.
        
        Returns:
            True if a new relationship was created, False if it already existed
        """
        try:
            # Normalize email address
            normalized_email = email_addr.email.lower().strip()
            
            if not normalized_email:
                return False
            
            # Decode the display name if present
            decoded_name = EmailIndexingService.decode_name(email_addr.name) if email_addr.name else ''
                
            # Get or create the indexed email address
            indexed_email, _ = IndexedEmailAddress.objects.get_or_create(
                email=normalized_email,
                defaults={
                    'display_name': decoded_name,
                    'message_count': 0
                }
            )
            
            # Update display name if this one is better
            if decoded_name and not indexed_email.display_name:
                indexed_email.display_name = decoded_name
                indexed_email.save()
            
            # Create the relationship
            relationship, created = MessageEmailAddress.objects.get_or_create(
                message=message,
                email_address=indexed_email,
                field=field_name,
                defaults={'display_name': decoded_name}
            )
            
            return created
            
        except Exception as e:
            logger.error(f'Error creating email relationship for {email_addr}: {e}')
            raise

    @staticmethod
    def _update_message_counts_for_message(message: GoogleMailMessage):
        """Update message counts for all email addresses in a specific message"""
        try:
            for email_addr in message.email_addresses.all():
                email_addr.message_count = email_addr.messages.count()
                email_addr.save()
        except Exception as e:
            logger.error(f'Error updating message counts for message {message.message_id}: {e}')
            raise

    @staticmethod
    def bulk_index_messages(
        messages_queryset, 
        batch_size: int = 1000,
        progress_callback: Optional[callable] = None
    ) -> Tuple[int, int]:
        """
        Index email addresses for multiple messages in batches.
        
        Args:
            messages_queryset: QuerySet of GoogleMailMessage objects
            batch_size: Number of messages to process in each batch
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Tuple of (processed_count, error_count)
        """
        total_messages = messages_queryset.count()
        processed_count = 0
        error_count = 0
        
        for batch_start in range(0, total_messages, batch_size):
            batch_end = min(batch_start + batch_size, total_messages)
            batch_messages = messages_queryset[batch_start:batch_end]
            
            if progress_callback:
                progress_callback(batch_start, batch_end, total_messages)
            
            with transaction.atomic():
                for message in batch_messages:
                    try:
                        EmailIndexingService.index_message_emails(message, update_counts=False)
                        processed_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f'Error processing message {message.message_id}: {e}')
        
        # Update all message counts at the end for better performance
        EmailIndexingService.update_all_message_counts()
        
        return processed_count, error_count

    @staticmethod
    def update_all_message_counts():
        """Update message counts for all indexed email addresses"""
        try:
            for indexed_email in IndexedEmailAddress.objects.all():
                # Get all messages for this email address
                messages = indexed_email.messages.all()
                message_count = messages.count()
                
                if message_count > 0:
                    # Get the earliest and latest message dates
                    earliest_date = messages.order_by('internal_date').first().internal_date
                    latest_date = messages.order_by('-internal_date').first().internal_date
                    
                    # Convert to timezone-aware datetime
                    first_seen = timezone.make_aware(
                        datetime.fromtimestamp(earliest_date / 1000.0)
                    )
                    last_seen = timezone.make_aware(
                        datetime.fromtimestamp(latest_date / 1000.0)
                    )
                    
                    # Update the email address record
                    indexed_email.message_count = message_count
                    indexed_email.first_seen = first_seen
                    indexed_email.last_seen = last_seen
                    indexed_email.save()
        except Exception as e:
            logger.error(f'Error updating all message counts: {e}')
            raise

    @staticmethod
    def get_emails_for_contact(email_pattern: str, limit: int = 10) -> List[IndexedEmailAddress]:
        """
        Search for email addresses matching a pattern.
        
        Args:
            email_pattern: Email address or pattern to search for
            limit: Maximum number of results to return
            
        Returns:
            List of matching IndexedEmailAddress objects
        """
        normalized_pattern = email_pattern.lower().strip()
        
        return IndexedEmailAddress.objects.filter(
            email__icontains=normalized_pattern
        ).order_by('-message_count', 'email')[:limit]

    @staticmethod
    def get_messages_for_email(
        email_address: str, 
        field_types: Optional[List[str]] = None,
        limit: int = None
    ) -> List[GoogleMailMessage]:
        """
        Get messages containing a specific email address.
        
        Args:
            email_address: The email address to search for
            field_types: List of field types to filter by ('from', 'to', 'cc', etc.)
            limit: Maximum number of messages to return
            
        Returns:
            List of GoogleMailMessage objects
        """
        normalized_email = email_address.lower().strip()
        
        try:
            indexed_email = IndexedEmailAddress.objects.get(email=normalized_email)
        except IndexedEmailAddress.DoesNotExist:
            return []
        
        queryset = indexed_email.messages.all()
        
        if field_types:
            # Filter by specific field types
            message_ids = MessageEmailAddress.objects.filter(
                email_address=indexed_email,
                field__in=field_types
            ).values_list('message_id', flat=True)
            queryset = queryset.filter(id__in=message_ids)
        
        queryset = queryset.order_by('-internal_date')
        
        if limit:
            queryset = queryset[:limit]
            
        return list(queryset)

    @staticmethod
    def get_contact_statistics(email_address: str) -> dict:
        """
        Get statistics for a specific email address.
        
        Args:
            email_address: The email address to get statistics for
            
        Returns:
            Dictionary containing statistics
        """
        normalized_email = email_address.lower().strip()
        
        try:
            indexed_email = IndexedEmailAddress.objects.get(email=normalized_email)
        except IndexedEmailAddress.DoesNotExist:
            return {}
        
        # Get field type counts
        field_counts = {}
        for field_type, _ in MessageEmailAddress.FIELD_CHOICES:
            count = MessageEmailAddress.objects.filter(
                email_address=indexed_email,
                field=field_type
            ).count()
            if count > 0:
                field_counts[field_type] = count
        
        return {
            'email': indexed_email.email,
            'display_name': indexed_email.display_name,
            'total_messages': indexed_email.message_count,
            'field_counts': field_counts,
            'first_seen': indexed_email.first_seen,
            'last_seen': indexed_email.last_seen,
        }

    @staticmethod
    def clear_index():
        """Clear the entire email index"""
        MessageEmailAddress.objects.all().delete()
        IndexedEmailAddress.objects.all().delete()

    @staticmethod
    def rebuild_index(account_email: Optional[str] = None, batch_size: int = 1000):
        """
        Rebuild the entire email index.
        
        Args:
            account_email: If provided, only rebuild for this account
            batch_size: Batch size for processing
        """
        # Clear existing index
        EmailIndexingService.clear_index()
        
        # Get messages to process
        queryset = GoogleMailMessage.objects.all()
        if account_email:
            queryset = queryset.filter(account_email=account_email)
        
        # Rebuild index
        return EmailIndexingService.bulk_index_messages(queryset, batch_size)

    @staticmethod
    def cleanup_orphaned_emails() -> int:
        """
        Remove email addresses that have no associated messages.
        
        Returns:
            Number of orphaned email addresses removed
        """
        try:
            # Find email addresses with no messages
            orphaned_emails = IndexedEmailAddress.objects.annotate(
                message_count_actual=Count('messages')
            ).filter(message_count_actual=0)
            
            orphaned_count = orphaned_emails.count()
            
            if orphaned_count > 0:
                logger.info(f'Removing {orphaned_count} orphaned email addresses')
                orphaned_emails.delete()
            
            return orphaned_count
            
        except Exception as e:
            logger.error(f'Error cleaning up orphaned emails: {e}')
            raise

    @staticmethod
    def validate_index(account_email: Optional[str] = None) -> dict:
        """
        Check for missing or outdated index entries.
        
        Args:
            account_email: If provided, only validate for this account
            
        Returns:
            Dictionary containing validation results
        """
        try:
            # Get base queryset
            messages_queryset = GoogleMailMessage.objects.all()
            if account_email:
                messages_queryset = messages_queryset.filter(account_email=account_email)
            
            total_messages = messages_queryset.count()
            
            # Find messages that have no email relationships
            messages_without_index = messages_queryset.exclude(
                id__in=MessageEmailAddress.objects.values('message_id')
            )
            missing_count = messages_without_index.count()
            
            # Find orphaned index entries (email addresses with no messages)
            orphaned_emails = IndexedEmailAddress.objects.annotate(
                message_count_actual=Count('messages')
            ).filter(message_count_actual=0)
            orphaned_count = orphaned_emails.count()
            
            # Check for inconsistent message counts
            inconsistent_emails = IndexedEmailAddress.objects.annotate(
                message_count_actual=Count('messages')
            ).filter(~Q(message_count=F('message_count_actual')))
            inconsistent_count = inconsistent_emails.count()
            
            # Calculate total issues
            total_issues = missing_count + orphaned_count + inconsistent_count
            
            return {
                'total_messages': total_messages,
                'missing_messages': missing_count,
                'orphaned_emails': orphaned_count,
                'inconsistent_counts': inconsistent_count,
                'total_issues': total_issues,
                'is_valid': total_issues == 0
            }
            
        except Exception as e:
            logger.error(f'Error validating index: {e}')
            raise

    @staticmethod
    def fix_missing_entries(
        account_email: Optional[str] = None, 
        batch_size: int = 1000,
        progress_callback: Optional[callable] = None
    ) -> dict:
        """
        Index only messages that are missing from the index.
        
        Args:
            account_email: If provided, only fix for this account
            batch_size: Number of messages to process in each batch
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary containing results
        """
        try:
            # Get base queryset
            messages_queryset = GoogleMailMessage.objects.all()
            if account_email:
                messages_queryset = messages_queryset.filter(account_email=account_email)
            
            # Find messages that have no email relationships
            missing_messages = messages_queryset.exclude(
                id__in=MessageEmailAddress.objects.values('message_id')
            )
            
            missing_count = missing_messages.count()
            
            if missing_count == 0:
                return {
                    'processed_count': 0,
                    'error_count': 0,
                    'missing_count': 0
                }
            
            # Use the service to process only missing messages
            processed_count, error_count = EmailIndexingService.bulk_index_messages(
                missing_messages,
                batch_size=batch_size,
                progress_callback=progress_callback
            )
            
            # Update message counts for all email addresses to fix inconsistencies
            EmailIndexingService.update_all_message_counts()
            
            return {
                'processed_count': processed_count,
                'error_count': error_count,
                'missing_count': missing_count
            }
            
        except Exception as e:
            logger.error(f'Error fixing missing entries: {e}')
            raise

    @staticmethod
    def maintenance_cleanup() -> dict:
        """
        Perform comprehensive maintenance cleanup operations.
        
        Returns:
            Dictionary containing cleanup results
        """
        try:
            results = {}
            
            # Clean up orphaned email addresses
            orphaned_count = EmailIndexingService.cleanup_orphaned_emails()
            results['orphaned_emails_removed'] = orphaned_count
            
            # Update all message counts to fix inconsistencies
            EmailIndexingService.update_all_message_counts()
            results['message_counts_updated'] = True
            
            return results
            
        except Exception as e:
            logger.error(f'Error during maintenance cleanup: {e}')
            raise

    @staticmethod
    def get_index_statistics(account_email: Optional[str] = None) -> dict:
        """
        Get comprehensive statistics about the email index.
        
        Args:
            account_email: If provided, only get stats for this account
            
        Returns:
            Dictionary containing index statistics
        """
        try:
            # Get base queryset
            messages_queryset = GoogleMailMessage.objects.all()
            if account_email:
                messages_queryset = messages_queryset.filter(account_email=account_email)
            
            total_messages = messages_queryset.count()
            total_indexed_emails = IndexedEmailAddress.objects.count()
            total_relationships = MessageEmailAddress.objects.count()
            
            # Get field type distribution
            field_counts = {}
            for field_type, _ in MessageEmailAddress.FIELD_CHOICES:
                count = MessageEmailAddress.objects.filter(field=field_type).count()
                if count > 0:
                    field_counts[field_type] = count
            
            # Get top email addresses by message count
            top_emails = IndexedEmailAddress.objects.order_by('-message_count')[:10]
            top_email_list = [
                {
                    'email': email.email,
                    'display_name': email.display_name,
                    'message_count': email.message_count
                }
                for email in top_emails
            ]
            
            return {
                'total_messages': total_messages,
                'total_indexed_emails': total_indexed_emails,
                'total_relationships': total_relationships,
                'field_distribution': field_counts,
                'top_email_addresses': top_email_list,
                'account_email': account_email
            }
            
        except Exception as e:
            logger.error(f'Error getting index statistics: {e}')
            raise 