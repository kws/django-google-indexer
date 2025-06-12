import logging
from typing import List, Optional, Tuple
from django.db import transaction
from django.utils import timezone
from .models import GoogleMailMessage, IndexedEmailAddress, MessageEmailAddress

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
                
            # Get or create the indexed email address
            indexed_email, _ = IndexedEmailAddress.objects.get_or_create(
                email=normalized_email,
                defaults={
                    'display_name': email_addr.name or '',
                    'message_count': 0
                }
            )
            
            # Update display name if this one is better
            if email_addr.name and not indexed_email.display_name:
                indexed_email.display_name = email_addr.name
                indexed_email.save()
            
            # Create the relationship
            relationship, created = MessageEmailAddress.objects.get_or_create(
                message=message,
                email_address=indexed_email,
                field=field_name,
                defaults={'display_name': email_addr.name or ''}
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
                email_addr.last_seen = timezone.now()
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
                message_count = indexed_email.messages.count()
                if indexed_email.message_count != message_count:
                    indexed_email.message_count = message_count
                    indexed_email.last_seen = timezone.now()
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