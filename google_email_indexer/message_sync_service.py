import base64
import logging
from typing import List, Optional, Dict, Any
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .models import GoogleMailMessage, SyncState
from .service import GoogleEmailService

logger = logging.getLogger(__name__)


class MessageSyncService:
    """
    Advanced message synchronization service using Gmail's History API.
    
    This service efficiently syncs Gmail messages by:
    1. Performing full sync on first run
    2. Using incremental sync via History API for subsequent runs
    3. Handling history ID expiration gracefully
    4. Batch processing for performance
    """
    
    def __init__(self, gmail_service: Optional[GoogleEmailService] = None, account_email_override: Optional[str] = None):
        self.gmail_service = gmail_service or GoogleEmailService()
        self._current_account_email = None
        self._account_email_override = account_email_override
        
    @property
    def current_account_email(self) -> Optional[str]:
        """Get the current account email, with override taking precedence."""
        # If we have an explicit override, use it
        if self._account_email_override:
            return self._account_email_override
            
        # Otherwise, lazy-load from Gmail profile if not already cached
        if self._current_account_email is None:
            try:
                profile = self.gmail_service.get_profile()
                self._current_account_email = profile.get('emailAddress')
            except Exception as e:
                logger.error(f"Failed to get Gmail profile: {e}")
                return None
        return self._current_account_email
        
    def sync_messages(self, max_results: int = 100, force_full_sync: bool = False, 
                     label_ids: Optional[List[str]] = None, label_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Main sync method that automatically chooses between full and incremental sync.
        
        Args:
            max_results: Maximum number of messages for full sync
            force_full_sync: Force full sync even if incremental is possible
            label_ids: List of Gmail label IDs to filter by
            label_names: List of Gmail label names to filter by (converted to IDs)
        
        Returns:
            Dict with sync statistics and information
        """
        # Ensure we have the current account email
        if not self.current_account_email:
            raise ValueError("Unable to determine current Gmail account email")
            
        # Convert label names to IDs
        resolved_label_ids = self._resolve_label_filters(label_ids, label_names)
        
        last_history_id = self._get_last_history_id()
        
        if force_full_sync or not last_history_id:
            logger.info(f"Performing full sync for {self.current_account_email}{' with labels: ' + str(resolved_label_ids) if resolved_label_ids else ''}")
            return self._full_sync(max_results, resolved_label_ids)
        else:
            logger.info(f"Attempting incremental sync for {self.current_account_email} from history ID: {last_history_id}{' with labels: ' + str(resolved_label_ids) if resolved_label_ids else ''}")
            incremental_result = self._incremental_sync(last_history_id, resolved_label_ids)
            
            if incremental_result is None:
                logger.warning(f"Incremental sync failed for {self.current_account_email}, falling back to full sync")
                return self._full_sync(max_results, resolved_label_ids)
            
            return incremental_result
    
    def _full_sync(self, max_results: int, label_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Perform a full synchronization of messages.
        
        This downloads the most recent messages and stores the latest history ID
        for future incremental syncs.
        """
        logger.info(f"Starting full sync with max_results={max_results}")
        
        # Get list of message IDs, optionally filtered by labels
        messages = self.gmail_service.list_messages(max_results=max_results, label_ids=label_ids)
        
        stats = {
            'sync_type': 'full',
            'label_filter': label_ids,
            'total_found': len(messages),
            'new_messages': 0,
            'updated_messages': 0,
            'errors': [],
            'history_id': None
        }
        
        # Process messages in batches for better performance
        batch_size = 50
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            batch_stats = self._process_message_batch(batch, force_update=False)
            
            stats['new_messages'] += batch_stats['new_messages']
            stats['updated_messages'] += batch_stats['updated_messages']
            stats['errors'].extend(batch_stats['errors'])
        
        # Store the latest history ID from the profile for future incremental syncs
        profile = self.gmail_service.get_profile()
        latest_history_id = profile.get('historyId')
        if latest_history_id:
            self._store_last_history_id(latest_history_id)
            stats['history_id'] = latest_history_id
        
        logger.info(f"Full sync completed: {stats}")
        return stats
    
    def _incremental_sync(self, start_history_id: str, label_ids: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Perform incremental sync using Gmail's History API.
        
        Returns None if history ID is too old and full sync is needed.
        """
        logger.info(f"Starting incremental sync from history ID: {start_history_id}")
        
        try:
            # Get history changes since last sync
            # Note: Gmail History API supports labelId filter for a single label
            label_id_filter = label_ids[0] if label_ids and len(label_ids) == 1 else None
            
            history_result = self.gmail_service.list_history(
                start_history_id=start_history_id,
                max_results=500,  # Higher limit for history records
                history_types=['messageAdded', 'messageDeleted', 'labelAdded', 'labelRemoved'],
                label_id=label_id_filter
            )
            
            if history_result is None:
                return None  # History too old, need full sync
            
            history_records = history_result.get('history', [])
            current_history_id = history_result.get('historyId')
            
            stats = {
                'sync_type': 'incremental',
                'label_filter': label_ids,
                'history_records': len(history_records),
                'messages_added': 0,
                'messages_deleted': 0,
                'labels_modified': 0,
                'errors': [],
                'history_id': current_history_id
            }
            
            # Process each history record
            for record in history_records:
                record_stats = self._process_history_record(record, label_ids)
                
                stats['messages_added'] += record_stats['messages_added']
                stats['messages_deleted'] += record_stats['messages_deleted']
                stats['labels_modified'] += record_stats['labels_modified']
                stats['errors'].extend(record_stats['errors'])
            
            # Update stored history ID
            if current_history_id:
                self._store_last_history_id(current_history_id)
            
            logger.info(f"Incremental sync completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Incremental sync failed: {e}")
            return None
    
    def _process_history_record(self, record: Dict[str, Any], label_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Process a single history record and update local messages accordingly."""
        stats = {
            'messages_added': 0,
            'messages_deleted': 0,
            'labels_modified': 0,
            'errors': []
        }
        
        try:
            # Handle added messages
            for msg_added in record.get('messagesAdded', []):
                message_id = msg_added['message']['id']
                try:
                    # Check if message matches label filter (if any)
                    if self._message_matches_labels(msg_added['message'], label_ids):
                        self._download_and_store_message(message_id)
                        stats['messages_added'] += 1
                except Exception as e:
                    stats['errors'].append(f"Failed to add message {message_id}: {e}")
            
            # Handle deleted messages
            for msg_deleted in record.get('messagesDeleted', []):
                message_id = msg_deleted['message']['id']
                try:
                    GoogleMailMessage.objects.filter(
                        message_id=message_id, 
                        account_email=self.current_account_email
                    ).delete()
                    stats['messages_deleted'] += 1
                except Exception as e:
                    stats['errors'].append(f"Failed to delete message {message_id}: {e}")
            
            # Handle label changes
            label_changes = len(record.get('labelsAdded', [])) + len(record.get('labelsRemoved', []))
            if label_changes > 0:
                stats['labels_modified'] += label_changes
                # Update labels for affected messages
                for label_change in record.get('labelsAdded', []) + record.get('labelsRemoved', []):
                    message_id = label_change['message']['id']
                    try:
                        self._update_message_labels(message_id)
                    except Exception as e:
                        stats['errors'].append(f"Failed to update labels for {message_id}: {e}")
        
        except Exception as e:
            stats['errors'].append(f"Failed to process history record: {e}")
        
        return stats
    
    def _process_message_batch(self, message_batch: List[Dict], force_update: bool = False) -> Dict[str, Any]:
        """Process a batch of messages for downloading and storage."""
        stats = {
            'new_messages': 0,
            'updated_messages': 0,
            'errors': []
        }
        
        for message_info in message_batch:
            message_id = message_info['id']
            
            try:
                # Check if message already exists
                existing_message = None
                if not force_update:
                    try:
                        existing_message = GoogleMailMessage.objects.get(
                            message_id=message_id,
                            account_email=self.current_account_email
                        )
                    except GoogleMailMessage.DoesNotExist:
                        pass
                
                if existing_message is None or force_update:
                    self._download_and_store_message(message_id)
                    if existing_message is None:
                        stats['new_messages'] += 1
                    else:
                        stats['updated_messages'] += 1
                        
            except Exception as e:
                stats['errors'].append(f"Failed to process message {message_id}: {e}")
                logger.error(f"Error processing message {message_id}: {e}")
        
        return stats
    
    def _download_and_store_message(self, message_id: str) -> GoogleMailMessage:
        """Download a message from Gmail and store it in the database."""
        # Get the full message data
        message_data = self.gmail_service.get_message(message_id, format="raw")
        raw = base64.urlsafe_b64decode(message_data.get("raw").encode("ASCII"))
        
        # Get additional metadata with minimal format for efficiency
        message_meta = self.gmail_service.get_message(message_id, format="minimal")
        
        # Create or update the message
        with transaction.atomic():
            message, created = GoogleMailMessage.objects.get_or_create(
                message_id=message_id,
                account_email=self.current_account_email,
                defaults={
                    "history_id": message_meta.get("historyId"),
                    "thread_id": message_meta.get("threadId"),
                    "snippet": message_meta.get("snippet", ""),
                    "label_ids": message_meta.get("labelIds", []),
                    "raw": raw,
                    "internal_date": message_meta.get("internalDate"),
                    "size_estimate": message_meta.get("sizeEstimate"),
                }
            )
            
            if not created:
                # Update existing message
                message.history_id = message_meta.get("historyId")
                message.thread_id = message_meta.get("threadId")
                message.snippet = message_meta.get("snippet", "")
                message.label_ids = message_meta.get("labelIds", [])
                message.raw = raw
                message.internal_date = message_meta.get("internalDate")
                message.size_estimate = message_meta.get("sizeEstimate")
                message.save()
            
            # Update flags based on labels
            message.update_flags_from_labels()
            message.save()
        
        return message
    
    def _update_message_labels(self, message_id: str):
        """Update labels for an existing message."""
        try:
            message = GoogleMailMessage.objects.get(
                message_id=message_id,
                account_email=self.current_account_email
            )
            message_meta = self.gmail_service.get_message(message_id, format="minimal")
            
            message.label_ids = message_meta.get("labelIds", [])
            message.history_id = message_meta.get("historyId")
            message.update_flags_from_labels()
            message.save()
            
        except GoogleMailMessage.DoesNotExist:
            # Message doesn't exist locally, download it
            self._download_and_store_message(message_id)
    
    def _get_last_history_id(self) -> Optional[str]:
        """Get the last stored history ID for incremental sync for the current account."""
        if not self.current_account_email:
            return None
            
        try:
            sync_state = SyncState.objects.get(account_email=self.current_account_email)
            return sync_state.last_history_id
        except SyncState.DoesNotExist:
            # Fallback: get from the most recent message for this account
            try:
                latest_message = GoogleMailMessage.objects.filter(
                    account_email=self.current_account_email
                ).order_by('-internal_date').first()
                return latest_message.history_id if latest_message else None
            except Exception:
                return None
    
    def _store_last_history_id(self, history_id: str):
        """Store the history ID for future incremental syncs for the current account."""
        if not self.current_account_email:
            logger.warning("Cannot store history ID: no current account email")
            return
            
        SyncState.objects.update_or_create(
            account_email=self.current_account_email,
            defaults={'last_history_id': history_id}
        )
        logger.info(f"Stored history ID {history_id} for account {self.current_account_email}")
    
    def force_resync_message(self, message_id: str) -> GoogleMailMessage:
        """Force re-download of a specific message."""
        return self._download_and_store_message(message_id)
    
    def cleanup_deleted_messages(self):
        """Remove messages that no longer exist in Gmail."""
        # This could be implemented to periodically clean up messages
        # that have been deleted from Gmail but still exist locally
        pass
    
    def _resolve_label_filters(self, label_ids: Optional[List[str]], label_names: Optional[List[str]]) -> Optional[List[str]]:
        """Convert label names to IDs and combine with provided IDs."""
        resolved_ids = []
        
        # Add provided label IDs
        if label_ids:
            resolved_ids.extend(label_ids)
        
        # Convert label names to IDs
        if label_names:
            for label_name in label_names:
                label = self.gmail_service.find_label_by_name(label_name)
                if label:
                    resolved_ids.append(label['id'])
                    logger.info(f"Resolved label '{label_name}' to ID: {label['id']}")
                else:
                    logger.warning(f"Label '{label_name}' not found")
        
        return resolved_ids if resolved_ids else None
    
    def _message_matches_labels(self, message: Dict[str, Any], label_ids: Optional[List[str]]) -> bool:
        """Check if a message matches the label filter."""
        if not label_ids:
            return True  # No filter means all messages match
        
        message_labels = message.get('labelIds', [])
        # Check if any of the filter labels are in the message's labels
        return any(label_id in message_labels for label_id in label_ids)
    
    def list_available_labels(self) -> List[Dict[str, Any]]:
        """Get a list of all available labels with their metadata."""
        labels = self.gmail_service.list_labels()
        
        # Add message counts for each label
        enhanced_labels = []
        for label in labels:
            enhanced_label = label.copy()
            
            # Add human-readable type
            if label.get('type') == 'system':
                enhanced_label['category'] = 'System'
            elif label.get('type') == 'user':
                enhanced_label['category'] = 'User'
            else:
                enhanced_label['category'] = 'Unknown'
            
            enhanced_labels.append(enhanced_label)
        
        return enhanced_labels 