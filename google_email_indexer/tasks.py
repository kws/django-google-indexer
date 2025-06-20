import logging
from celery import shared_task

from google_email_indexer.message_sync_service import MessageSyncService
from google_email_indexer.models import MesssageSource
from google_email_indexer.service import GoogleEmailService
from google_email_indexer.email_indexing_service import EmailIndexingService

logger = logging.getLogger(__name__)

@shared_task
def sync_configuration(source_ids: list[int] | None = None):
    if source_ids is None:
        sources = MesssageSource.objects.all()
    else:
        sources = MesssageSource.objects.filter(id__in=source_ids)
    
    for source in sources:
        logger.info(f"Syncing [{source.id}]: {source.inbox}")
        label_names = source.labels.split(',') if source.labels else None
        # We do direct sync because we want to then update the index after the sync is complete
        result = sync_mailbox(source.inbox, max_results=1000, force_full=False, label_names=label_names)
        for key, value in result.items():
            logger.info(f"  {key}: {value}")

    logger.info("Maintaining email index")
    result = maintain_email_index()
    for key, value in result.items():
        logger.info(f"  {key}: {value}")

    logger.info("Email index maintenance complete")



@shared_task
def sync_mailbox(account_email: str, max_results: int = 100, force_full: bool = False, label_names: list[str] | None = None):
    gmail_service = GoogleEmailService(user_email=account_email)
    sync_service = MessageSyncService(gmail_service=gmail_service, account_email_override=account_email)

    sync_result = sync_service.sync_messages(
        max_results=max_results,
        force_full_sync=force_full,
        label_names=label_names
    )

    return sync_result



@shared_task
def maintain_email_index(account_email: str | None = None):
    """
    Simple task to maintain the email index.
    
    Args:
        account_email: Optional account email to limit operations to
    """
    try:
        # Validate the index
        validation_results = EmailIndexingService.validate_index(account_email)
        
        # Fix missing entries if any found
        if validation_results['missing_messages'] > 0:
            EmailIndexingService.fix_missing_entries(account_email=account_email)
        
        # Clean up orphaned entries if any found
        if validation_results['orphaned_emails'] > 0:
            EmailIndexingService.maintenance_cleanup()
        
        return {
            'success': True,
            'issues_fixed': validation_results['total_issues'],
            'missing_messages': validation_results['missing_messages'],
            'orphaned_emails': validation_results['orphaned_emails']
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }