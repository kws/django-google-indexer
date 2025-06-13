import json
from collections import namedtuple
from email.utils import formataddr, parseaddr, parsedate_to_datetime
from mailbox import Message

from django.db import models


class SyncState(models.Model):
    """Tracks sync state for each Gmail account"""
    account_email = models.EmailField(unique=True, help_text="Gmail account email")
    last_history_id = models.CharField(max_length=255, help_text="Last processed history ID for incremental sync")
    last_sync_at = models.DateTimeField(auto_now=True, help_text="When the last sync completed")
    
    class Meta:
        ordering = ['-last_sync_at']
    
    def __str__(self):
        return f"SyncState for {self.account_email} (History ID: {self.last_history_id})"


class EmailAddress(namedtuple("BaseEmailAddress", "name email")):
    __slots__ = ()

    @classmethod
    def from_rfc_address(cls, rfc_address):
        name, email = parseaddr(rfc_address)
        return cls(name, email)

    def __str__(self):
        return formataddr(self)


class IndexedEmailAddress(models.Model):
    """Stores unique email addresses for indexing and filtering"""
    email = models.EmailField(unique=True, help_text="Normalized (lowercase) email address")
    display_name = models.CharField(max_length=255, blank=True, help_text="Most common display name for this email")
    first_seen = models.DateTimeField(null=True, blank=True, help_text="When this email address first appeared in a message")
    last_seen = models.DateTimeField(null=True, blank=True, help_text="When this email address last appeared in a message")
    message_count = models.PositiveIntegerField(default=0, help_text="Number of messages this email appears in")
    
    class Meta:
        ordering = ['email']
        indexes = [
            models.Index(fields=['email']),
            models.Index(fields=['message_count']),
        ]
    
    def __str__(self):
        return self.email
    
    def save(self, *args, **kwargs):
        # Ensure email is stored in lowercase for case-insensitive lookups
        self.email = self.email.lower()
        super().save(*args, **kwargs)


class MessageEmailAddress(models.Model):
    """Through model for many-to-many relationship between messages and email addresses"""
    FIELD_CHOICES = [
        ('from', 'From'),
        ('to', 'To'), 
        ('cc', 'CC'),
        ('bcc', 'BCC'),
        ('reply_to', 'Reply-To'),
    ]
    
    message = models.ForeignKey('GoogleMailMessage', on_delete=models.CASCADE)
    email_address = models.ForeignKey('IndexedEmailAddress', on_delete=models.CASCADE)
    field = models.CharField(max_length=10, choices=FIELD_CHOICES, help_text="Which email field this address appears in")
    display_name = models.CharField(max_length=255, blank=True, help_text="Display name used in this specific message")
    
    class Meta:
        unique_together = ['message', 'email_address', 'field']
        indexes = [
            models.Index(fields=['message', 'field']),
            models.Index(fields=['email_address', 'field']),
        ]
    
    def __str__(self):
        return f"{self.email_address.email} ({self.field}) in {self.message.message_id}"


def _as_text(value):
    if isinstance(value, list):
        return [_as_text(v) for v in value]
    else:
        return str(value)


# Create your models here.
class GoogleMailMessage(models.Model):
    message_id = models.CharField(max_length=255)
    account_email = models.EmailField(help_text="Gmail account email this message belongs to")
    history_id = models.CharField(max_length=255)
    thread_id = models.CharField(max_length=255)
    snippet = models.TextField()
    label_ids = models.JSONField()
    raw = models.BinaryField()
    internal_date = models.PositiveBigIntegerField()
    
    # Additional useful fields for email management
    size_estimate = models.PositiveIntegerField(null=True, blank=True, help_text="Estimated size in bytes")
    is_read = models.BooleanField(default=False)
    is_starred = models.BooleanField(default=False)
    is_important = models.BooleanField(default=False)
    
    # Many-to-many relationship with email addresses
    email_addresses = models.ManyToManyField(
        'IndexedEmailAddress',
        through='MessageEmailAddress',
        related_name='messages',
        help_text="Email addresses that appear in this message"
    )
    
    # Timestamps for tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-internal_date']
        indexes = [
            models.Index(fields=['account_email']),
            models.Index(fields=['thread_id']),
            models.Index(fields=['internal_date']),
            models.Index(fields=['is_read']),
            models.Index(fields=['account_email', 'internal_date']),
        ]
        constraints = [
            models.UniqueConstraint(fields=['message_id', 'account_email'], name='unique_message_per_account')
        ]

    def __str__(self):
        return f"{self.message_id} - {self.header_subject or '(No Subject)'}"

    @property
    def mbox(self):
        if not getattr(self, "_mbox", None):
            self._mbox = Message(self.raw)
        return self._mbox

    @property
    def json(self):
        return {
            "from": _as_text(self.header_from),
            "to": _as_text(self.header_to),
            "cc": _as_text(self.header_cc),
            "subject": self.header_subject,
            "text": "\n".join(
                [
                    p.get_payload(decode=True).decode(p.get_content_charset())
                    for p in self.find_parts("text/plain")
                ]
            ),
        }

    def to_dict(self, include_attachments=False, max_message_length=None):
        data = {
            "from": _as_text(self.header_from),
            "to": _as_text(self.header_to),
            "cc": _as_text(self.header_cc),
            "subject": self.header_subject,
            "text": "\n".join(
                [
                    p.get_payload(decode=True).decode(p.get_content_charset())
                    for p in self.find_parts("text/plain")
                ]
            ),
        }
        if max_message_length:
            data["text"] = data["text"][:max_message_length]

        if include_attachments:
            data["attachments"] = list(self.summarise_attachments())

        return data


    def find_parts(self, content_type):
        return [
            part for part in self.mbox.walk() if content_type in part.get_content_type()
        ]

    @property
    def header_from(self) -> EmailAddress:
        return EmailAddress.from_rfc_address(self.mbox.get("From"))

    @property
    def header_to(self) -> EmailAddress:
        return [EmailAddress.from_rfc_address(a) for a in self.mbox.get_all("To", [])]

    @property
    def header_cc(self) -> EmailAddress:
        return [EmailAddress.from_rfc_address(a) for a in self.mbox.get_all("Cc", [])]

    @property
    def header_subject(self) -> str:
        return self.mbox.get("Subject")

    def update_flags_from_labels(self):
        """Update boolean flags based on Gmail label_ids"""
        if not self.label_ids:
            return
            
        # Common Gmail system labels
        self.is_read = 'UNREAD' not in self.label_ids
        self.is_starred = 'STARRED' in self.label_ids
        self.is_important = 'IMPORTANT' in self.label_ids
        
    def summarise_attachments(self):
        """Generator that yields attachment summaries"""
        for part in self.mbox.walk():
            if part.get_content_disposition() == 'attachment':
                filename = part.get_filename()
                if filename:
                    yield {
                        'filename': filename,
                        'content_type': part.get_content_type(),
                        'size': len(part.get_payload(decode=True)) if part.get_payload() else 0
                    }

    def index_email_addresses(self):
        """Extract and index all email addresses from this message"""
        from django.utils import timezone
        from datetime import datetime
        from email.utils import parsedate_to_datetime
        
        # Get the email date from the message headers
        date_str = self.mbox.get('Date')
        if date_str:
            try:
                message_date = parsedate_to_datetime(date_str)
            except (TypeError, ValueError):
                # Fallback to internal_date if Date header is invalid
                message_date = datetime.fromtimestamp(self.internal_date / 1000.0)
        else:
            # Fallback to internal_date if no Date header
            message_date = datetime.fromtimestamp(self.internal_date / 1000.0)
        
        # Clear existing email address relationships for this message
        self.messageemailaddress_set.all().delete()
        
        email_field_mapping = [
            ('from', [self.header_from] if self.header_from else []),
            ('to', self.header_to or []),
            ('cc', self.header_cc or []),
            # Add more fields as needed (bcc, reply_to, etc.)
        ]
        
        for field_name, email_list in email_field_mapping:
            for email_addr in email_list:
                if email_addr and email_addr.email:
                    # Get or create the indexed email address
                    indexed_email, created = IndexedEmailAddress.objects.get_or_create(
                        email=email_addr.email.lower(),
                        defaults={
                            'display_name': email_addr.name or '',
                            'message_count': 0,
                            'first_seen': message_date,
                            'last_seen': message_date
                        }
                    )
                    
                    # Update the display name if this one is better (has a name when the stored one doesn't)
                    if email_addr.name and not indexed_email.display_name:
                        indexed_email.display_name = email_addr.name
                    
                    # For existing records, update first_seen if it's not set or if this message is older
                    if not created:
                        if indexed_email.first_seen is None or message_date < indexed_email.first_seen:
                            indexed_email.first_seen = message_date
                        if indexed_email.last_seen is None or message_date > indexed_email.last_seen:
                            indexed_email.last_seen = message_date
                    
                    indexed_email.save()
                    
                    # Create the relationship
                    MessageEmailAddress.objects.get_or_create(
                        message=self,
                        email_address=indexed_email,
                        field=field_name,
                        defaults={'display_name': email_addr.name or ''}
                    )
        
        # Update message counts for all related email addresses
        for email_addr in self.email_addresses.all():
            email_addr.message_count = email_addr.messages.count()
            email_addr.save()

