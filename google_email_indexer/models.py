import json
from collections import namedtuple
from email.utils import formataddr, parseaddr
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

