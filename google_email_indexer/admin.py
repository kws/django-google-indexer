from datetime import datetime
from email.header import decode_header
from email.utils import parseaddr
import html
from django.contrib import admin
from django.db.models import Q
from django.urls import reverse
from django.utils.html import format_html
from django.utils import timezone

from .models import GoogleMailMessage, IndexedEmailAddress, MessageEmailAddress


def decode_mime_header(header_value):
    """Decode MIME-encoded email headers to readable text"""
    if not header_value:
        return ""
    
    try:
        # Parse the email address to separate name and email
        name, email = parseaddr(str(header_value))
        
        if name:
            # Decode the name part if it's MIME encoded
            decoded_parts = decode_header(name)
            decoded_name = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_name += part.decode(encoding or 'utf-8', errors='replace')
                else:
                    decoded_name += part
            
            # Return formatted as "Decoded Name <email@domain.com>"
            if email:
                return f"{decoded_name} <{email}>"
            else:
                return decoded_name
        else:
            # Just return the email if no name part
            return email or str(header_value)
    except Exception:
        # Fallback to original string if decoding fails
        return str(header_value)


# Inline classes defined first
class MessageEmailAddressInline(admin.TabularInline):
    model = MessageEmailAddress
    extra = 0
    readonly_fields = ("email_address", "field", "display_name")
    can_delete = False
    show_change_link = True
    verbose_name = "Email Address"
    verbose_name_plural = "Email Addresses in this Message"

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('email_address')


class MessageInline(admin.TabularInline):
    model = GoogleMailMessage.email_addresses.through
    extra = 0
    readonly_fields = ("message", "field", "display_name")
    can_delete = False
    show_change_link = True
    verbose_name = "Message"
    verbose_name_plural = "Messages containing this Email Address"

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('message')


# Filter classes
class EmailAddressFilter(admin.SimpleListFilter):
    """Custom filter to filter messages by email address"""
    title = 'email address'
    parameter_name = 'email_address'

    def lookups(self, request, model_admin):
        # Get email addresses sorted alphabetically for easier finding
        emails = IndexedEmailAddress.objects.filter(
            message_count__gt=0
        ).order_by('email')[:100]  # Increased limit since alphabetical is easier to browse
        return [(email.email, f"{email.email} ({email.message_count} messages)") for email in emails]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(email_addresses__email=self.value()).distinct()
        return queryset


class EmailFieldFilter(admin.SimpleListFilter):
    """Filter messages by which field an email address appears in"""
    title = 'email field type'
    parameter_name = 'email_field'

    def lookups(self, request, model_admin):
        return MessageEmailAddress.FIELD_CHOICES

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(messageemailaddress__field=self.value()).distinct()
        return queryset


class SenderFilter(admin.SimpleListFilter):
    """Filter messages by sender email address"""
    title = 'sender'
    parameter_name = 'sender'

    def lookups(self, request, model_admin):
        # Get senders sorted alphabetically for easier finding
        from django.db.models import Count
        senders = IndexedEmailAddress.objects.filter(
            messageemailaddress__field='from'
        ).annotate(
            from_count=Count('messageemailaddress', filter=Q(messageemailaddress__field='from'))
        ).filter(
            from_count__gt=0
        ).order_by('email').distinct()[:50]  # Alphabetical order, increased limit
        
        return [(sender.email, f"{sender.email} ({sender.from_count} sent)") for sender in senders]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(
                messageemailaddress__email_address__email=self.value(),
                messageemailaddress__field='from'
            ).distinct()
        return queryset


class RecipientFilter(admin.SimpleListFilter):
    """Filter messages by recipient email address (to/cc)"""
    title = 'recipient'
    parameter_name = 'recipient'

    def lookups(self, request, model_admin):
        # Get recipients sorted alphabetically for easier finding
        from django.db.models import Count
        recipients = IndexedEmailAddress.objects.filter(
            messageemailaddress__field__in=['to', 'cc']
        ).annotate(
            recipient_count=Count('messageemailaddress', filter=Q(messageemailaddress__field__in=['to', 'cc']))
        ).filter(
            recipient_count__gt=0
        ).order_by('email').distinct()[:50]  # Alphabetical order, increased limit
        
        return [(recipient.email, f"{recipient.email} ({recipient.recipient_count} received)") for recipient in recipients]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(
                messageemailaddress__email_address__email=self.value(),
                messageemailaddress__field__in=['to', 'cc']
            ).distinct()
        return queryset


@admin.register(GoogleMailMessage)
class GoogleMailMessageAdmin(admin.ModelAdmin):
    list_display = (
        "formated_date",
        "header_from",
        "header_to",
        "subject",
        "decoded_snippet",
        "thread_message_count",
        "history_id",
        "account_email",
        "email_addresses_count",
    )
    list_filter = (
        "account_email", 
        "is_read", 
        "is_starred", 
        "is_important",
        EmailAddressFilter,
        EmailFieldFilter,
        SenderFilter,
        RecipientFilter,
        "created_at",
    )
    search_fields = (
        "raw", 
        "snippet", 
        "message_id", 
        "thread_id",
        "messageemailaddress__email_address__email",
        "messageemailaddress__display_name",
    )
    readonly_fields = ("email_addresses_display",)
    inlines = [MessageEmailAddressInline]

    @admin.display(description="Date", ordering="internal_date")
    def formated_date(self, obj) -> str:
        return timezone.make_aware(datetime.fromtimestamp(obj.internal_date / 1000)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @admin.display(description="Header From", ordering=None)
    def header_from(self, obj) -> str:
        return decode_mime_header(obj.header_from)

    @admin.display(description="Header To", ordering=None)
    def header_to(self, obj) -> str:
        if not obj.header_to:
            return ""
        decoded_addresses = [decode_mime_header(addr) for addr in obj.header_to]
        return ", ".join(decoded_addresses)

    @admin.display(description="Subject", ordering=None)
    def subject(self, obj) -> str:
        return obj.header_subject

    @admin.display(description="Snippet", ordering=None)
    def decoded_snippet(self, obj) -> str:
        """Display snippet with HTML entities decoded"""
        return html.unescape(obj.snippet) if obj.snippet else ""

    @admin.display(description="Thread Messages", ordering=None)
    def thread_message_count(self, obj):
        """Display count of messages in thread as clickable link to filter by thread"""
        if not obj.thread_id:
            return "No thread"
        
        # Count messages in this thread
        count = GoogleMailMessage.objects.filter(thread_id=obj.thread_id).count()
        
        # Create URL for filtering by this thread_id
        url = reverse('admin:google_email_indexer_googlemailmessage_changelist')
        filter_url = f"{url}?thread_id__exact={obj.thread_id}"
        
        # Return clickable link with count
        return format_html(
            '<a href="{}" title="View all {} messages in this thread">{} messages</a>',
            filter_url,
            count,
            count
        )

    @admin.display(description="Email Count", ordering=None)
    def email_addresses_count(self, obj) -> int:
        """Display the number of unique email addresses in this message"""
        return obj.email_addresses.count()

    @admin.display(description="Email Addresses")
    def email_addresses_display(self, obj) -> str:
        """Display all email addresses associated with this message"""
        relationships = obj.messageemailaddress_set.select_related('email_address').all()
        if not relationships:
            return "No indexed email addresses"
        
        grouped = {}
        for rel in relationships:
            field = rel.field
            if field not in grouped:
                grouped[field] = []
            display_name = rel.display_name or rel.email_address.email
            grouped[field].append(f"{display_name} <{rel.email_address.email}>")
        
        result = []
        for field, emails in grouped.items():
            result.append(f"{field.upper()}: {', '.join(emails)}")
        
        return "\n".join(result)

    def get_queryset(self, request):
        # Optimize queries by prefetching related email addresses
        return super().get_queryset(request).prefetch_related(
            'email_addresses',
            'messageemailaddress_set__email_address'
        )


@admin.register(IndexedEmailAddress)
class IndexedEmailAddressAdmin(admin.ModelAdmin):
    list_display = (
        "email",
        "display_name",
        "message_count",
        "first_seen",
        "last_seen",
    )
    list_filter = ("first_seen", "last_seen")
    search_fields = ("email", "display_name")
    readonly_fields = ("first_seen", "last_seen", "message_count")
    ordering = ("-message_count", "email")
    inlines = [MessageInline]
    
    def get_queryset(self, request):
        # Optimize queries by selecting related data
        return super().get_queryset(request).prefetch_related(
            'messages',
            'messageemailaddress_set'
        )


@admin.register(MessageEmailAddress)
class MessageEmailAddressAdmin(admin.ModelAdmin):
    list_display = (
        "email_address",
        "message_snippet",
        "field",
        "display_name",
        "message_date",
    )
    list_filter = ("field", "email_address__email")
    search_fields = ("email_address__email", "display_name", "message__snippet")
    readonly_fields = ("message", "email_address", "field", "display_name")
    
    @admin.display(description="Message", ordering="message__internal_date")
    def message_snippet(self, obj) -> str:
        return f"{obj.message.message_id} - {obj.message.snippet[:50]}..."
    
    @admin.display(description="Date", ordering="message__internal_date")
    def message_date(self, obj) -> str:
        return timezone.make_aware(datetime.fromtimestamp(obj.message.internal_date / 1000)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    
    def get_queryset(self, request):
        # Optimize queries by selecting related data
        return super().get_queryset(request).select_related('message', 'email_address')
