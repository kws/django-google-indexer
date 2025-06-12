from datetime import datetime

from django.contrib import admin

from .models import GoogleMailMessage


@admin.register(GoogleMailMessage)
class GoogleMailMessageAdmin(admin.ModelAdmin):
    list_display = (
        "formated_date",
        "account_email",
        "header_from",
        "header_to",
        "subject",
        "snippet",
        "message_id",
        "thread_id",
        "history_id",
    )
    list_filter = ("account_email", "is_read", "is_starred", "is_important")
    search_fields = ("raw",)

    @admin.display(description="Date", ordering="internal_date")
    def formated_date(self, obj) -> str:
        return datetime.fromtimestamp(obj.internal_date / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @admin.display(description="Header From", ordering=None)
    def header_from(self, obj) -> str:
        return str(obj.header_from)

    @admin.display(description="Header To", ordering=None)
    def header_to(self, obj) -> str:
        return ", ".join([str(a) for a in obj.header_to]) if obj.header_to else ""

    @admin.display(description="Subject", ordering=None)
    def subject(self, obj) -> str:
        return obj.header_subject
