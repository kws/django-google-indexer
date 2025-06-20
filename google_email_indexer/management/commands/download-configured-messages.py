from django.core.management.base import BaseCommand
from google_email_indexer.tasks import sync_configuration


class Command(BaseCommand):
    help = "Download messages from configured sources"

    def handle(self, *args, **options):
        sync_configuration()