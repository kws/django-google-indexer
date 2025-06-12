import os
import json
from django.conf import settings
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

class GoogleEmailService:
    """Basic Gmail API service for accessing email data."""
    
    def __init__(self, credentials_file=None, token_file=None, user_email=None):
        # Use Django settings with fallback to parameters or defaults
        self.credentials_file = (
            credentials_file or 
            getattr(settings, 'GOOGLE_CREDENTIALS_FILE', 'credentials.json')
        )
        self.token_file = (
            token_file or 
            getattr(settings, 'GOOGLE_TOKEN_FILE', 'token.json')
        )
        # For service account domain-wide delegation
        self.user_email = user_email or getattr(settings, 'GOOGLE_USER_EMAIL', None)
        # Get scopes from settings
        self.scopes = getattr(settings, 'GOOGLE_API_SCOPES', [
            'https://www.googleapis.com/auth/gmail.readonly'
        ])
        self._service = None  # Lazy initialization
    
    def _is_service_account_credentials(self):
        """Check if the credentials file is for a service account."""
        try:
            with open(self.credentials_file, 'r') as f:
                creds_data = json.load(f)
                return creds_data.get('type') == 'service_account'
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            return False
    
    @property
    def service(self):
        """Lazy initialization of the Gmail API service."""
        if self._service is None:
            self._service = self._build_service()
        return self._service
    
    def _build_service(self):
        """Build and return the Gmail API service."""
        if self._is_service_account_credentials():
            return self._build_service_account_service()
        else:
            return self._build_oauth2_service()
    
    def _build_service_account_service(self):
        """Build Gmail API service using service account credentials."""
        # Load service account credentials
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_file, scopes=self.scopes
        )
        
        # If user_email is specified, delegate domain-wide authority
        if self.user_email:
            creds = creds.with_subject(self.user_email)
        
        return build('gmail', 'v1', credentials=creds)
    
    def _build_oauth2_service(self):
        """Build Gmail API service using OAuth2 credentials (original implementation)."""
        creds = None
        
        # The file token.json stores the user's access and refresh tokens.
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)
        
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.scopes)
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        
        return build('gmail', 'v1', credentials=creds)
    
    def list_messages(self, max_results=100, query=None, label_ids=None):
        """List messages in the user's mailbox with pagination support."""
        all_messages = []
        next_page_token = None
        
        # Gmail API has a max of 500 per request, so we need pagination for larger requests
        page_size = min(max_results, 500)
        
        while len(all_messages) < max_results:
            # Calculate how many messages to request in this page
            remaining = max_results - len(all_messages)
            current_page_size = min(remaining, page_size)
            
            kwargs = {
                'userId': 'me',
                'maxResults': current_page_size
            }
            
            if query:
                kwargs['q'] = query
            if label_ids:
                kwargs['labelIds'] = label_ids
            if next_page_token:
                kwargs['pageToken'] = next_page_token
                
            result = self.service.users().messages().list(**kwargs).execute()
            
            # Add messages from this page
            page_messages = result.get('messages', [])
            all_messages.extend(page_messages)
            
            # Check if there are more pages
            next_page_token = result.get('nextPageToken')
            if not next_page_token or len(page_messages) == 0:
                break  # No more pages available
        
        return all_messages[:max_results]  # Ensure we don't exceed max_results
    
    def get_message(self, message_id, format='full'):
        """Get a specific message by ID."""
        result = self.service.users().messages().get(
            userId='me', 
            id=message_id, 
            format=format
        ).execute()
        return result
    
    def list_history(self, start_history_id, max_results=100, history_types=None, label_id=None):
        """List history of changes since a specific history ID."""
        kwargs = {
            'userId': 'me',
            'startHistoryId': start_history_id,
            'maxResults': max_results
        }
        
        if history_types:
            kwargs['historyTypes'] = history_types
        if label_id:
            kwargs['labelId'] = label_id
            
        try:
            result = self.service.users().history().list(**kwargs).execute()
            return result
        except Exception as e:
            if "404" in str(e):
                # History ID is too old, need full sync
                return None
            raise
    
    def get_profile(self):
        """Get the user's Gmail profile."""
        return self.service.users().getProfile(userId='me').execute()
    
    def list_labels(self):
        """List all labels in the user's mailbox."""
        result = self.service.users().labels().list(userId='me').execute()
        return result.get('labels', [])
    
    def get_label(self, label_id):
        """Get details about a specific label."""
        return self.service.users().labels().get(userId='me', id=label_id).execute()
    
    def find_label_by_name(self, label_name):
        """Find a label by its name (case-insensitive)."""
        labels = self.list_labels()
        for label in labels:
            if label.get('name', '').lower() == label_name.lower():
                return label
        return None 