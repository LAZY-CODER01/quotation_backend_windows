"""
Gmail API service for QuoteSnap application.

This module handles all Gmail API interactions including
email monitoring, fetching, and authentication management.
"""

import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone
import threading
import time
import json
import os
import base64
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Gmail API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# AI Extraction Service
from app.services.ai_email_extraction import extract_hardware_quotation_details
from app.services.duckdb_service import DuckDBService

# File processing utilities
from config.settings import Config
from utils import process_attachment
from app.extensions import socketio

logger = logging.getLogger(__name__)

class GmailService:
    """
    Service class for handling Gmail API operations.
    """
    
    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize Gmail service with authentication credentials.
        
        Args:
            credentials_path (str): Path to Gmail API credentials file
        """
        self.credentials_path = credentials_path
        self.service = None
        self.credentials = None
        self.monitoring_active = False
        self.monitoring_thread = None
        self.last_check_time = None
        self.flow = None  # Store OAuth flow for web-based authentication
        
        logger.info("Gmail service initialized")
    
    def get_authorization_url(self, redirect_uri: str, state: Optional[str] = None) -> Optional[str]:
        """
        Generate Google OAuth authorization URL for web-based authentication.
        
        Args:
            redirect_uri (str): The URI to redirect to after authorization
            state (str): Optional state parameter for CSRF protection
            
        Returns:
            Optional[str]: Authorization URL or None if failed
        """
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                  'https://www.googleapis.com/auth/gmail.modify',
                  'https://www.googleapis.com/auth/drive.file']
        
        try:
            if not self.credentials_path or not os.path.exists(self.credentials_path):
                logger.error("Gmail credentials file not found")
                return None
            
            # Create OAuth flow for web application
            self.flow = Flow.from_client_secrets_file(
                self.credentials_path,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
            
            # Generate authorization URL
            authorization_url, state_token = self.flow.authorization_url(
                access_type='offline',
                include_granted_scopes='true',
                state=state,
                prompt='consent'  # Force consent screen to ensure refresh token
            )
            
            logger.info(f"Generated authorization URL for redirect_uri: {redirect_uri}")
            return authorization_url
            
        except Exception as e:
            logger.error(f"Failed to generate authorization URL: {str(e)}")
            return None
    
    def exchange_and_save_company_token(self, code: str, redirect_uri: str) -> bool:
        """
        Exchange authorization code for credentials and save to company_tokens.
        """
        try:
            if not self.credentials_path or not os.path.exists(self.credentials_path):
                logger.error("Gmail credentials file not found")
                return False

            SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                      'https://www.googleapis.com/auth/gmail.modify',
                      'https://www.googleapis.com/auth/drive.file']
            
            # Always recreate the flow to ensure clean state
            flow = Flow.from_client_secrets_file(
                self.credentials_path,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
            
            # Exchange code for credentials
            flow.fetch_token(code=code)
            creds = flow.credentials
            
            # Convert to JSON
            token_json = creds.to_json()
            
            # Save to DuckDB using save_company_token
            db = DuckDBService()
            if db.connect():
                success = db.save_company_token(token_json)
                db.disconnect()
                
                if success:
                    # Set up service immediately
                    self.credentials = creds
                    self.service = build('gmail', 'v1', credentials=creds)
                    self._initialize_labels()
                    return True
            
            return False

        except Exception as e:
            logger.error(f"Failed to exchange/save company token: {str(e)}")
            return False

    def authenticate_from_info(self, token_info: dict) -> bool:
        """
        Authenticate using token dictionary (from DB).
        """
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                  'https://www.googleapis.com/auth/gmail.modify',
                  'https://www.googleapis.com/auth/drive.file']
        
        try:
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            
            # Refresh if needed
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    # Auto-save refreshed token
                    db = DuckDBService()
                    if db.connect():
                        db.save_company_token(creds.to_json())
                        db.disconnect()
                except Exception as e:
                    if "invalid_scope" in str(e):
                        logger.error("  SCOPE MISMATCH: The stored token is missing required permissions (likely Google Drive). Please RE-AUTHENTICATE in the Admin Panel.")
                    else:
                        logger.error(f"Failed to refresh credentials: {str(e)}")
                    return False
            
            if not creds or not creds.valid:
                logger.error("Invalid credentials from info")
                return False
            
            # Build Gmail service
            self.service = build('gmail', 'v1', credentials=creds)
            self.credentials = creds
            logger.info("Gmail API authentication successful from info")
            
            # Initialize required labels
            self._initialize_labels()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate from info: {str(e)}")
            return False
    
    def _initialize_labels(self):
        """
        Initialize all required Gmail labels for SnapQuote system.
        Creates labels if they don't exist.
        """
        labels_to_create = [
            ("SnapQuote-Fetched", "green"),
            ("SnapQuote-Irrelevant", "grey"), 
            ("SnapQuote-Reprocess", "blue")
        ]
        
        for label_name, color in labels_to_create:
            try:
                self.create_label_if_not_exists(label_name, color)
            except Exception as e:
                logger.error(f"Failed to initialize label '{label_name}': {str(e)}")

    def create_label_if_not_exists(self, label_name: str, color: str) -> Optional[str]:
        """Create a label if it doesn't already exist."""
        try:
            if not self.service:
                return None
                
            results = self.service.users().labels().list(userId='me').execute()
            labels = results.get('labels', [])
            
            for label in labels:
                if label['name'].lower() == label_name.lower():
                    return label['id']
            
            # Create label
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show',
                'color': {
                    'backgroundColor': self._get_color_hex(color),
                    'textColor': '#ffffff'
                }
            }
            created_label = self.service.users().labels().create(userId='me', body=label_object).execute()
            return created_label['id']
        except Exception as e:
            logger.warning(f"Label creation warning: {e}")
            return None

    def _get_color_hex(self, color_name: str) -> str:
        colors = {
            'green': '#00ff00', # Simplified
            'blue': '#0000ff',
            'grey': '#666666'
        }
        return colors.get(color_name, '#666666')
    
    def start_monitoring(self, check_interval: int = 30) -> bool:
        """
        Start monitoring Gmail inbox for new emails.
        """
        if self.monitoring_active:
            logger.warning("Email monitoring is already active")
            return True
        
        if not self.service:
            logger.error("Gmail service not authenticated")
            return False
        
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(check_interval,),
            daemon=True
        )
        self.monitoring_thread.start()
        logger.info(f"Gmail monitoring started with {check_interval}s interval")
        return True
    
    def stop_monitoring(self) -> bool:
        """
        Stop monitoring Gmail inbox.
        """
        if not self.monitoring_active:
            logger.warning("Email monitoring is not active")
            return True
        
        self.monitoring_active = False
        if self.monitoring_thread:
            # We don't join here to avoid blocking Flask
            # self.monitoring_thread.join(timeout=10) 
            pass
        
        logger.info("Email monitoring stopped")
        return True
    
    def _monitoring_loop(self, check_interval: int):
        """
        Main monitoring loop that runs in background thread.
        """
        logger.info("Email monitoring loop started")
        
        while self.monitoring_active:
            try:
                # Check for new emails (not yet processed)
                new_emails = self._check_for_new_emails()
                
                # Check for reprocess emails
                reprocess_emails = self._check_for_reprocess_emails()
                
                all_emails = []
                if new_emails: all_emails.extend(new_emails)
                if reprocess_emails: all_emails.extend(reprocess_emails)
                
                if all_emails:
                    db_service = DuckDBService()
                    if db_service.connect():
                        # Ensure table exists (idempotent)
                        db_service.create_table()
                        
                        for email_data in all_emails:
                            self._process_single_email(email_data, db_service)
                            
                        db_service.disconnect()
                
                self.last_check_time = datetime.now()
                for _ in range(check_interval):
                    if not self.monitoring_active: break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(60)

    def _process_single_email(self, email_data, db_service):
        """Process a single email: Extraction -> DB -> Label Update"""
        gmail_id = email_data.get('gmail_id')
        is_reprocess = email_data.get('is_reprocess', False)
        
        combined_content = self._combine_email_content(email_data)
        
        try:
            extraction_result = extract_hardware_quotation_details(combined_content)
            
            if is_reprocess:
                self.remove_label_from_email(gmail_id, "SnapQuote-Reprocess")

            if extraction_result.get("status") == "NOT_VALID":
                self.add_label_to_email(gmail_id, "SnapQuote-Irrelevant", "grey")
            else:
                existing_record = db_service.get_extraction(gmail_id)
                if existing_record:
                    db_service.update_extraction(gmail_id, extraction_result)
                    event_type = 'UPDATE'
                else:
                    db_service.insert_extraction(email_data, extraction_result)
                    event_type = 'NEW'
                
                self.add_label_to_email(gmail_id, "SnapQuote-Fetched", "green")
                
                # Emit Socket Event
                try:
                    updated_ticket = db_service.get_extraction(gmail_id)
                    if updated_ticket:
                        # Convert datetime objects to string if needed, but jsonify usually handles isoformat if we use standard simplejson or similar. 
                        # DuckDBService usually returns dicts with objects. 
                        # Let's ensure serialization is safe. JSON dumps in backend_app handles it. 
                        # socketio.emit uses json.dumps which might fail on datetime.
                        # DuckDBService get_extraction returns some datetimes.
                        # We should serialize them.
                        # Ideally pass it through a serializer. 
                        # For now, let's rely on standard serialization or converting.
                        # Backend app uses jsonify. SocketIO uses json. 
                        # Let's manually convert datetimes in the object if needed.
                        # Checking DuckDBService: returns received_at as timestamp.
                        # Let's simple format dates.
                        
                        def json_serial(obj):
                            if isinstance(obj, (datetime, datetime.date)): 
                                return obj.isoformat()
                            raise TypeError ("Type %s not serializable" % type(obj))
                            
                        # Dump and Load to ensure clean JSON
                        sanitized_data = json.loads(json.dumps(updated_ticket, default=str))
                        
                        socketio.emit('ticket_update', {'type': event_type, 'data': sanitized_data})
                        logger.info(f"Emitted {event_type} event for {gmail_id}")
                except Exception as emit_err:
                    logger.error(f"Socket emit failed: {emit_err}")
                
        except Exception as e:
            logger.error(f"AI extraction failed: {str(e)}")

    def _combine_email_content(self, email_data):
        parts = [
            f"Subject: {email_data.get('subject', '')}",
            f"━━━ EMAIL BODY ━━━\n{email_data.get('body_text', '')}"
        ]
        # Add each attachment with a clear section header
        if 'attachment_contents' in email_data:
            for i, att_content in enumerate(email_data['attachment_contents'], 1):
                parts.append(f"━━━ ATTACHMENT {i} ━━━\n{att_content}")
        return "\n\n".join(parts)

    def _check_for_new_emails(self):
        try:
            # Query: Inbox + Not Processed + Newer than 1 Day
            query = "in:inbox -label:SnapQuote-Fetched -label:SnapQuote-Irrelevant newer_than:1d"
            results = self.service.users().messages().list(userId='me', q=query, maxResults=10).execute()
            messages = results.get('messages', [])
            
            logger.info(f"Checking for new emails. Query found {len(messages)} potential messages.")
            
            email_details_list = []
            for m in messages:
                details = self.get_email_details(m['id'])
                if details:
                    email_details_list.append(details)
            
            return email_details_list
        except Exception as e:
            logger.error(f"Check new emails error: {e}")
            return []

    def _check_for_reprocess_emails(self):
        try:
            self.create_label_if_not_exists("SnapQuote-Reprocess", "blue")
            query = "label:SnapQuote-Reprocess"
            results = self.service.users().messages().list(userId='me', q=query, maxResults=50).execute()
            messages = results.get('messages', [])
            reprocess_emails = []
            for m in messages:
                details = self.get_email_details(m['id'])
                if details:
                    details['is_reprocess'] = True
                    reprocess_emails.append(details)
            return reprocess_emails
        except Exception as e: 
            logger.error(f"Reprocess check error: {e}")
            return []

    def get_email_details(self, email_id: str) -> Optional[Dict]:
        if not self.service: return None
        try:
            message = self.service.users().messages().get(userId='me', id=email_id, format='full').execute()
            # ... (Simplified extraction for brevity in this rewrite, or should I be more careful?)
            # The previous file had robust extraction. I should probably copy it or allow it to be robust.
            # Since I am replacing the whole file, I must be careful not to lose logic.
            # I will use a robust extraction logic similar to before but cleaner.
            
            headers = {h['name'].lower(): h['value'] for h in message['payload'].get('headers', [])}
            body_text = ""
            attachments = []
            
            def extract_parts(part):
                nonlocal body_text, attachments
                if part.get('mimeType') == 'text/plain' and 'data' in part['body']:
                    body_text += base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
                elif 'parts' in part:
                    for p in part['parts']: extract_parts(p)
                
                filename = part.get('filename')
                if filename and part['body'].get('attachmentId'):
                    attachments.append({
                        'filename': filename,
                        'attachmentId': part['body']['attachmentId'],
                        'mimeType': part.get('mimeType')
                    })

            if 'parts' in message['payload']:
                for p in message['payload']['parts']: extract_parts(p)
            else:
                extract_parts(message['payload'])

            # Attachments content
            attachment_contents = []
            for att in attachments:
                if self._is_supported_file(att['filename']):
                    content = self._get_attachment_content(email_id, att['attachmentId'])
                    if content:
                        processed = process_attachment(att['filename'], content)
                        attachment_contents.append(f"Attachment {att['filename']}:\n{processed}")

            return {
                'gmail_id': email_id,
                'subject': headers.get('subject', ''),
                'sender': headers.get('from', ''),
                'received_at': (datetime.fromtimestamp(int(message['internalDate'])/1000, timezone.utc) + timedelta(hours=4)).replace(tzinfo=None).isoformat(),
                'body_text': body_text,
                'attachment_contents': attachment_contents,
                'attachments': attachments
            }
        except Exception as e:
            logger.error(f"Error details {email_id}: {e}")
            return None

    def _get_attachment_content(self, email_id, att_id):
        try:
            att = self.service.users().messages().attachments().get(userId='me', messageId=email_id, id=att_id).execute()
            return base64.urlsafe_b64decode(att['data'])
        except Exception: 
            return None

    def _is_supported_file(self, filename):
        return any(filename.lower().endswith(ext) for ext in ['.pdf', '.xlsx', '.xls', '.docx', '.doc', '.png', '.jpg', '.jpeg', '.tiff', '.bmp'])

    def add_label_to_email(self, email_id, label_name, color):
        try:
            label_id = self.create_label_if_not_exists(label_name, color)
            if label_id:
                self.service.users().messages().modify(userId='me', id=email_id, body={'addLabelIds': [label_id]}).execute()
        except Exception as e: logger.error(f"Add label error: {e}")

    def remove_label_from_email(self, email_id, label_name):
        try:
            # We need label ID first
            label_id = self.create_label_if_not_exists(label_name, 'blue') # Color doesn't matter for finding ID
            if label_id:
                self.service.users().messages().modify(userId='me', id=email_id, body={'removeLabelIds': [label_id]}).execute()
        except Exception as e: logger.error(f"Remove label error: {e}")