"""
Gmail API service for QuoteSnap application.

This module handles all Gmail API interactions including
email monitoring, fetching, and authentication management.
"""

import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import threading
import time
import json
import os
import base64
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json

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
from utils import process_attachment

logger = logging.getLogger(__name__)

class GmailService:
    """
    Service class for handling Gmail API operations.
    """
    
    def __init__(self, credentials_path: Optional[str] = None, token_path: Optional[str] = None):
        """
        Initialize Gmail service with authentication credentials.
        
        Args:
            credentials_path (str): Path to Gmail API credentials file
            token_path (str): Path to store OAuth tokens
        """
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        self.credentials = None
        self.monitoring_active = False
        self.monitoring_thread = None
        self.last_check_time = None
        self.flow = None  # Store OAuth flow for web-based authentication
        
        logger.info("Gmail service initialized")
    
    def authenticate(self) -> bool:
        """
        Authenticate with Gmail API using OAuth 2.0.
        
        Returns:
            bool: True if authentication successful
        """
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                  'https://www.googleapis.com/auth/gmail.modify']
        
        creds = None
        token_path = self.token_path or 'token.json'
        
        # The file token.json stores the user's access and refresh tokens.
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Failed to refresh credentials: {str(e)}")
                    return False
            else:
                if not self.credentials_path or not os.path.exists(self.credentials_path):
                    logger.error("Gmail credentials file not found")
                    return False
                
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    logger.error(f"Failed to authenticate with Gmail: {str(e)}")
                    return False
            
            # Save the credentials for the next run
            try:
                with open(token_path, 'w') as token:
                    token.write(creds.to_json())
            except Exception as e:
                logger.error(f"Failed to save token: {str(e)}")
        
        try:
            self.service = build('gmail', 'v1', credentials=creds)
            self.credentials = creds
            logger.info("Gmail API authentication successful")
            
            # Initialize required labels
            self._initialize_labels()
            
            return True
        except Exception as e:
            logger.error(f"Failed to build Gmail service: {str(e)}")
            return False
    
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
                  'https://www.googleapis.com/auth/gmail.modify']
        
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
    
    def authenticate_from_code(self, code: str, redirect_uri: str) -> bool:
        """
        Complete OAuth flow by exchanging authorization code for credentials.
        
        Args:
            code (str): Authorization code from OAuth callback
            redirect_uri (str): The redirect URI used in the initial request
            
        Returns:
            bool: True if authentication successful
        """
        try:
            if not self.flow:
                # Recreate flow if not available
                SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                          'https://www.googleapis.com/auth/gmail.modify']
                self.flow = Flow.from_client_secrets_file(
                    self.credentials_path,
                    scopes=SCOPES,
                    redirect_uri=redirect_uri
                )
            
            # Exchange authorization code for credentials
            self.flow.fetch_token(code=code)
            creds = self.flow.credentials
            
            # Save the credentials
            token_path = self.token_path or 'token.json'
            try:
                # Ensure token directory exists
                token_dir = os.path.dirname(token_path)
                if token_dir and not os.path.exists(token_dir):
                    os.makedirs(token_dir, exist_ok=True)
                    
                with open(token_path, 'w') as token:
                    token.write(creds.to_json())
                logger.info(f"Saved credentials to {token_path}")
            except Exception as e:
                logger.error(f"Failed to save token: {str(e)}")
                return False
            
            # Build Gmail service
            self.service = build('gmail', 'v1', credentials=creds)
            self.credentials = creds
            logger.info("Gmail API authentication successful")
            
            # Initialize required labels
            self._initialize_labels()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate from code: {str(e)}")
            return False
    
    def authenticate_from_token(self, token_path: Optional[str] = None) -> bool:
        """
        Authenticate using existing token file.
        
        Args:
            token_path (str): Path to token file (uses self.token_path if not provided)
            
        Returns:
            bool: True if authentication successful
        """
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                  'https://www.googleapis.com/auth/gmail.modify']
        
        token_file = token_path or self.token_path or 'token.json'
        
        if not os.path.exists(token_file):
            logger.error(f"Token file not found: {token_file}")
            return False
        
        try:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
            
            # Refresh if needed
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    # Save refreshed credentials
                    with open(token_file, 'w') as token:
                        token.write(creds.to_json())
                except Exception as e:
                    logger.error(f"Failed to refresh credentials: {str(e)}")
                    return False
            
            if not creds or not creds.valid:
                logger.error("Invalid credentials")
                return False
            
            # Build Gmail service
            self.service = build('gmail', 'v1', credentials=creds)
            self.credentials = creds
            logger.info("Gmail API authentication successful from token")
            
            # Initialize required labels
            self._initialize_labels()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to authenticate from token: {str(e)}")
            return False
    
    def _build_service(self):
        """
        Build Gmail API service from credentials.
        
        Returns:
            Gmail API service object
        """
        try:
            if self.credentials:
                service = build('gmail', 'v1', credentials=self.credentials)
                logger.info("Gmail API service built successfully")
                
                # Initialize required labels
                self._initialize_labels()
                
                return service
            else:
                logger.error("No credentials available to build service")
                return None
        except Exception as e:
            logger.error(f"Failed to build Gmail service: {str(e)}")
            return None
    
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
                label_id = self.create_label_if_not_exists(label_name, color)
                if label_id:
                    logger.info(f"Label '{label_name}' ready (ID: {label_id})")
            except Exception as e:
                logger.error(f"Failed to initialize label '{label_name}': {str(e)}")
    
    def start_monitoring(self, check_interval: int = 300) -> bool:
        """
        Start monitoring Gmail inbox for new emails.
        
        Args:
            check_interval (int): Interval between checks in seconds
            
        Returns:
            bool: True if monitoring started successfully
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
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(check_interval,),
            daemon=True
        )
        self.monitoring_thread.start()
        
        logger.info(f"Email monitoring started with {check_interval}s interval")
        return True
    
    def stop_monitoring(self) -> bool:
        """
        Stop monitoring Gmail inbox.
        
        Returns:
            bool: True if monitoring stopped successfully
        """
        if not self.monitoring_active:
            logger.warning("Email monitoring is not active")
            return True
        
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=10)
        
        logger.info("Email monitoring stopped")
        return True
    
    def _monitoring_loop(self, check_interval: int):
        """
        Main monitoring loop that runs in background thread.
        
        Args:
            check_interval (int): Interval between checks in seconds
        """
        logger.info("Email monitoring loop started")
        
        while self.monitoring_active:
            try:
                logger.info("Starting email check cycle...")
                
                # Check for new emails (not yet processed)
                new_emails = self._check_for_new_emails()
                logger.info(f"Email check completed, found {len(new_emails) if new_emails else 0} new emails")
                
                # Check for reprocess emails (manually marked for reprocessing)
                reprocess_emails = self._check_for_reprocess_emails()
                logger.info(f"Reprocess check completed, found {len(reprocess_emails) if reprocess_emails else 0} emails to reprocess")
                
                # Combine both lists for processing
                all_emails = []
                if new_emails:
                    all_emails.extend(new_emails)
                if reprocess_emails:
                    all_emails.extend(reprocess_emails)
                
                if all_emails:
                    logger.info(f"Processing {len(all_emails)} emails ({len(new_emails) if new_emails else 0} new, {len(reprocess_emails) if reprocess_emails else 0} reprocess)")
                    
                    # Initialize DuckDB service
                    db_service = DuckDBService()
                    if not db_service.connect():
                        logger.error("Failed to connect to DuckDB")
                        continue
                    
                    # Ensure table exists
                    db_service.create_table()
                    
                    for email_data in all_emails:
                        gmail_id = email_data.get('gmail_id')
                        
                        # Check if this is a reprocess email or a new email
                        is_reprocess = email_data.get('is_reprocess', False)
                        
                        # Check if email is already processed in database
                        existing_record = db_service.get_extraction(gmail_id)
                        
                        # Combine email body and attachment contents
                        combined_content = self._combine_email_content(email_data)
                        
                        if is_reprocess:
                            print(f"\n🔄 REPROCESSING EMAIL (Manager Request):")
                        else:
                            print(f"\n🔔 {'UPDATING' if existing_record else 'NEW'} EMAIL:")
                        print(f"Subject: {email_data.get('subject', 'No Subject')}")
                        print(f"From: {email_data.get('sender', 'Unknown')}")
                        print(f"Received: {email_data.get('received_at', 'Unknown')}")
                        print(f"Attachments: {len(email_data.get('attachments', []))} files")
                        
                        # Debug: Show content type and length
                        has_html = bool(email_data.get('body_html', '').strip())
                        has_text = bool(email_data.get('body_text', '').strip())
                        print(f"Content types: HTML={has_html}, Text={has_text}")
                        print(f"Combined content length: {len(combined_content)} characters")
                        
                        # Show first 500 characters of combined content for debugging
                        print(f"\n📄 COMBINED CONTENT PREVIEW:")
                        print("-" * 50)
                        print(combined_content[:500] + ("..." if len(combined_content) > 500 else ""))
                        print("-" * 50)
                        
                        # Process with AI extraction
                        print(f"\n🤖 AI EXTRACTION RESULT:")
                        print("=" * 80)
                        try:
                            extraction_result = extract_hardware_quotation_details(combined_content)
                            print(json.dumps(extraction_result, indent=2))
                            
                            # Handle labels for reprocess vs new emails
                            if is_reprocess:
                                # Remove reprocess label first
                                self.remove_label_from_email(gmail_id, "SnapQuote-Reprocess")
                                print(f"🏷️ Removed label: SnapQuote-Reprocess (Blue)")
                            
                            # Check if email is valid before saving to database
                            if extraction_result.get("status") == "NOT_VALID":
                                # Irrelevant email - only apply label, do NOT save to database
                                self.add_label_to_email(gmail_id, "SnapQuote-Irrelevant", "grey")
                                print(f"🏷️ Applied label: SnapQuote-Irrelevant (Grey)")
                                print(f"⚠️ Email marked as irrelevant - NOT saved to database")
                            else:
                                # Valid quotation - save to database and apply green label
                                if existing_record:
                                    success = db_service.update_extraction(gmail_id, extraction_result)
                                    if success:
                                        print(f"📝 Updated existing database record")
                                    else:
                                        print(f"❌ Failed to update database record")
                                else:
                                    record_id = db_service.insert_extraction(email_data, extraction_result)
                                    if record_id:
                                        print(f"💾 Saved to database with ID: {record_id}")
                                    else:
                                        print(f"❌ Failed to save to database")
                                
                                # Apply green label for valid quotations
                                self.add_label_to_email(gmail_id, "SnapQuote-Fetched", "green")
                                print(f"🏷️ Applied label: SnapQuote-Fetched (Green)")
                                
                        except Exception as e:
                            print(f"❌ AI extraction failed: {str(e)}")
                            logger.error(f"AI extraction error: {str(e)}")
                        
                        print("=" * 80)
                        print("✅ END OF PROCESSING\n")
                    
                    # Close database connection
                    db_service.disconnect()
                
                self.last_check_time = datetime.now()
                logger.info(f"Sleeping for {check_interval} seconds before next check...")
                time.sleep(check_interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {str(e)}")
                print(f"❌ Email monitoring error: {str(e)}")
                logger.info("Sleeping for 60 seconds due to error...")
                time.sleep(60)  # Wait longer on error
    
    def _check_for_new_emails(self):
        """
        Check Gmail inbox for new emails since last check.
        
        Returns:
            List[Dict]: List of new email data
        """
        try:
            # Build query for recent emails WITHOUT SnapQuote labels
            query = "in:inbox -label:SnapQuote-Fetched -label:SnapQuote-Irrelevant"
            if self.last_check_time:
                # Get emails newer than last check (within last hour for safety)
                hours_ago = max(1, int((datetime.now() - self.last_check_time).total_seconds() / 3600) + 1)
                query += f" newer_than:{hours_ago}h"
            else:
                # First run, get emails from last 1 hour
                query += " newer_than:1h"
            
            logger.info(f"Gmail API query: {query}")
            
            # Search for emails
            logger.info("Making Gmail API call...")
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=10
            ).execute()
            logger.info("Gmail API call completed")
            
            messages = results.get('messages', [])
            new_emails = []
            
            for message in messages:
                try:
                    email_data = self.get_email_details(message['id'])
                    if email_data:
                        new_emails.append(email_data)
                except Exception as e:
                    logger.error(f"Error processing email {message['id']}: {str(e)}")
                    continue
            
            return new_emails
            
        except HttpError as e:
            logger.error(f"Gmail API error: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error checking emails: {str(e)}")
            return []
    
    def _check_for_reprocess_emails(self):
        """
        Check Gmail for emails with SnapQuote-Reprocess label.
        These emails should be reprocessed regardless of time constraints.
        
        Returns:
            List[Dict]: List of email data marked for reprocessing
        """
        try:
            # Create the reprocess label if it doesn't exist
            self.create_label_if_not_exists("SnapQuote-Reprocess", "blue")
            
            # Build query for emails with reprocess label
            query = "label:SnapQuote-Reprocess"
            
            logger.info(f"Gmail reprocess query: {query}")
            
            # Search for emails
            logger.info("Making Gmail API call for reprocess emails...")
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=50
            ).execute()
            logger.info("Gmail reprocess API call completed")
            
            messages = results.get('messages', [])
            reprocess_emails = []
            
            for message in messages:
                try:
                    email_data = self.get_email_details(message['id'])
                    if email_data:
                        # Mark this as a reprocess email
                        email_data['is_reprocess'] = True
                        reprocess_emails.append(email_data)
                except Exception as e:
                    logger.error(f"Error processing reprocess email {message['id']}: {str(e)}")
                    continue
            
            return reprocess_emails
            
        except HttpError as e:
            logger.error(f"Gmail API error checking reprocess emails: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error checking reprocess emails: {str(e)}")
            return []
    
    def get_email_list(self, max_results: int = 100, query: str = "") -> List[Dict]:
        """
        Get list of emails from Gmail inbox.
        
        Args:
            max_results (int): Maximum number of emails to retrieve
            query (str): Gmail search query to filter emails
            
        Returns:
            List[Dict]: List of email metadata dictionaries
        """
        # TODO: Implement email list retrieval
        # - Use Gmail API to search emails
        # - Apply query filters
        # - Extract basic email metadata
        # - Return structured email data
        
        logger.info(f"Retrieving email list (max: {max_results}, query: '{query}')")
        
        # Mock response for now
        return []
    
    def get_email_details(self, email_id: str) -> Optional[Dict]:
        """
        Get detailed information about a specific email including attachments.
        
        Args:
            email_id (str): Gmail message ID
            
        Returns:
            Optional[Dict]: Email details or None if not found
        """
        if not self.service:
            logger.error("Gmail service not authenticated")
            return None
        
        try:
            # Get the email
            message = self.service.users().messages().get(
                userId='me', id=email_id, format='full'
            ).execute()
            
            # Extract headers
            headers = {}
            for header in message['payload'].get('headers', []):
                headers[header['name'].lower()] = header['value']
            
            # Extract body and attachments
            body_text = ""
            body_html = ""
            attachments = []
            
            def extract_parts(part):
                nonlocal body_text, body_html, attachments
                
                if part.get('mimeType') == 'text/plain':
                    data = part['body'].get('data', '')
                    if data:
                        body_text = base64.urlsafe_b64decode(data).decode('utf-8')
                        logger.info(f"Extracted plain text body: {len(body_text)} characters")
                elif part.get('mimeType') == 'text/html':
                    data = part['body'].get('data', '')
                    if data:
                        body_html = base64.urlsafe_b64decode(data).decode('utf-8')
                        logger.info(f"Extracted HTML body: {len(body_html)} characters")
                        # Check if HTML contains tables
                        table_count = body_html.lower().count('<table')
                        if table_count > 0:
                            logger.info(f"HTML contains {table_count} table(s)")
                elif 'parts' in part:
                    for subpart in part['parts']:
                        extract_parts(subpart)
                else:
                    # Check if this part is an attachment
                    filename = part.get('filename', '')
                    if filename and part['body'].get('attachmentId'):
                        # This is an attachment
                        attachment_info = {
                            'filename': filename,
                            'mimeType': part.get('mimeType', ''),
                            'size': part['body'].get('size', 0),
                            'attachmentId': part['body']['attachmentId']
                        }
                        attachments.append(attachment_info)
            
            # Handle both simple and multipart messages
            if 'parts' in message['payload']:
                for part in message['payload']['parts']:
                    extract_parts(part)
            else:
                extract_parts(message['payload'])
            
            # Process attachments and get their content
            attachment_contents = []
            for attachment in attachments:
                if self._is_supported_file(attachment['filename']):
                    content = self._get_attachment_content(email_id, attachment['attachmentId'])
                    if content:
                        processed_content = process_attachment(attachment['filename'], content)
                        print
                        attachment_contents.append(f"\n\n---\n# Attachment: {attachment['filename']}\n\n{processed_content}")
            
            # Convert timestamp
            timestamp = int(message['internalDate']) / 1000
            received_at = datetime.fromtimestamp(timestamp)
            
            return {
                'gmail_id': email_id,
                'subject': headers.get('subject', ''),
                'sender': headers.get('from', ''),
                'recipient': headers.get('to', ''),
                'received_at': received_at.isoformat(),
                'body_text': body_text,
                'body_html': body_html,
                'attachments': attachments,
                'attachment_contents': attachment_contents
            }
            
        except Exception as e:
            logger.error(f"Error retrieving email details: {str(e)}")
            return None
    
    def _is_supported_file(self, filename: str) -> bool:
        """Check if the file type is supported for processing."""
        supported_extensions = ['.pdf', '.xlsx', '.xls', '.docx', '.doc']
        return any(filename.lower().endswith(ext) for ext in supported_extensions)
    
    def _get_attachment_content(self, email_id: str, attachment_id: str) -> Optional[bytes]:
        """Get the content of an attachment."""
        try:
            attachment = self.service.users().messages().attachments().get(
                userId='me', messageId=email_id, id=attachment_id
            ).execute()
            
            data = attachment.get('data')
            if data:
                return base64.urlsafe_b64decode(data)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving attachment content: {str(e)}")
            return None
    
    def _combine_email_content(self, email_data: Dict) -> str:
        """
        Combine email body and attachment contents into one markdown string.
        
        Args:
            email_data (Dict): Email data containing body and attachments
            
        Returns:
            str: Combined content in markdown format
        """
        combined = []
        
        # Add email metadata
        combined.append("# Email Content\n")
        combined.append(f"**Subject:** {email_data.get('subject', 'No Subject')}\n")
        combined.append(f"**From:** {email_data.get('sender', 'Unknown')}\n")
        combined.append(f"**To:** {email_data.get('recipient', 'Unknown')}\n")
        combined.append(f"**Received:** {email_data.get('received_at', 'Unknown')}\n\n")
        
        # Add email body
        body_text = email_data.get('body_text', '').strip()
        body_html = email_data.get('body_html', '').strip()
        
        # Prioritize HTML content when available as it may contain tables
        if body_html:
            combined.append("## Email Body (HTML)\n\n")
            # Parse HTML and extract tables as markdown
            soup = BeautifulSoup(body_html, "html.parser")
            # Extract tables
            tables = soup.find_all("table")
            
            # Debug logging
            logger.info(f"Found {len(tables)} tables in HTML content")
            
            for i, table in enumerate(tables):
                logger.info(f"Processing table {i+1}")
                # Convert table to markdown
                rows = table.find_all("tr")
                table_data = []
                for row in rows:
                    cols = row.find_all(["td", "th"])
                    table_data.append([col.get_text(strip=True) for col in cols])
                
                if table_data:
                    combined.append(f"\n### Table {i+1}\n\n")
                    # Markdown table header
                    header = table_data[0] if table_data else []
                    if header:
                        md_table = "| " + " | ".join(header) + " |\n"
                        md_table += "| " + " | ".join(["---"] * len(header)) + " |\n"
                        for row in table_data[1:]:
                            # Pad row with empty cells if needed
                            while len(row) < len(header):
                                row.append("")
                            md_table += "| " + " | ".join(row[:len(header)]) + " |\n"
                        combined.append(md_table + "\n")
                        logger.info(f"Converted table {i+1} to markdown with {len(table_data)} rows")
                
                # Remove the table from soup so it doesn't get duplicated in text
                table.decompose()
            
            # Add the rest of the HTML as plain text
            clean_html = soup.get_text(separator="\n", strip=True)
            if clean_html:
                combined.append("### Additional Content\n\n")
                combined.append(f"{clean_html}\n\n")
            
        elif body_text:
            combined.append("## Email Body (Text)\n\n")
            combined.append(f"{body_text}\n\n")
        else:
            combined.append("## Email Body\n\n*[No text content]*\n\n")
        
        # Add attachment contents
        attachment_contents = email_data.get('attachment_contents', [])
        if attachment_contents:
            combined.append("# Attachments\n")
            for content in attachment_contents:
                combined.append(content)
        else:
            attachments = email_data.get('attachments', [])
            if attachments:
                combined.append("# Attachments\n\n")
                for attachment in attachments:
                    filename = attachment.get('filename', 'Unknown')
                    if not self._is_supported_file(filename):
                        combined.append(f"## Attachment: {filename}\n\n")
                        combined.append("*[Unsupported file type - content not extracted]*\n\n")
        
        return "".join(combined)

    def get_email_content(self, email_id: str) -> Optional[Dict]:
        """
        Get full content of a specific email (alias for get_email_details).
        
        Args:
            email_id (str): Gmail message ID
            
        Returns:
            Optional[Dict]: Email content data or None if not found
        """
        return self.get_email_details(email_id)
    
    def get_email_attachments(self, email_id: str) -> List[Dict]:
        """
        Get attachments from a specific email.
        
        Args:
            email_id (str): Gmail message ID
            
        Returns:
            List[Dict]: List of attachment metadata and content
        """
        # TODO: Implement attachment retrieval
        # - Extract attachment metadata from email
        # - Download attachment content
        # - Handle different attachment types
        # - Return attachment data with content
        
        logger.info(f"Retrieving attachments for email ID: {email_id}")
        
        return []
    
    def mark_email_processed(self, email_id: str, label: str = "PROCESSED") -> bool:
        """
        Mark an email as processed by adding a label.
        
        Args:
            email_id (str): Gmail message ID
            label (str): Label to add to the email
            
        Returns:
            bool: True if successfully labeled
        """
        # TODO: Implement email labeling
        # - Create custom labels if they don't exist
        # - Apply label to email
        # - Handle Gmail API errors
        # - Log labeling activity
        
        logger.info(f"Marking email {email_id} as processed with label: {label}")
        
        return True
    
    def get_quota_usage(self) -> Dict:
        """
        Get current Gmail API quota usage information.
        
        Returns:
            Dict: Quota usage statistics
        """
        # TODO: Implement quota monitoring
        # - Check current API usage
        # - Calculate remaining quota
        # - Return quota statistics
        
        return {
            'daily_limit': 1000000000,
            'used_today': 0,
            'remaining_today': 1000000000,
            'reset_time': (datetime.now() + timedelta(days=1)).isoformat()
        }
    
    def test_connection(self) -> bool:
        """
        Test Gmail API connection and permissions.
        
        Returns:
            bool: True if connection is working
        """
        # TODO: Implement connection test
        # - Make simple API call to verify connectivity
        # - Check required permissions
        # - Return connection status
        
        logger.info("Testing Gmail API connection")
        
        if not self.service:
            return False
        
        try:
            # Mock connection test for now
            return True
        except Exception as e:
            logger.error(f"Gmail API connection test failed: {str(e)}")
            return False
    
    def get_monitoring_status(self) -> Dict:
        """
        Get current monitoring status and statistics.
        
        Returns:
            Dict: Monitoring status information
        """
        return {
            'is_active': self.monitoring_active,
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'thread_alive': self.monitoring_thread.is_alive() if self.monitoring_thread else False,
            'emails_processed_today': 0,  # TODO: Get from database
            'last_error': None
        }
    
    def create_label_if_not_exists(self, label_name: str, color: str = 'grey') -> Optional[str]:
        """
        Create Gmail label if it doesn't exist.
        
        Args:
            label_name (str): Name of the label
            color (str): Color for the label ('green', 'grey', etc.)
            
        Returns:
            Optional[str]: Label ID if successful, None otherwise
        """
        if not self.service:
            logger.error("Gmail service not authenticated")
            return None
        
        try:
            # Check if label already exists
            labels = self.service.users().labels().list(userId='me').execute()
            for label in labels.get('labels', []):
                if label['name'] == label_name:
                    return label['id']
            
            # Gmail uses predefined color indices, not hex colors
            # Let's create labels without custom colors first, then we can modify them
            color_map = {
                'green': None,  # Will use default and modify later
                'grey': None,   # Will use default and modify later  
                'blue': None,   # Will use default and modify later
                'red': None,    # Will use default and modify later
            }
            
            # Create label without color first
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show'
            }
            
            # Add color only if it's specified and we have a valid mapping
            if color and color_map.get(color) is not None:
                label_object['color'] = color_map[color]
            
            created_label = self.service.users().labels().create(
                userId='me', body=label_object
            ).execute()
            
            logger.info(f"Created Gmail label: {label_name} with ID: {created_label['id']}")
            return created_label['id']
            
        except Exception as e:
            logger.error(f"Error creating Gmail label: {str(e)}")
            return None
    
    def add_label_to_email(self, email_id: str, label_name: str, color: str = 'grey') -> bool:
        """
        Add label to an email.
        
        Args:
            email_id (str): Gmail message ID
            label_name (str): Name of the label to add
            color (str): Color for the label if it needs to be created
            
        Returns:
            bool: True if label added successfully
        """
        if not self.service:
            logger.error("Gmail service not authenticated")
            return False
        
        try:
            # Get or create label
            label_id = self.create_label_if_not_exists(label_name, color)
            if not label_id:
                return False
            
            # Add label to email
            self.service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'addLabelIds': [label_id]}
            ).execute()
            
            logger.info(f"Added label '{label_name}' to email {email_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding label to email: {str(e)}")
            return False
    
    def remove_label_from_email(self, email_id: str, label_name: str) -> bool:
        """
        Remove label from an email.
        
        Args:
            email_id (str): Gmail message ID
            label_name (str): Name of the label to remove
            
        Returns:
            bool: True if label removed successfully
        """
        if not self.service:
            logger.error("Gmail service not authenticated")
            return False
        
        try:
            # Find the label ID by name
            labels = self.service.users().labels().list(userId='me').execute()
            label_id = None
            for label in labels.get('labels', []):
                if label['name'] == label_name:
                    label_id = label['id']
                    break
            
            if not label_id:
                logger.warning(f"Label '{label_name}' not found")
                return False
            
            # Remove label from email
            self.service.users().messages().modify(
                userId='me',
                id=email_id,
                body={'removeLabelIds': [label_id]}
            ).execute()
            
            logger.info(f"Removed label '{label_name}' from email {email_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing label from email: {str(e)}")
            return False
    
    def mark_email_for_reprocessing(self, email_id: str) -> bool:
        """
        Mark an email for reprocessing by adding the SnapQuote-Reprocess label.
        This is typically used by managers to request reprocessing of specific emails.
        
        Args:
            email_id (str): Gmail message ID
            
        Returns:
            bool: True if successfully marked for reprocessing
        """
        return self.add_label_to_email(email_id, "SnapQuote-Reprocess", "blue")
    
    def get_reprocess_queue(self) -> List[Dict]:
        """
        Get list of emails currently marked for reprocessing.
        
        Returns:
            List[Dict]: List of emails with SnapQuote-Reprocess label
        """
        return self._check_for_reprocess_emails()