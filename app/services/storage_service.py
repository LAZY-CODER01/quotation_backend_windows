import os
import logging
import json
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from app.services.duckdb_service import DuckDBService

logger = logging.getLogger(__name__)

class StorageService:
    def __init__(self):
        # Scopes must match what we requested in GmailService
        self.scopes = [
            'https://www.googleapis.com/auth/gmail.readonly', 
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/drive.file'
        ]
        
    def _get_drive_service(self):
        """
        Authenticate and return Google Drive service using company token from DB.
        """
        try:
            db = DuckDBService()
            if not db.connect():
                logger.error("DB connection failed for Drive auth")
                return None
            
            token_json = db.get_company_token()
            db.disconnect()
            
            if not token_json:
                logger.error("No company token found for Drive auth")
                return None
            
            token_info = json.loads(token_json)
            creds = Credentials.from_authorized_user_info(token_info, self.scopes)
            
            # Note: We don't handle auto-refresh here because we expect 
            # the token to be valid or refreshed by the periodic GmailService checks.
            # However, for robustness, we could try to refresh.
            if creds and creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                try:
                    creds.refresh(Request())
                    # We should save the refreshed token back to DB
                    db = DuckDBService()
                    if db.connect():
                        db.save_company_token(creds.to_json())
                        db.disconnect()
                except Exception as e:
                    if "invalid_scope" in str(e):
                        logger.error("  SCOPE MISMATCH: The stored token is missing required permissions (likely Google Drive). Please RE-AUTHENTICATE in the Admin Panel.")
                    else:
                        logger.error(f"Failed to refresh credentials in StorageService: {e}")
                    # Continue anyway? It might fail.
                    # If refresh failed due to scope, the token is useless.
                    return None
            
            service = build('drive', 'v3', credentials=creds)
            return service
            
        except Exception as e:
            logger.error(f"Drive service build failed: {e}")
            return None

    def _get_or_create_folder(self, service, folder_name, parent_id=None):
        """
        Finds or creates a folder with the given name.
        """
        try:
            # Query to check if folder exists
            query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
                
            results = service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            
            if files:
                return files[0]['id']
                
            # Create folder if not found
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
                
            folder = service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')
            
        except Exception as e:
            logger.error(f"Error getting/creating folder '{folder_name}': {e}")
            return None

    def _ensure_folder_path(self, service, path):
        """
        Ensures the full folder path exists (e.g., 'snapquote/tickets/123').
        Returns the ID of the final folder.
        """
        parts = path.strip('/').split('/')
        parent_id = None
        
        for part in parts:
            if not part: continue
            parent_id = self._get_or_create_folder(service, part, parent_id)
            if not parent_id:
                return None # Failed to create part of the path
                
        return parent_id

    def upload_file(self, file_obj, folder="uploads", public_id=None):
        """
        Uploads a file to Google Drive.
        
        Args:
            file_obj: The file object (FileStorage or similar)
            folder: The target folder path (string)
            public_id: Ignored for Drive (Drive generates IDs)
            
        Returns:
            str: The webViewLink (URL) of the uploaded file
        """
        service = self._get_drive_service()
        if not service:
            return None
            
        try:
            # 1. Resolve Parent Folder ID
            folder_id = self._ensure_folder_path(service, folder)
            if not folder_id:
                logger.error(f"Failed to resolve folder path: {folder}")
                return None
            
            # 2. Prepare File Metadata
            # Use original filename if available
            filename = getattr(file_obj, 'filename', 'unknown_file')
            mimetype = getattr(file_obj, 'content_type', 'application/octet-stream')
            
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            # 3. Create Media Upload
            # Use the underlying stream if available (for Flask FileStorage)
            stream = getattr(file_obj, 'stream', file_obj)

            # Ensure we are at the start of the file
            if hasattr(stream, 'seek'):
                stream.seek(0)
                
            media = MediaIoBaseUpload(stream, mimetype=mimetype, resumable=True)
            
            # 4. Upload
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink, webContentLink'
            ).execute()
            
            file_id = file.get('id')
            
            # 5. Set Permissions (Anyone with link can read)
            # This mimics Cloudinary's public URL behavior
            try:
                permission = {
                    'type': 'anyone',
                    'role': 'reader',
                }
                service.permissions().create(
                    fileId=file_id,
                    body=permission
                ).execute()
            except Exception as e:
                logger.warning(f"Failed to set public permission for file {file_id}: {e}")
                
            # Return webContentLink (direct download) as requested
            return file.get('webContentLink')
            
        except Exception as e:
            logger.exception(f"Drive upload failed: {e}")
            return None

    # Alias to match old API if needed (though backend_app uses upload_file)
    def upload_document(self, file_obj, filename, folder="documents"):
        # Create a wrapper specific for document if needed
        # But for now, we map it to upload_file
        # We might need to manually set filename if file_obj doesn't have it
        # But usually file_obj is the request.files['file']
        
        url = self.upload_file(file_obj, folder=folder)
        if url:
             return {
                "url": url,
                "public_id": "google_drive_file", # Dummy
                "format": "unknown"
            }
        return None

    def delete_excel(self, public_id): # For backward compatibility
        return self.delete_file(public_id)

    def delete_file(self, file_id_or_url):
        """
        Deletes a file from Drive.
        Args:
           file_id_or_url: Can be the Drive File ID or the full URL
        """
        service = self._get_drive_service()
        if not service: return None
        
        try:
            file_id = file_id_or_url
            
            # Basic URL parsing to extract ID if URL is passed
            if 'drive.google.com' in str(file_id_or_url):
                # Try simple extraction
                # https://drive.google.com/file/d/FILE_ID/view
                import re
                match = re.search(r'/d/([a-zA-Z0-9_-]+)', file_id_or_url)
                if match:
                    file_id = match.group(1)
            
            service.files().delete(fileId=file_id).execute()
            return True
        except Exception as e:
            logger.error(f"Drive delete failed: {e}")
            return None
