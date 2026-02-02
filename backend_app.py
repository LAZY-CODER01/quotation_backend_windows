"""
QuoteSnap - Gmail Email Monitor (JWT Refactor)
Dockerized Flask application with JWT Auth and Single Company Gmail Monitoring.
"""

import logging
import os
import threading
import json
import uuid
from datetime import datetime
from flask import Flask, jsonify, send_file, request, current_app
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.security import check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import generate_password_hash
# Services and Config cdc
from app.services.gmail_service import GmailService
from app.services.duckdb_service import DuckDBService
from app.services.new_excel_generation import ExcelGenerationService
from config.settings import Config
from app.auth.jwt_utils import create_jwt
from app.auth.jwt_required import jwt_required
from app.services.storage_service import StorageService

from werkzeug.utils import secure_filename
from app.extensions import socketio



# Load environment variables
load_dotenv()

# Global State for SINGLE Company Gmail Monitoring
company_gmail_service = None
monitoring_thread = None
monitoring_active = False

logger = logging.getLogger(__name__)

def start_company_gmail_monitoring():
    db = DuckDBService()
    if not db.connect():
        print("❌ DB connection failed for Gmail startup")
        return

    # Check for company token (ID=1)
    token_json = db.get_company_token()
    db.disconnect()

    if not token_json:
        print("⚠️ Company Gmail not connected yet")
        return

    gmail = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)

    try:
        if gmail.authenticate_from_info(json.loads(token_json)):
             # Only start if auth success
            interval = Config.EMAIL_CHECK_INTERVAL
            gmail.start_monitoring(check_interval=interval)
            print(f"✅ Company Gmail monitoring started with {interval}s interval")
        else:
            print("❌ Gmail authentication failed (Initial Startup)")
    except Exception as e:
        print(f"❌ Error starting Gmail monitoring: {e}")

# 🔥 AUTO START ON APP BOOT
# 🔥 AUTO START moved to create_flask_app


def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def create_flask_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    # Load Config
    app.config.from_object(Config)

    # CORS CONFIGURATION (No credentials needed for JWT usually, but good to have)
    CORS(app,
         origins=app.config.get('CORS_ORIGINS'),
         allow_headers=["Content-Type", "Authorization"],
         methods=["GET", "POST", "OPTIONS", "DELETE", "PUT"])
    
    socketio.init_app(app)

    # -------------------------------------------------------------------------
    # AUTHENTICATION ROUTES
    # -------------------------------------------------------------------------

    @app.route('/api/auth/login', methods=['POST'])
    def login():
        """
        User login endpoint.
        Returns JWT token if credentials are valid.
        """
        try:
            data = request.get_json()
            username = data.get('username')
            password = data.get('password')

            if not username or not password:
                return jsonify({"error": "Username and password required"}), 400

            db = DuckDBService()
            if not db.connect(): 
                return jsonify({"error": "Database connection failed"}), 500

            user = db.get_user_by_username(username)
            db.disconnect()

            if user and check_password_hash(user['password_hash'], password):
                token = create_jwt(user)
                return jsonify({
                    "success": True,
                    "token": token,
                    "user": {
                        "id": user['id'],
                        "username": user['username'],
                        "role": user['role']
                    }
                })
            
            return jsonify({"error": "Invalid credentials"}), 401

        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            return jsonify({"error": "Login failed"}), 500

    @app.route('/api/auth/me', methods=['GET'])
    @jwt_required()
    def get_current_user():
        """Get current user info from token."""
        return jsonify({
            "success": True,
            "user": request.user
        })

    # -------------------------------------------------------------------------
    # ADMIN GMAIL ROUTES (Company Account)
    # -------------------------------------------------------------------------

    @app.route('/api/admin/gmail/status', methods=['GET'])
    @jwt_required(roles=['ADMIN'])
    def gmail_status():
        """Check if Company Gmail is connected/monitoring."""
        global monitoring_active
        
        db = DuckDBService()
        if db.connect():
            token_json = db.get_company_token()
            db.disconnect()
            is_connected = token_json is not None
        else:
            is_connected = False
            
        return jsonify({
            "connected": is_connected,
            "monitoring": monitoring_active,
            "company_gmail_id": Config.COMPANY_GMAIL_ID
        })

    @app.route('/api/admin/gmail/connect', methods=['GET'])
    @jwt_required(roles=['ADMIN'])
    def connect_gmail():
        """Generate OAuth URL for Company Gmail connection."""
        service = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)
        # Use a random state for CSRF, but since we are stateless JWT, 
        # we might just pass a static state or handle it on frontend.
        # Ideally, we should check this state in callback. 
        # For simplicity in this refactor, we'll use a simple state we can verify if needed,
        # or just rely on Admin role protection.
        state = "company_connect_state" 
        
        auth_url = service.get_authorization_url(
            redirect_uri=Config.OAUTH_REDIRECT_URI,
            state=state
        )
        
        if auth_url:
            return jsonify({"authorization_url": auth_url})
        return jsonify({"error": "Failed to generate auth URL"}), 500

    @app.route('/api/admin/gmail/callback', methods=['GET'])
    def gmail_callback():
        # 1. Read 'code' and 'error' from query params
        code = request.args.get("code")
        error = request.args.get("error")
        
        if error:
            return jsonify({"error": f"OAuth error: {error}"}), 400
        if not code:
            return jsonify({"error": "Missing code"}), 400
            
        try:
            # 2. Exchange code for Company Token
            service = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)
            
            # This method saves the token to DB (id=1) 
            success = service.exchange_and_save_company_token(
                code=code, 
                redirect_uri=Config.OAUTH_REDIRECT_URI
            )
            
            if success:
                # 3. Start monitoring immediately
                if not service.monitoring_active:
                     service.start_monitoring(Config.EMAIL_CHECK_INTERVAL)
                
                return jsonify({"success": True, "message": "Company Gmail connected and monitoring started"})
            else:
                return jsonify({"error": "Authentication/Token Exchange failed"}), 500
                
        except Exception as e:
            logger.error(f"Callback error: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route('/api/admin/gmail/disconnect', methods=['POST'])
    @jwt_required(roles=['ADMIN'])
    def disconnect_gmail():
        """Disconnect company Gmail and stop monitoring."""
        global monitoring_active, company_gmail_service
        
        try:
            # Stop monitoring
            if company_gmail_service:
                company_gmail_service.stop_monitoring()
            monitoring_active = False
            company_gmail_service = None
            
            # Remove token from DB
            db = DuckDBService()
            if db.connect():
                db.delete_user_token(Config.COMPANY_GMAIL_ID)
                db.disconnect()
                
            return jsonify({"success": True, "message": "Disconnected Company Gmail"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # -------------------------------------------------------------------------
    # CORE API ROUTES (Protected)
    # -------------------------------------------------------------------------

    @app.route('/api/emails', methods=['GET'])
    @jwt_required() # Any role can view emails
    def get_all_emails():
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            
            # Extract query params
            status_filter = request.args.get('status')
            page = int(request.args.get('page', 1)) 
            limit = int(request.args.get('limit', 50))
            offset = (page - 1) * limit
            
            # Extract User Context
            current_user = request.user
            user_role = current_user.get('role', 'user')
            username = current_user.get('username')

            extractions = db_service.get_all_extractions(
                limit=limit,
                offset=offset,
                status_filter=status_filter,
                user_role=user_role, 
                username=username
            )
            count = len(extractions) # valid for this page
            # Ideally we should also return total_count for pagination UI
            total_count = db_service.get_total_count(status_filter, user_role, username)
            
            db_service.disconnect()
            
            return jsonify({
                'success': True,
                'count': count,
                'total': total_count,
                'page': page,
                'limit': limit,
                'data': extractions
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/emails/stats', methods=['GET'])
    @jwt_required()
    def get_email_stats():
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            
            total = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()[0]
            valid = db_service.connection.execute("SELECT COUNT(*) FROM email_extractions WHERE extraction_status = 'VALID'").fetchone()[0]
            irrelevant = db_service.connection.execute("SELECT COUNT(*) FROM email_extractions WHERE extraction_status = 'IRRELEVANT'").fetchone()[0]
            
            db_service.disconnect()
            return jsonify({
                'success': True,
                'stats': {
                    'total_emails': total,
                    'valid_quotations': valid,
                    'irrelevant_emails': irrelevant
                }
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/quotation/generate/<gmail_id>', methods=['GET'])
    @jwt_required()
    def generate_quotation(gmail_id: str):
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
                
            extraction_data = db_service.get_extraction(gmail_id)
            db_service.disconnect()
            
            if not extraction_data:
                return jsonify({'error': 'Extraction not found'}), 404
            
            if extraction_data.get('extraction_status') != 'VALID':
                return jsonify({'error': 'Cannot generate quotation for irrelevant email'}), 400
                
            excel_service = ExcelGenerationService()
            output_file = excel_service.generate_quotation_excel(gmail_id, extraction_data)
            
            if not output_file:
                return jsonify({'error': 'Failed to generate Excel'}), 500
                
            # Create filename
            subject = extraction_data.get('subject', 'quotation')[:30]
            clean_subject = "".join(c for c in subject if c.isalnum() or c in (' ', '-', '_')).rstrip()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            download_filename = f"Quotation_{clean_subject}_{timestamp}.xlsx"

            return send_file(
                os.path.abspath(output_file),
                as_attachment=True,
                download_name=download_filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                max_age=0
            )
        except Exception as e:
            logger.error(f"Generate error: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/quotation/download/<filename>', methods=['GET'])
    # Need token even for download? Yes, usually.
    # Since download links might be clicked directly, passing header is hard.
    # Usually we use a short-lived token in query param or just allow if token is in query param.
    # For now, let's enforce header (assuming frontend downloads via blob/fetch with header).
    # OR allow query param 'token'.
    # Let's support query param for this specific route.
    def download_quotation(filename: str):
        token = request.args.get('token')
        auth_header = request.headers.get('Authorization')
        
        # Simple manual check since decorator might not support query param easily
        actual_token = None
        if auth_header and auth_header.startswith("Bearer "):
            actual_token = auth_header.split(" ")[1]
        elif token:
            actual_token = token
            
        if not actual_token:
            return jsonify({"error": "Unauthorized"}), 401
            
        # Verify token (manual verification to support query param)
        try:
            import jwt
            jwt.decode(actual_token, app.config['JWT_SECRET'], algorithms=[app.config['JWT_ALGORITHM']])
        except:
             return jsonify({"error": "Invalid token"}), 401

        # File logic
        try:
            file_path = os.path.join('generated', filename)
            if not os.path.exists(file_path):
                return jsonify({'error': 'File not found'}), 404
                
            # Security check
            if '..' in filename or not filename.endswith('.xlsx'):
                return jsonify({'error': 'Invalid filename'}), 400
                
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
            logging.error(f"Download error: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/database/clear', methods=['POST'])
    @jwt_required(roles=['ADMIN'])
    def clear_database():
        try:
            db = DuckDBService()
            if db.connect():
                db.connection.execute('DELETE FROM email_extractions')
                db.disconnect()
                return jsonify({'success': True, 'message': 'Database cleared'})
            return jsonify({'error': 'DB connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/requirement/delete', methods=['POST'])
    @jwt_required(roles=['ADMIN', 'EMPLOYEE']) # Employees can delete requirements too?
    def delete_requirement():
        # Keep existing logic...
        try:
            data = request.get_json(force=True)
            # ... (Logic from old file)
            # Re-implementing briefly for brevity or copy-paste:
            gmail_id = data.get('gmail_id')
            index = data.get('index')
            if not gmail_id or index is None:
                return jsonify({'error': 'Missing params'}), 400
                
            db = DuckDBService()
            if not db.connect():
                return jsonify({'error': 'DB fail'}), 500
                
            extraction = db.get_extraction(gmail_id)
            if not extraction:
                db.disconnect()
                return jsonify({'error': 'Not found'}), 404
                
            res = extraction.get('extraction_result', {})
            req_key = next((k for k in res.keys() if k.lower() == 'requirements'), None)
            if not req_key or not isinstance(res[req_key], list):
                db.disconnect()
                return jsonify({'error': 'No requirements'}), 400
                
            idx = int(index)
            if 0 <= idx < len(res[req_key]):
                removed = res[req_key].pop(idx)
                db.update_extraction(gmail_id, res)
                db.disconnect()
                return jsonify({'success': True, 'removed': removed, 'requirements': res[req_key]})
            
            db.disconnect()
            return jsonify({'error': 'Index out of bounds'}), 400
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/ticket/update-priority', methods=['POST'])
    @jwt_required()
    def update_priority():
        try:
            data = request.get_json()
            gmail_id = data.get('gmail_id')
            priority = data.get('priority')

            if not gmail_id or priority not in ['NORMAL', 'URGENT']:
                return jsonify({'error': 'Invalid parameters'}), 400

            db = DuckDBService()
            if db.connect():
                success = db.update_ticket_priority(gmail_id, priority)
                if success:
                    # ✅ Log Activity
                    try:
                        db.add_activity_log(
                            gmail_id, 
                            "PRIORITY_CHANGE", 
                            f"Priority changed to {priority}", 
                            request.user.get('username', 'System')
                        )
                    except Exception as log_err:
                        logger.error(f"Logging failed: {log_err}")
                        
                    db.disconnect() # 👈 Disconnect AFTER logging
                    return jsonify({'success': True, 'priority': priority})
                else:
                    db.disconnect()
                    return jsonify({'error': 'Failed to update priority'}), 500
            
            return jsonify({'error': 'Database connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/ticket/update-status', methods=['POST'])
    @jwt_required()
    def update_status():
        try:
            data = request.get_json()
            gmail_id = data.get('gmail_id')
            status = data.get('status')

            if not gmail_id or not status:
                return jsonify({'error': 'Invalid parameters'}), 400

            db = DuckDBService()
            if db.connect():
                db.connection.execute("""
                    UPDATE email_extractions 
                    SET ticket_status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE gmail_id = ?
                """, [status, gmail_id])
                
                
                
                db.connection.commit()

                # ✅ Log Activity
                try:
                    db.add_activity_log(
                        gmail_id, 
                        "STATUS_CHANGE", 
                        f"Status changed to {status}", 
                        request.user.get('username', 'System')
                    )
                except Exception as log_err:
                    logger.error(f"Logging failed: {log_err}")

                db.disconnect()
                
                return jsonify({'success': True, 'status': status})
            
            return jsonify({'error': 'Database connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500 
   
    @app.route('/api/ticket/<ticket_number>/status', methods=['PUT'])
    @jwt_required()
    def update_ticket_status_rbac(ticket_number):
        """
        Update ticket workflow status with RBAC.
        - Admin: Can set any status (OPEN, CLOSED, ORDER_COMPLETED, etc.)
        - User: Can ONLY set status to COMPLETION_REQUESTED
        """
        try:
            data = request.get_json()
            new_status = data.get('status')
            
            if not new_status:
                return jsonify({'error': 'Status is required'}), 400

            db = DuckDBService()
            if not db.connect():
                return jsonify({'error': 'Database connection failed'}), 500

            # 1. Resolve ticket_number to gmail_id for logging
            gmail_id = db.get_gmail_id_from_ticket(ticket_number)
            if not gmail_id:
                db.disconnect()
                return jsonify({'error': 'Ticket not found'}), 404

            # 2. RBAC Logic
            user_role = request.user.get('role', 'user')
            
            if user_role != 'ADMIN':
                # Standard User Restrictions
                if new_status not in ['COMPLETION_REQUESTED']:
                    db.disconnect()
                    return jsonify({'error': 'Unauthorized: Only Admins can close/complete tickets.'}), 403
            
            # 3. Update Status
            success = db.update_ticket_status(ticket_number, new_status)
            
            if success:
                # 4. Log Activity
                try:
                    db.add_activity_log(
                        gmail_id, 
                        "STATUS_CHANGE", 
                        f"Status changed to {new_status}", 
                        request.user.get('username', 'System')
                    )
                except Exception as log_err:
                    logger.error(f"Logging failed: {log_err}")
                
                db.disconnect()
                return jsonify({'success': True, 'status': new_status})
            else:
                db.disconnect()
                return jsonify({'error': 'Failed to update status'}), 500

        except Exception as e:
            return jsonify({'error': str(e)}), 500 
   

    @app.route('/api/ticket/update-file-amount', methods=['POST'])
    @jwt_required()
    def update_file_amount():
        try:
            data = request.get_json()
            db = DuckDBService()
            if db.connect() and db.update_file_amount(data.get('gmail_id'), data.get('file_id'), data.get('amount')):
                db.disconnect()
                return jsonify({'success': True})
            return jsonify({'error': 'Failed'}), 500
        except Exception as e: return jsonify({'error': str(e)}), 500    
    @app.route('/api/ticket/update-requirements', methods=['POST'])
    @jwt_required()
    def update_requirements():
        try:
            data = request.get_json()
            gmail_id = data.get('gmail_id')
            new_requirements = data.get('requirements') # List of items

            if not gmail_id or new_requirements is None:
                return jsonify({'error': 'Invalid data'}), 400

            db = DuckDBService()
            if db.connect():
                # 1. Fetch existing record to preserve other data
                extraction = db.get_extraction(gmail_id)
                if not extraction:
                    db.disconnect()
                    return jsonify({'error': 'Ticket not found'}), 404

                # 2. Update ONLY the requirements list inside the JSON
                current_result = extraction.get('extraction_result', {})
                current_result['Requirements'] = new_requirements
                
                # 3. Save to DB
                success = db.update_extraction(gmail_id, current_result)
                db.disconnect()
                
                if success:
                    return jsonify({'success': True})
                else:
                    return jsonify({'error': 'Failed to save'}), 500
            
            return jsonify({'error': 'DB connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500   
    @app.route('/api/ticket/upload-quotation', methods=['POST'])
    @jwt_required()
    def upload_quotation_file():
        try:
            if 'file' not in request.files:
                return jsonify({'error': 'No file part'}), 400
            
            file = request.files['file']
            gmail_id = request.form.get('gmail_id')
            amount = request.form.get('amount', '')
            
            if file.filename == '' or not gmail_id:
                return jsonify({'error': 'No selected file or Ticket ID'}), 400

            # Optional: Check extensions
            # if not allowed_file(file.filename): ...

            filename = secure_filename(file.filename)
            
            # Use UUID for unique filename in Cloudinary
            unique_id = uuid.uuid4().hex
            folder_path = f"snapquote/tickets/{gmail_id}"
            
            # ✅ Upload to Cloudinary
            storage = StorageService()
            file_url = storage.upload_file(
                file, 
                folder=folder_path,
                public_id=unique_id
            )

            if not file_url:
                return jsonify({'error': 'Cloud upload failed'}), 500

            # Prepare Metadata for DB
            file_id = str(uuid.uuid4())
            file_data = {
                "id": file_id,
                "name": filename,
                "url": file_url,      # Cloudinary URL
                "amount": amount,
                "uploaded_at": datetime.now().isoformat()
            }

            # Save Metadata to DuckDB
            db = DuckDBService()
            if db.connect():
                success = db.add_quotation_file(gmail_id, file_data)
                if success:
                    # ✅ Log Activity
                    try:
                        db.add_activity_log(
                            gmail_id, 
                            "QUOTATION_UPLOAD", 
                            f"Uploaded quotation: {filename}", 
                            request.user.get('username', 'System'),
                            metadata={"file_id": file_id, "amount": amount}
                        )
                    except Exception as log_err:
                        logger.error(f"Logging failed: {log_err}")

                    db.disconnect() # 👈 Disconnect AFTER logging
                    return jsonify({'success': True, 'file': file_data})
            
            db.disconnect()
            return jsonify({'error': 'Database save failed'}), 500
            
        except Exception as e:
            # logger.error(f"Upload error: {e}")
            return jsonify({'error': str(e)}), 500  
    @app.route('/api/ticket/upload-cpo', methods=['POST'])
    @jwt_required()
    def upload_cpo_file():
       try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        
        file = request.files['file']
        gmail_id = request.form.get('gmail_id')
        po_number = request.form.get('po_number', '')
        amount = request.form.get('amount', '0') # 👈 Capture Amount

        if file.filename == '' or not gmail_id:
            return jsonify({'error': 'No selected file or Ticket ID'}), 400

        filename = secure_filename(file.filename)
        unique_id = uuid.uuid4().hex
        # Save to a specific CPO folder in cloud
        folder_path = f"snapquote/cpo/{gmail_id}" 
        
        storage = StorageService()
        file_url = storage.upload_file(file, folder=folder_path, public_id=unique_id)

        if not file_url:
            return jsonify({'error': 'Cloud upload failed'}), 500

        file_id = str(uuid.uuid4())
        file_data = {
            "id": file_id,
            "name": filename,
            "url": file_url,
            "po_number": po_number,
            "amount": amount,
            "uploaded_at": datetime.now().isoformat()
        }

        db = DuckDBService()
        if db.connect():
            success = db.add_cpo_file(gmail_id, file_data)
            if success:
                # ✅ Log Activity
                try:
                    db.add_activity_log(
                        gmail_id, 
                        "CPO_UPLOAD", 
                        f"Uploaded CPO: {filename}", 
                        request.user.get('username', 'System'),
                        metadata={"file_id": file_id, "po_number": po_number}
                    )
                except Exception as log_err:
                    logger.error(f"Logging failed: {log_err}")

                db.disconnect() # 👈 Disconnect AFTER logging
                return jsonify({'success': True, 'file': file_data})
        
        db.disconnect()
        return jsonify({'error': 'Database save failed'}), 500
        
       except Exception as e:
        return jsonify({'error': str(e)}), 500    
    # Inside app.py

    @app.route('/api/ticket/add-note', methods=['POST'])
    @jwt_required()
    def add_internal_note():
        try:
            data = request.get_json()
            gmail_id = data.get('gmail_id')
            text = data.get('text')
            
            if not gmail_id or not text:
                return jsonify({'error': 'Missing gmail_id or text'}), 400

            # Create Note Object
            note_data = {
                "id": str(uuid.uuid4()),
                "text": text,
                "author": request.user.get('username', 'Unknown'), # Get username from JWT
                "created_at": datetime.now().isoformat()
            }

            db = DuckDBService()
            if db.connect():
                success = db.add_internal_note(gmail_id, note_data)
                if success:
                    # ✅ Log Activity
                    try:
                        db.add_activity_log(
                            gmail_id, 
                            "NOTE_ADDED", 
                            "Internal note added", 
                            request.user.get('username', 'System'),
                            metadata={"note_id": note_data['id']}
                        )
                    except Exception as log_err:
                        logger.error(f"Logging failed: {log_err}")
                    
                    db.disconnect() # 👈 Disconnect AFTER logging
                    return jsonify({'success': True, 'note': note_data})
            
            db.disconnect()
            return jsonify({'error': 'Database connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500   
    @app.route('/api/admin/create-user', methods=['POST'])
    @jwt_required(roles=['ADMIN']) # 👈 Ensures only Admins can access this
    def create_user():
        try:
            data = request.get_json()
            username = data.get('username')
            password = data.get('password')
            employee_code = data.get('employee_code')
            role = data.get('role', 'user') # Default to 'user' if not specified

            # Validation
            if not all([username, password, employee_code]):
                return jsonify({'error': 'Username, Password, and Employee Code are required'}), 400

            db = DuckDBService()
            if not db.connect():
                return jsonify({'error': 'Database connection failed'}), 500

            # Hash the password
            password_hash = generate_password_hash(password)

            # Create the user
            user_id = db.create_user(username, password_hash, employee_code, role)
            db.disconnect()

            if user_id:
                return jsonify({
                    'success': True, 
                    'message': f'User {username} created successfully',
                    'user_id': user_id,
                    'employee_code': employee_code
                })
            else:
                return jsonify({'error': 'Failed to create user. Username or Employee Code may already exist.'}), 409

        except Exception as e:
            logger.error(f"Create user error: {e}")
            return jsonify({'error': str(e)}), 500      
    @app.route('/api/users/list', methods=['GET'])
    @jwt_required()
    def get_users_list():
        try:
            db = DuckDBService()
            if db.connect():
                users = db.get_all_users_list()
                db.disconnect()
                return jsonify({'success': True, 'users': users})
            return jsonify({'error': 'DB connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # 2. Endpoint to Assign Ticket
    @app.route('/api/ticket/assign', methods=['POST'])
    @jwt_required(roles=['ADMIN'])
    def assign_ticket():
        try:
            data = request.get_json()
            gmail_id = data.get('gmail_id')
            assigned_to = data.get('assigned_to') # Username

            if not gmail_id:
                return jsonify({'error': 'Missing Ticket ID'}), 400

            db = DuckDBService()
            if db.connect():
                # 1. Perform Assignment
                success = db.assign_ticket(gmail_id, assigned_to)
                
                log_entry = None
                if success:
                    # ✅ FIX: Pass arguments individually, NOT as a dictionary
                    description = f"Assigned ticket to {assigned_to}" if assigned_to else "Unassigned ticket"
                    
                    log_entry = db.add_activity_log(
                        gmail_id=gmail_id,
                        action="ASSIGNMENT_CHANGE",
                        description=description,
                        user=request.user.get('username')
                    )

                db.disconnect()
                
                if success:
                    # Return the log_entry so frontend updates instantly
                    return jsonify({'success': True, 'log': log_entry})
                else:
                    return jsonify({'error': 'Failed to assign'}), 500
            
            return jsonify({'error': 'DB Connection failed'}), 500
        except Exception as e:
            return jsonify({'error': str(e)}), 500 
    @app.route('/api/health')
    def health():
        return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})

    # Proxy Fix for Docker/Reverse Proxy
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    
    # Start Monitoring on App Startup
    # Start Monitoring on App Startup
    start_company_gmail_monitoring()

    
    return app

    


app = create_flask_app()   

if __name__ == '__main__':
    # This block is for local dev only
    setup_logging()
  
    socketio.run(app, host='0.0.0.0', port=5001, debug=Config.DEBUG)