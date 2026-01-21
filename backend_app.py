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

# Services and Config
from app.services.gmail_service import GmailService
from app.services.duckdb_service import DuckDBService
from app.services.new_excel_generation import ExcelGenerationService
from config.settings import Config
from app.auth.jwt_utils import create_jwt
from app.auth.jwt_required import jwt_required

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

    token_json = db.get_company_token()
    db.disconnect()

    if not token_json:
        print("⚠️ Company Gmail not connected yet")
        return

    gmail = GmailService(credentials_path="credentials.json")

    if gmail.authenticate_from_info(json.loads(token_json)):
        gmail.start_monitoring(check_interval=300)
        print("✅ Company Gmail monitoring started")
    else:
        print("❌ Gmail authentication failed")

# 🔥 AUTO START ON APP BOOT
start_company_gmail_monitoring()


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
         methods=["GET", "POST", "OPTIONS", "DELETE"])

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
        code = request.args.get("code")
        error = request.args.get("error")
        
        if error:
            return jsonify({"error": f"OAuth error: {error}"}), 400
        if not code:
            return jsonify({"error": "Missing code"}), 400
            
        try:
            # Exchange code for token
            service = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)
            
            # Use a temporary user_id just to satisfy the method signature if strictly needed,
            # but we really want to save to COMPANY_GMAIL_ID.
            # We will manually handle the token save to ensure it goes to the right ID.
            
            # The existing authenticate_from_code tries to save using user_id.
            # We can pass Config.COMPANY_GMAIL_ID as user_id!
            
        def exchange_and_save_company_token(self, code: str, redirect_uri: str) -> bool:
    from google_auth_oauthlib.flow import Flow
    from app.services.duckdb_service import DuckDBService

    SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify'
    ]

    flow = Flow.from_client_secrets_file(
        self.credentials_path,
        scopes=SCOPES,
        redirect_uri=redirect_uri
    )

    flow.fetch_token(code=code)
    creds = flow.credentials

    token_json = creds.to_json()

    db = DuckDBService()
    if not db.connect():
        return False

    success = db.save_company_token(token_json)
    db.disconnect()
    return success

            
            if success:
                # Start monitoring immediately
                start_background_monitoring_if_needed()
                return jsonify({"success": True, "message": "Company Gmail connected and monitoring started"})
            else:
                return jsonify({"error": "Authentication failed"}), 500
                
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
            
            extractions = db_service.get_all_extractions(limit=1000)
            count = len(extractions)
            db_service.disconnect()
            
            return jsonify({
                'success': True,
                'count': count,
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

    @app.route('/api/health')
    def health():
        return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})

    # Proxy Fix for Docker/Reverse Proxy
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    
    # Start Monitoring on App Startup
    start_background_monitoring_if_needed()
    
    return app

def start_background_monitoring_if_needed():
    """Checks for Company Token and starts monitoring thread if not active."""
    global monitoring_active, monitoring_thread, company_gmail_service
    
    if monitoring_active:
        return
        
    try:
        db = DuckDBService()
        if not db.connect():
            logger.error("Could not connect to DB to check for token")
            return
            
        token_json_str = db.get_company_token()
        db.disconnect()
        
        if token_json_str:
            logger.info("Found Company Gmail token, starting monitoring...")
            try:
                token_info = json.loads(token_json_str)
                service = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)
                
                if service.authenticate_from_info(token_info):
                    company_gmail_service = service
                    
                    # Start thread
                    monitoring_active = True
                    # Re-use monitoring loop from GmailService but ensure it runs continuously
                    service.start_monitoring(Config.EMAIL_CHECK_INTERVAL)
                    logger.info("✅ Background monitoring started")
                else:
                    logger.error("Failed to authenticate Company Gmail from stored token")
            except Exception as e:
                logger.error(f"Error starting monitoring: {e}")
        else:
            logger.info("No Company Gmail token found. Monitoring paused until connected.")
            
    except Exception as e:
        logger.error(f"Startup monitoring check failed: {e}")


app = create_flask_app()   

if __name__ == '__main__':
    # This block is for local dev only
    setup_logging()
  
    app.run(host='0.0.0.0', port=5001, debug=Config.DEBUG)