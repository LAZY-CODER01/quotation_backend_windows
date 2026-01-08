"""
QuoteSnap - Gmail Email Monitor
Simple Gmail monitoring application that prints new emails.
"""

import logging
import os
import threading
import uuid
import json
from datetime import datetime
from flask import Flask, jsonify, send_file, redirect, session, request
from flask_cors import CORS
from dotenv import load_dotenv
from app.services.gmail_service import GmailService
from app.services.duckdb_service import DuckDBService
from app.services.new_excel_generation import ExcelGenerationService
from werkzeug.middleware.proxy_fix import ProxyFix
from config.settings import Config

# Load environment variables
load_dotenv()

# Global Gmail service instances (one per user session)
gmail_services = {}  # session_id -> GmailService instance
monitoring_active = {}  # session_id -> bool

logger = logging.getLogger(__name__)

def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def create_flask_app():
    """Create and configure Flask application."""
    app = Flask(__name__)


    # 🔐 Use a strong secret key
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change")

    # ✅ CORS CONFIGURATION (important for session cookies)
    CORS(app,
         supports_credentials=True,
         origins=Config.CORS_ORIGINS,
         allow_headers=["Content-Type", "Authorization"],
         methods=["GET", "POST", "OPTIONS"])

    # 🔑 Ensure session cookies are properly configured
    # Respect environment variables for local development
    session_secure = os.getenv("SESSION_COOKIE_SECURE", "False").lower() == "true"
    session_samesite = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")  # Lax for same-site, None for cross-site
    cookie_domain = os.getenv("COOKIE_DOMAIN", None)
    
    # For localhost, use Lax and non-secure cookies
    # For production/cross-site, use None and secure cookies
    if session_samesite == "None":
        session_secure = True  # None requires Secure=True
    
    app.config.update(
        SESSION_COOKIE_SAMESITE=session_samesite,
        SESSION_COOKIE_SECURE=session_secure,
        SESSION_PERMANENT=False
    )
    
    # Set cookie domain if specified (remove trailing dot if present)
    if cookie_domain:
        cookie_domain = cookie_domain.rstrip('.')
        app.config['SESSION_COOKIE_DOMAIN'] = cookie_domain
    
    logger.info(f"Session cookie config: Secure={session_secure}, SameSite={session_samesite}, Domain={cookie_domain}")

    def get_user_token_path(session_id):
        token_dir = Config.GMAIL_TOKEN_DIRECTORY
        os.makedirs(token_dir, exist_ok=True)
        return os.path.join(token_dir, f"token_{session_id}.json")

    def get_or_create_session():
        if "user_id" not in session:
            session["user_id"] = str(uuid.uuid4())
        return session["user_id"]

    
    @app.route('/api/auth/status', methods=['GET'])
    def auth_status():
        """
        Check if user is authenticated with Gmail.
        
        Returns:
            JSON response with authentication status
        """
        try:
            # Debug: Log session and cookie information
            logger.debug(f"Auth status check - Session keys: {list(session.keys())}")
            logger.debug(f"Auth status check - Cookies received: {list(request.cookies.keys())}")
            logger.debug(f"Auth status check - Session cookie value: {request.cookies.get('session', 'NOT SET')}")
            
            # Try to get or create session
            user_id = session.get('user_id')
            if not user_id:
                # Create a new session if one doesn't exist
                user_id = get_or_create_session()
                logger.info(f"Created new session in auth_status: user_id={user_id}")
                # Return early since we just created the session (no token yet)
                return jsonify({
                    'authenticated': False,
                    'message': 'No session found - session created',
                    'session_created': True,
                    'user_id': user_id
                })
            
            # Check for token in database instead of file system
            db = DuckDBService()
            if not db.connect():
                logger.error("Failed to connect to database")
                return jsonify({
                    'authenticated': False,
                    'message': 'Database connection failed'
                }), 500
            
            # Ensure table exists
            db.create_table()
            
            # Get token from database
            token_json_str = db.get_user_token(user_id)
            
            if token_json_str:
                # Token exists in database, check if valid
                from google.oauth2.credentials import Credentials
                try:
                    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                              'https://www.googleapis.com/auth/gmail.modify']
                    creds = Credentials.from_authorized_user_info(json.loads(token_json_str), SCOPES)
                    
                    if creds and creds.valid:
                        db.disconnect()
                        return jsonify({
                            'authenticated': True,
                            'message': 'User is authenticated'
                        })
                    elif creds and creds.expired and creds.refresh_token:
                        from google.auth.transport.requests import Request
                        try:
                            creds.refresh(Request())
                            # Save refreshed credentials to database
                            refreshed_token_json = creds.to_json()
                            if db.save_user_token(user_id, refreshed_token_json):
                                db.disconnect()
                                return jsonify({
                                    'authenticated': True,
                                    'message': 'Token refreshed successfully'
                                })
                            else:
                                db.disconnect()
                                logger.error("Failed to save refreshed token to database")
                                return jsonify({
                                    'authenticated': False,
                                    'message': 'Token refreshed but failed to save'
                                })
                        except Exception as e:
                            db.disconnect()
                            logger.error(f"Failed to refresh token: {str(e)}")
                            return jsonify({
                                'authenticated': False,
                                'message': 'Token expired and refresh failed'
                            })
                except Exception as e:
                    db.disconnect()
                    logger.error(f"Error checking credentials: {str(e)}")
                    return jsonify({
                        'authenticated': False,
                        'message': 'Invalid credentials'
                    })
            
            db.disconnect()
            return jsonify({
                'authenticated': False,
                'message': 'No authentication token found'
            })
            
        except Exception as e:
            logging.error(f"Auth status error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({'authenticated': False, 'error': str(e)}), 500
    
    @app.route('/api/auth/debug', methods=['GET'])
    def auth_debug():
        """
        Debug endpoint to check session and cookie state.
        
        Returns:
            JSON response with debugging information
        """
        try:
            debug_info = {
                'session_keys': list(session.keys()),
                'session_user_id': session.get('user_id'),
                'session_oauth_state': session.get('oauth_state'),
                'cookies_received': list(request.cookies.keys()),
                'session_cookie_present': 'session' in request.cookies,
                'session_cookie_value': request.cookies.get('session', 'NOT SET')[:50] if 'session' in request.cookies else 'NOT SET',
                'request_origin': request.headers.get('Origin'),
                'request_referer': request.headers.get('Referer'),
                'cors_origins': Config.CORS_ORIGINS,
                'session_cookie_secure': app.config.get('SESSION_COOKIE_SECURE'),
                'session_cookie_samesite': app.config.get('SESSION_COOKIE_SAMESITE'),
                'session_cookie_domain': app.config.get('SESSION_COOKIE_DOMAIN'),
            }
            
            # Check if token exists for current user in database
            user_id = session.get('user_id')
            if user_id:
                db = DuckDBService()
                if db.connect():
                    db.create_table()
                    token_json_str = db.get_user_token(user_id)
                    db.disconnect()
                    debug_info['token_exists_in_db'] = token_json_str is not None
                else:
                    debug_info['token_exists_in_db'] = False
                    debug_info['db_connection_error'] = True
            
            return jsonify(debug_info)
        except Exception as e:
            import traceback
            return jsonify({
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500
    
    @app.route("/api/auth/login", methods=["GET"])
    def auth_login():
        try:
            user_id = get_or_create_session()
            state = str(uuid.uuid4())
            session["oauth_state"] = state  # store CSRF token
            session["user_id"] = user_id  # Ensure user_id is in session
            
            # Force session to be saved
            session.permanent = False
            
            logger.info(f"Login: Created session user_id={user_id}, state={state}")
            logger.debug(f"Session data: {dict(session)}")

            gmail_service = GmailService(
                credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                token_path=get_user_token_path(user_id)
            )

            auth_url = gmail_service.get_authorization_url(
                redirect_uri=Config.OAUTH_REDIRECT_URI,
                state=state
            )

            if not auth_url:
                return jsonify({"success": False, "message": "Failed to generate auth URL"}), 500

            gmail_services[user_id] = gmail_service

            response = jsonify({
                "success": True,
                "authorization_url": auth_url
            })
            
            # Ensure session cookie is set in response
            return response

        except Exception as e:
            logger.error(f"Auth login error: {e}")
            return jsonify({"success": False, "error": str(e)}), 500


    @app.route("/api/auth/callback", methods=["GET"])
    def auth_callback():
        try:
            code = request.args.get("code")
            state = request.args.get("state")
            error = request.args.get("error")

            logger.info(f"Callback received: code={'present' if code else 'missing'}, state={state}, error={error}")
            logger.debug(f"Session data: {dict(session)}")
            logger.debug(f"Session cookie present: {'session' in request.cookies}")

            if error:
                logger.error(f"OAuth error: {error}")
                return redirect(f"{Config.FRONTEND_URL}/?error=oauth_failed")

            if not code:
                return jsonify({"success": False, "error": "Missing code"}), 400

            stored_state = session.get("oauth_state")
            stored_user_id = session.get("user_id")
            
            logger.info(f"State check: stored_state={stored_state}, received_state={state}, stored_user_id={stored_user_id}")
            
            # Check OAuth state for CSRF protection
            # In development, we can be more lenient if session wasn't preserved
            development_mode = os.getenv("FLASK_DEBUG", "False").lower() == "true"
            
            if not stored_state or stored_state != state:
                logger.error(f"❌ Invalid OAuth state: stored={stored_state}, got={state}")
                logger.error(f"Session keys: {list(session.keys())}")
                logger.error(f"All cookies: {list(request.cookies.keys())}")
                
                # In development, if session was lost, try to continue with warning
                if development_mode and not stored_state:
                    logger.warning("⚠️ Development mode: Continuing without state validation (session lost)")
                else:
                    return redirect(f"{Config.FRONTEND_URL}/?error=invalid_state")

            user_id = stored_user_id or session.get("user_id")
            if not user_id:
                logger.error("❌ No user session found during callback")
                # Try to create a new session as fallback
                user_id = get_or_create_session()
                logger.warning(f"Created new session user_id={user_id} during callback (session was lost)")

            gmail_service = gmail_services.get(user_id) or GmailService(
                credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                token_path=get_user_token_path(user_id)
            )

            # Pass user_id to authenticate_from_code so it can save token to DB
            if gmail_service.authenticate_from_code(code, Config.OAUTH_REDIRECT_URI, user_id=user_id):
                gmail_services[user_id] = gmail_service

                if not monitoring_active.get(user_id):
                    thread = threading.Thread(
                        target=start_monitoring_loop, args=(gmail_service,), daemon=True
                    )
                    thread.start()
                    monitoring_active[user_id] = True

                session.pop("oauth_state", None)
                return redirect(f"{Config.FRONTEND_URL}/dashboard")

            else:
                return redirect(f"{Config.FRONTEND_URL}/?error=auth_failed")

        except Exception as e:
            logger.error(f"Auth callback error: {e}")
            return redirect(f"{Config.FRONTEND_URL}/?error=server_error")
    
    @app.route('/api/auth/logout', methods=['POST'])
    def auth_logout():
        """
        Logout user by removing token from database.
        
        Returns:
            JSON response confirming logout
        """
        try:
            user_id = session.get('user_id')
            if user_id:
                # Delete token from database instead of file system
                db = DuckDBService()
                if db.connect():
                    db.create_table()  # Ensure table exists
                    db.delete_user_token(user_id)
                    db.disconnect()
                    logger.info(f"Deleted token from database for user {user_id}")
                
                # Stop monitoring
                if user_id in gmail_services:
                    gmail_service = gmail_services[user_id]
                    gmail_service.stop_monitoring()
                    del gmail_services[user_id]
                
                if user_id in monitoring_active:
                    del monitoring_active[user_id]
                
                # Clear session
                session.clear()
            
            return jsonify({
                'success': True,
                'message': 'Logged out successfully'
            })
        except Exception as e:
            logging.error(f"Logout error: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/emails', methods=['GET'])
    def get_all_emails():
        """
        API endpoint to fetch all stored email extractions.
        
        Returns:
            JSON response with all email extraction records
        """
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            
            # Debug: Check if table exists and has data
            try:
                count_result = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()
                table_count = count_result[0] if count_result else 0
                logging.info(f"Database has {table_count} records")
            except Exception as e:
                logging.error(f"Error checking table: {str(e)}")
                return jsonify({'error': f'Database table error: {str(e)}'}), 500
            
            # Get all extractions (limit to 1000 for performance)
            extractions = db_service.get_all_extractions(limit=1000)
            db_service.disconnect()
            
            return jsonify({
                'success': True,
                'count': len(extractions),
                'table_count': table_count,
                'data': extractions
            })
            
        except Exception as e:
            logging.error(f"API error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/debug/database', methods=['GET'])
    def debug_database():
        """
        Debug endpoint to check database status and content.
        
        Returns:
            JSON response with database debug information
        """
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            
            # Check if table exists
            tables = db_service.connection.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            # Check table schema if exists
            schema = None
            count = 0
            sample_data = []
            
            if 'email_extractions' in table_names:
                # Get table schema
                schema_result = db_service.connection.execute("DESCRIBE email_extractions").fetchall()
                schema = [{'column': row[0], 'type': row[1]} for row in schema_result]
                
                # Get count
                count_result = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()
                count = count_result[0] if count_result else 0
                
                # Get sample data
                if count > 0:
                    sample_result = db_service.connection.execute('SELECT * FROM email_extractions LIMIT 3').fetchall()
                    sample_data = [list(row) for row in sample_result]
            
            db_service.disconnect()
            
            return jsonify({
                'success': True,
                'database_path': db_service.db_path,
                'tables': table_names,
                'email_extractions_exists': 'email_extractions' in table_names,
                'schema': schema,
                'record_count': count,
                'sample_data': sample_data
            })
            
        except Exception as e:
            logging.error(f"Database debug error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/emails/stats', methods=['GET'])
    def get_email_stats():
        """
        API endpoint to get email processing statistics.
        
        Returns:
            JSON response with statistics
        """
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            
            # Get statistics
            total_result = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()
            valid_result = db_service.connection.execute("SELECT COUNT(*) FROM email_extractions WHERE extraction_status = 'VALID'").fetchone()
            irrelevant_result = db_service.connection.execute("SELECT COUNT(*) FROM email_extractions WHERE extraction_status = 'IRRELEVANT'").fetchone()
            
            db_service.disconnect()
            
            return jsonify({
                'success': True,
                'stats': {
                    'total_emails': total_result[0] if total_result else 0,
                    'valid_quotations': valid_result[0] if valid_result else 0,
                    'irrelevant_emails': irrelevant_result[0] if irrelevant_result else 0
                }
            })
            
        except Exception as e:
            logging.error(f"API error: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/requirement/delete', methods=['POST'])
    def delete_requirement():
        """
        Delete a specific requirement from an extraction's requirements list.

        Expects JSON body: { "gmail_id": "<id>", "index": <int> }
        """
        try:
            data = request.get_json(force=True)
            gmail_id = data.get('gmail_id')
            index = data.get('index')

            if not gmail_id:
                return jsonify({'success': False, 'error': 'Missing gmail_id'}), 400
            if index is None:
                return jsonify({'success': False, 'error': 'Missing index'}), 400

            try:
                index = int(index)
            except Exception:
                return jsonify({'success': False, 'error': 'Index must be an integer'}), 400

            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'success': False, 'error': 'Failed to connect to database'}), 500

            extraction = db_service.get_extraction(gmail_id)
            if not extraction:
                db_service.disconnect()
                return jsonify({'success': False, 'error': 'Extraction not found for given gmail_id'}), 404

            extraction_result = extraction.get('extraction_result') or {}

            # Support either 'Requirements' or 'requirements' (case-insensitive)
            req_key = None
            for k in extraction_result.keys():
                if k.lower() == 'requirements':
                    req_key = k
                    break

            if req_key is None:
                db_service.disconnect()
                return jsonify({'success': False, 'error': 'No requirements list found for this extraction'}), 400

            requirements = extraction_result.get(req_key)
            if not isinstance(requirements, list):
                db_service.disconnect()
                return jsonify({'success': False, 'error': 'Requirements field is not a list'}), 400

            if index < 0 or index >= len(requirements):
                db_service.disconnect()
                return jsonify({'success': False, 'error': 'Index out of range'}), 400

            # Remove the item and preserve the original key name
            removed = requirements.pop(index)
            extraction_result[req_key] = requirements

            updated = db_service.update_extraction(gmail_id, extraction_result)
            db_service.disconnect()

            if not updated:
                return jsonify({'success': False, 'error': 'Failed to update extraction in database'}), 500

            return jsonify({'success': True, 'removed': removed, 'requirements': requirements})

        except Exception as e:
            logging.error(f"Requirement delete error: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/api/database/clear', methods=['POST'])
    def clear_database():
        """
        Delete all quotations from the database (clear all email_extractions records).
        
        Returns:
            JSON response with number of records deleted
        """
        try:
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'success': False, 'error': 'Failed to connect to database'}), 500
            
            # Get count before deletion
            count_result = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()
            records_before = count_result[0] if count_result else 0
            
            # Delete all records
            db_service.connection.execute('DELETE FROM email_extractions')
            
            # Verify deletion
            count_result = db_service.connection.execute('SELECT COUNT(*) FROM email_extractions').fetchone()
            records_after = count_result[0] if count_result else 0
            
            db_service.disconnect()
            
            deleted_count = records_before - records_after
            
            logging.info(f"Database cleared: {deleted_count} records deleted")
            
            return jsonify({
                'success': True,
                'message': f'Database cleared successfully',
                'records_deleted': deleted_count,
                'records_before': records_before,
                'records_after': records_after
            })
            
        except Exception as e:
            logging.error(f"Database clear error: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check endpoint."""
        return jsonify({
            'success': True,
            'message': 'SnapQuote API is running',
            'timestamp': datetime.now().isoformat()
        })
    
    @app.route('/api/quotation/generate/<gmail_id>', methods=['GET'])
    def generate_quotation(gmail_id: str):
        """
        Generate and download Excel quotation file from stored email extraction data using the new ExcelGenerationService (win32com version).
        Args:
            gmail_id (str): Gmail message ID
        Returns:
            Excel file for immediate download
        """
        try:
            # Get email data from database
            db_service = DuckDBService()
            if not db_service.connect():
                return jsonify({'error': 'Failed to connect to database'}), 500
            extraction_data = db_service.get_extraction(gmail_id)
            db_service.disconnect()
            if not extraction_data:
                return jsonify({'error': f'Email with gmail_id {gmail_id} not found'}), 404
            # Check if it's a valid quotation (not irrelevant)
            if extraction_data.get('extraction_status') != 'VALID':
                return jsonify({
                    'error': 'Cannot generate quotation for irrelevant email',
                    'status': extraction_data.get('extraction_status')
                }), 400
            
            # Debug: Log extraction data
            extraction_result = extraction_data.get('extraction_result', {})
            logger.info(f"Generating Excel for gmail_id: {gmail_id}")
            logger.debug(f"Extraction result keys: {list(extraction_result.keys()) if extraction_result else 'None'}")
            if extraction_result:
                requirements = extraction_result.get('Requirements', [])
                logger.info(f"Found {len(requirements)} requirements in extraction_result")
                if requirements:
                    logger.debug(f"First requirement: {requirements[0]}")
            
            # Generate Excel file on disk using new service
            excel_service = ExcelGenerationService()
            output_file = excel_service.generate_quotation_excel(gmail_id, extraction_data)
            if not output_file or not os.path.exists(output_file):
                return jsonify({'error': 'Failed to generate Excel file'}), 500

            # Ensure the file is closed and saved before sending (important for win32com)
            # (Already handled in service, but double-check)
            import time
            time.sleep(0.2)  # Small delay to ensure file system flush (esp. on Windows)

            # Create a descriptive filename for download
            subject = extraction_data.get('subject', 'quotation')[:30]  # Limit length
            clean_subject = "".join(c for c in subject if c.isalnum() or c in (' ', '-', '_')).rstrip()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            download_filename = f"Quotation_{clean_subject}_{timestamp}.xlsx"

            # Open the file in binary mode and send as attachment (ensures no in-memory re-save)
            return send_file(
                os.path.abspath(output_file),
                as_attachment=True,
                download_name=download_filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                max_age=0  # Prevent caching
            )
        except Exception as e:
            logging.error(f"Quotation generation error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/quotation/download/<filename>', methods=['GET'])
    def download_quotation(filename: str):
        """
        Download generated quotation Excel file.
        
        Args:
            filename (str): Filename to download
            
        Returns:
            File download
        """
        try:
            file_path = os.path.join('generated', filename)
            
            if not os.path.exists(file_path):
                return jsonify({'error': 'File not found'}), 404
            
            # Validate filename to prevent directory traversal
            if not filename.endswith('.xlsx') or '..' in filename:
                return jsonify({'error': 'Invalid filename'}), 400
            
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
        except Exception as e:
            logging.error(f"File download error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    # The new ExcelGenerationService does not provide a template analysis method, so this endpoint is now deprecated or should be updated if needed.
    @app.route('/api/template/analyze', methods=['GET'])
    def analyze_template():
        """
        (Deprecated) Analyze the Excel template structure. Not supported in new ExcelGenerationService.
        """
        return jsonify({'success': False, 'error': 'Template analysis not supported in new ExcelGenerationService.'}), 501
    app.wsgi_app = ProxyFix(
        app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1
    )

    return app

def start_monitoring_loop(gmail_service):
    """
    Background thread function to monitor emails continuously.
    
    Args:
        gmail_service: Authenticated GmailService instance
    """
    try:
        logging.info(f"📧 Starting email monitoring (checking every {Config.EMAIL_CHECK_INTERVAL} seconds)")
        logging.info("📱 Monitoring active - new emails will be processed automatically")
        logging.info("=" * 60)
        
        gmail_service.start_monitoring(Config.EMAIL_CHECK_INTERVAL)
        
    except Exception as e:
        logging.error(f"Email monitoring error: {str(e)}")

def initialize_database():
    """Initialize DuckDB database on startup."""
    try:
        print("🗄️ Initializing DuckDB database...")
        db_service = DuckDBService()
        if db_service.connect():
            if db_service.create_table():
                print("✅ DuckDB database ready")
            else:
                print("⚠️ Warning: Could not create database table")
            db_service.disconnect()
        else:
            print("⚠️ Warning: Could not connect to database")
    except Exception as e:
        print(f"❌ Database initialization error: {str(e)}")
        logging.error(f"Database initialization error: {str(e)}")

def check_and_start_monitoring_for_existing_users():
    """Check for existing user tokens and start monitoring for each."""
    try:
        token_dir = Config.GMAIL_TOKEN_DIRECTORY
        
        if not os.path.exists(token_dir):
            print("� No token directory found. Users need to login via frontend.")
            return
        
        # Find all token files
        token_files = [f for f in os.listdir(token_dir) if f.startswith('token_') and f.endswith('.json')]
        
        if not token_files:
            print("💡 No authentication tokens found. Users need to login via frontend.")
            return
        
        print(f"🔍 Found {len(token_files)} existing user token(s)...")
        
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                  'https://www.googleapis.com/auth/gmail.modify']
        
        for token_file in token_files:
            try:
                # Extract user_id from filename (token_USER_ID.json)
                user_id = token_file.replace('token_', '').replace('.json', '')
                token_path = os.path.join(token_dir, token_file)
                
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)
                
                # Refresh if expired
                if creds and creds.expired and creds.refresh_token:
                    print(f"🔄 Refreshing token for user {user_id[:8]}...")
                    try:
                        creds.refresh(Request())
                        with open(token_path, 'w') as token:
                            token.write(creds.to_json())
                        print(f"✅ Token refreshed for user {user_id[:8]}")
                    except Exception as e:
                        print(f"⚠️ Failed to refresh token for user {user_id[:8]}: {str(e)}")
                        continue
                
                if creds and creds.valid:
                    # Create Gmail service for this user
                    gmail_service = GmailService(
                        credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                        token_path=token_path
                    )
                    gmail_service.credentials = creds
                    gmail_service.service = gmail_service._build_service()
                    
                    # Store service instance
                    gmail_services[user_id] = gmail_service
                    
                    # Start monitoring in background
                    monitoring_thread = threading.Thread(
                        target=start_monitoring_loop,
                        args=(gmail_service,),
                        daemon=True
                    )
                    monitoring_thread.start()
                    monitoring_active[user_id] = True
                    
                    print(f"✅ Email monitoring active for user {user_id[:8]}")
                else:
                    print(f"⚠️ Invalid token for user {user_id[:8]}")
                    
            except Exception as e:
                print(f"⚠️ Error processing token {token_file}: {str(e)}")
                continue
        
        active_count = len([v for v in monitoring_active.values() if v])
        if active_count > 0:
            print(f"� Email monitoring active for {active_count} user(s)")
        
    except Exception as e:
        logging.error(f"Error checking authentication: {str(e)}")
        print(f"⚠️ Error checking for existing tokens: {str(e)}")


# 🔥 Gunicorn entry point (MUST be global)
app = create_flask_app()

# Flag to prevent double initialization
_app_initialized = False

# Initialize database and start monitoring when app is created (for gunicorn)
# This runs when the module is imported, not just when __main__ is executed
def _initialize_app():
    """Initialize database and start monitoring for existing users."""
    global _app_initialized
    
    if _app_initialized:
        return  # Already initialized
    
    try:
        print("🚀 Initializing SnapQuote application...")
        _app_initialized = True
        
        # Configure logging
        setup_logging()
        
        # Validate configuration
        try:
            Config.validate_config()
            print("✅ Configuration validated")
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            return
        
        # Initialize database
        initialize_database()
        
        # Check if already authenticated and start monitoring for existing users
        check_and_start_monitoring_for_existing_users()
        
        print("✅ Application initialization complete")
        
    except Exception as e:
        print(f"❌ Initialization error: {str(e)}")
        logging.error(f"Application initialization error: {str(e)}")
        _app_initialized = False

# Run initialization (will run when module is imported by gunicorn)
_initialize_app()

def main():
    """Main application entry point (for direct execution, not gunicorn)."""
    global _app_initialized
    
    print("🚀 Starting SnapQuote Gmail Monitor with API...")
    
    # If already initialized (e.g., by gunicorn import), skip initialization
    if not _app_initialized:
        # Configure logging
        setup_logging()
        
        # Validate configuration
        try:
            Config.validate_config()
            print("✅ Configuration validated")
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            return
        
        try:
            # Initialize database
            initialize_database()
            
            # Check if already authenticated and start monitoring for existing users
            check_and_start_monitoring_for_existing_users()
            _app_initialized = True
        except Exception as e:
            print(f"❌ Initialization error: {str(e)}")
            logging.error(f"Application initialization error: {str(e)}")
            return
    
    try:
        # Use the global app (already created and initialized)
        flask_app = app
        
        print("🌐 Starting Flask API server...")
        print("📡 API Endpoints available:")
        print("   GET /api/auth/status - Check authentication status")
        print("   GET /api/auth/login - Get OAuth authorization URL")
        print("   GET /api/auth/callback - OAuth callback handler")
        print("   POST /api/auth/logout - Logout")
        print("   GET /api/emails - Get all stored emails")
        print("   GET /api/emails/stats - Get email statistics")
        print("   GET /api/quotation/generate/<gmail_id> - Download quotation")
        print("   GET /api/health - Health check")
        print("🚀 Server running at http://localhost:8000")
        print("=" * 60)
        active_users = len([v for v in monitoring_active.values() if v])
        if active_users == 0:
            print("💡 Tip: Email monitoring will start automatically when you login via frontend")
            print("=" * 60)
        
        # Run Flask app
        flask_app.run(host='0.0.0.0', port=8000, debug=True, use_reloader=False)
        
    except KeyboardInterrupt:
        print("\n👋 Shutting down SnapQuote...")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        logging.error(f"Application error: {str(e)}")



if __name__ == '__main__':
    main()