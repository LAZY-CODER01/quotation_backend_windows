"""
QuoteSnap - Gmail Email Monitor
Production-ready Flask backend served via Waitress.
"""

import logging
import os
import threading
import uuid
from datetime import datetime
from flask import Flask, jsonify, send_file, redirect, session, request
from flask_cors import CORS
from dotenv import load_dotenv
from app.services.gmail_service import GmailService
from app.services.duckdb_service import DuckDBService
from app.services.new_excel_generation import ExcelGenerationService
from config.settings import Config

# Load environment variables
load_dotenv()

# Global Gmail service instances
gmail_services = {}       # session_id -> GmailService instance
monitoring_active = {}    # session_id -> bool

logger = logging.getLogger(__name__)

def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def start_monitoring_loop(gmail_service):
    """Background thread to monitor Gmail inbox."""
    try:
        logging.info(f"📧 Starting email monitoring (checking every {Config.EMAIL_CHECK_INTERVAL}s)")
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
            print("💡 No token directory found. Users need to login via frontend.")
            return

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
                user_id = token_file.replace('token_', '').replace('.json', '')
                token_path = os.path.join(token_dir, token_file)
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)

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
                    gmail_service = GmailService(
                        credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                        token_path=token_path
                    )
                    gmail_service.credentials = creds
                    gmail_service.service = gmail_service._build_service()

                    gmail_services[user_id] = gmail_service
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
            print(f"📬 Email monitoring active for {active_count} user(s)")

    except Exception as e:
        logging.error(f"Error checking authentication: {str(e)}")
        print(f"⚠️ Error checking for existing tokens: {str(e)}")

def create_flask_app():
    """Create and configure Flask app."""
    app = Flask(__name__)
    app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')
    CORS(app, supports_credentials=True, origins=Config.CORS_ORIGINS)

    def get_user_token_path(session_id: str) -> str:
        """Get token file path for a specific user session."""
        token_dir = Config.GMAIL_TOKEN_DIRECTORY
        os.makedirs(token_dir, exist_ok=True)
        return os.path.join(token_dir, f'token_{session_id}.json')
    
    def get_or_create_session():
        """Get existing session ID or create a new one."""
        if 'user_id' not in session:
            session['user_id'] = str(uuid.uuid4())
        return session['user_id']
    
    @app.route('/api/auth/status', methods=['GET'])
    def auth_status():
        """
        Check if user is authenticated with Gmail.
        
        Returns:
            JSON response with authentication status
        """
        try:
            user_id = session.get('user_id')
            if not user_id:
                return jsonify({
                    'authenticated': False,
                    'message': 'No session found'
                })
            
            token_path = get_user_token_path(user_id)
            
            if os.path.exists(token_path):
                # Token file exists, check if valid
                from google.oauth2.credentials import Credentials
                try:
                    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 
                              'https://www.googleapis.com/auth/gmail.modify']
                    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
                    
                    if creds and creds.valid:
                        return jsonify({
                            'authenticated': True,
                            'message': 'User is authenticated'
                        })
                    elif creds and creds.expired and creds.refresh_token:
                        from google.auth.transport.requests import Request
                        try:
                            creds.refresh(Request())
                            # Save refreshed credentials
                            with open(token_path, 'w') as token:
                                token.write(creds.to_json())
                            return jsonify({
                                'authenticated': True,
                                'message': 'Token refreshed successfully'
                            })
                        except Exception as e:
                            logger.error(f"Failed to refresh token: {str(e)}")
                            return jsonify({
                                'authenticated': False,
                                'message': 'Token expired and refresh failed'
                            })
                except Exception as e:
                    logger.error(f"Error checking credentials: {str(e)}")
                    return jsonify({
                        'authenticated': False,
                        'message': 'Invalid credentials'
                    })
            
            return jsonify({
                'authenticated': False,
                'message': 'No authentication token found'
            })
            
        except Exception as e:
            logging.error(f"Auth status error: {str(e)}")
            return jsonify({'authenticated': False, 'error': str(e)}), 500
    
    @app.route('/api/auth/login', methods=['GET'])
    def auth_login():
        """
        Initiate Gmail OAuth flow by returning authorization URL.
        
        Returns:
            JSON with authorization URL for frontend to redirect to
        """
        try:
            # Get or create user session
            user_id = get_or_create_session()
            
            # Store state for CSRF protection
            state = str(uuid.uuid4())
            session['oauth_state'] = state
            
            # Create Gmail service
            gmail_service = GmailService(
                credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                token_path=get_user_token_path(user_id)
            )
            
            # Generate authorization URL
            auth_url = gmail_service.get_authorization_url(
                redirect_uri=Config.OAUTH_REDIRECT_URI,
                state=state
            )
            
            if not auth_url:
                return jsonify({
                    'success': False,
                    'message': 'Failed to generate authorization URL'
                }), 500
            
            # Store gmail service temporarily
            gmail_services[user_id] = gmail_service
            
            return jsonify({
                'success': True,
                'authorization_url': auth_url,
                'message': 'Redirect to authorization URL'
            })
            
        except Exception as e:
            logging.error(f"Auth login error: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/auth/callback', methods=['GET'])
    def auth_callback():
        """
        Handle OAuth callback from Google.
        Exchange authorization code for tokens and redirect to dashboard.
        
        Returns:
            Redirect to frontend dashboard
        """
        try:
            # Get authorization code and state from query parameters
            code = request.args.get('code')
            state = request.args.get('state')
            error = request.args.get('error')
            
            if error:
                logger.error(f"OAuth error: {error}")
                return redirect(f"{Config.FRONTEND_URL}/?error=oauth_failed")
            
            if not code:
                return jsonify({'success': False, 'error': 'No authorization code provided'}), 400
            
            # Verify state to prevent CSRF
            stored_state = session.get('oauth_state')
            if not stored_state or stored_state != state:
                logger.error("State mismatch - possible CSRF attack")
                return redirect(f"{Config.FRONTEND_URL}/?error=invalid_state")
            
            user_id = session.get('user_id')
            if not user_id:
                return redirect(f"{Config.FRONTEND_URL}/?error=no_session")
            
            # Get or create Gmail service
            gmail_service = gmail_services.get(user_id)
            if not gmail_service:
                gmail_service = GmailService(
                    credentials_path=Config.GMAIL_CREDENTIALS_FILE,
                    token_path=get_user_token_path(user_id)
                )
            
            # Exchange code for credentials
            if gmail_service.authenticate_from_code(code, Config.OAUTH_REDIRECT_URI):
                # Store the service instance
                gmail_services[user_id] = gmail_service
                
                # Start monitoring if not already active
                if user_id not in monitoring_active or not monitoring_active[user_id]:
                    monitoring_thread = threading.Thread(
                        target=start_monitoring_loop,
                        args=(gmail_service,),
                        daemon=True
                    )
                    monitoring_thread.start()
                    monitoring_active[user_id] = True
                    logger.info(f"📧 Email monitoring started for user {user_id}")
                
                # Clear OAuth state
                session.pop('oauth_state', None)
                
                # Redirect to dashboard
                return redirect(f"{Config.FRONTEND_URL}/dashboard")
            else:
                return redirect(f"{Config.FRONTEND_URL}/?error=auth_failed")
                
        except Exception as e:
            logging.error(f"Auth callback error: {str(e)}")
            return redirect(f"{Config.FRONTEND_URL}/?error=server_error")
    
    @app.route('/api/auth/logout', methods=['POST'])
    def auth_logout():
        """
        Logout user by removing token file.
        
        Returns:
            JSON response confirming logout
        """
        try:
            user_id = session.get('user_id')
            if user_id:
                token_path = get_user_token_path(user_id)
                if os.path.exists(token_path):
                    os.remove(token_path)
                
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
    
    return app

# --------------------- APP INITIALIZATION ---------------------
print("🚀 Starting SnapQuote Gmail Monitor with API (Waitress mode)...")
setup_logging()

try:
    Config.validate_config()
    print("✅ Configuration validated")
except ValueError as e:
    print(f"❌ Configuration error: {e}")
    raise SystemExit(1)

initialize_database()
check_and_start_monitoring_for_existing_users()

# Create Flask app (to be imported by wsgi.py)
app = create_flask_app()
print("🌐 Flask app created successfully — ready for Waitress.")

# Only run Flask dev server if executed directly (not via waitress-serve)
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)
