import duckdb
import os
import logging
import json
from datetime import datetime
from config.settings import Config

logger = logging.getLogger(__name__)

class DuckDBService:
    def __init__(self):
        """
        Initialize DuckDB connection.
        Connects to MotherDuck if token is present, otherwise falls back to local file.
        """
        self.token = os.getenv('MOTHERDUCK_TOKEN')
        
        if self.token:
            # ✅ Connect to MotherDuck Cloud
            self.db_path = f'md:snapquote_db?motherduck_token={self.token}'
            self.is_cloud = True
            logger.info("🔌 Configured for MotherDuck Cloud Database")
        else:
            # ⚠️ Fallback for local development
            self.db_path = 'local_dev.duckdb'
            self.is_cloud = False
            logger.warning("⚠️ MOTHERDUCK_TOKEN not found. Using local file 'local_dev.duckdb'")
            
        self.connection = None

    def connect(self):
        """Establish connection to the database."""
        try:
            self.connection = duckdb.connect(self.db_path)
            return True
        except Exception as e:
            logger.error(f"❌ Database connection error: {str(e)}")
            return False

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.connection = None

    def create_table(self):
        """Create necessary tables (Emails + Auth Tokens + Users)."""
        try:
            # 1. Create Email Extractions Table
            self.connection.execute("""
                CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;
                CREATE TABLE IF NOT EXISTS email_extractions (
                    id INTEGER DEFAULT nextval('id_sequence'),
                    gmail_id VARCHAR PRIMARY KEY,
                    sender VARCHAR,
                    received_at TIMESTAMP,
                    subject VARCHAR,
                    body_text TEXT,
                    extraction_result JSON,
                    extraction_status VARCHAR,
                    updated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 2. Auto-repair: Add updated_at if missing
            try:
                self.connection.execute("SELECT updated_at FROM email_extractions LIMIT 1")
            except:
                logger.info("🛠️ Column 'updated_at' missing. Adding it now...")
                self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")

            # 3. Create User Tokens Table (Google OAuth)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS user_tokens (
                    user_id VARCHAR PRIMARY KEY,
                    token_json JSON,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 4. ✅ Create Users Table (App Authentication)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    username VARCHAR UNIQUE,
                    password_hash VARCHAR,
                    role VARCHAR DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # 5. Company Gmail OAuth Token (SINGLE ROW)
            self.connection.execute("""
              CREATE TABLE IF NOT EXISTS company_tokens (
              company_id VARCHAR PRIMARY KEY,
              token_json TEXT NOT NULL,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP); """)

            
            logger.info("✅ Database tables initialized (Emails, Tokens, Users)")
            return True
        except Exception as e:
            logger.error(f"❌ Table creation error: {str(e)}")
            return False

    # --- User Authentication Methods ---

    def get_user_by_username(self, username):
        """Fetch a user by username for login verification."""
        try:
            q = "SELECT id, username, password_hash, role FROM users WHERE username = ?"
            row = self.connection.execute(q, [username]).fetchone()
            
            if not row:
                return None
            
            return {
                "id": str(row[0]),
                "username": row[1],
                "password_hash": row[2],
                "role": row[3]
            }
        except Exception as e:
            logger.error(f"Error getting user by username: {str(e)}")
            return None

    def create_user(self, username, password_hash, role='user'):
        """Create a new user."""
        try:
            # DuckDB handles UUID generation with gen_random_uuid() if default is set,
            # or we can let the DEFAULT value handle 'id' and 'created_at'.
            q = """
                INSERT INTO users (username, password_hash, role) 
                VALUES (?, ?, ?)
                RETURNING id
            """
            result = self.connection.execute(q, [username, password_hash, role]).fetchone()
            self.connection.commit()
            
            if result:
                return str(result[0])
            return None
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return None

    # --- Email Methods ---
    
    def insert_extraction(self, email_data, extraction_result):
        # ... (Same as your existing code) ...
        try:
            extraction_result_json = json.dumps(extraction_result)
            status = extraction_result.get('status', 'VALID')
            current_time = datetime.now()
            
            query = """
                INSERT INTO email_extractions (
                    gmail_id, sender, received_at, subject, body_text, 
                    extraction_result, extraction_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (gmail_id) DO UPDATE SET
                    extraction_result = EXCLUDED.extraction_result,
                    extraction_status = EXCLUDED.extraction_status,
                    updated_at = EXCLUDED.updated_at
            """
            
            self.connection.execute(query, [
                email_data.get('gmail_id'),
                email_data.get('sender', ''),
                email_data.get('received_at'),
                email_data.get('subject', ''),
                email_data.get('body_text', ''),
                extraction_result_json,
                status,
                current_time
            ])
            self.connection.commit()
            return True
        except Exception as e:
            if 'updated_at' in str(e) or 'Binder Error' in str(e):
                try:
                    self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")
                    return self.insert_extraction(email_data, extraction_result)
                except:
                    pass
            logger.error(f"Error inserting extraction: {str(e)}")
            return False

    def get_all_extractions(self, limit=100):
        # ... (Same as your existing code) ...
        try:
            result = self.connection.execute(
                "SELECT * FROM email_extractions ORDER BY received_at DESC LIMIT ?", 
                [limit]
            ).fetchall()
            
            columns = [
                'id', 'gmail_id', 'sender', 'received_at', 'subject', 
                'body_text', 'extraction_result', 'extraction_status', 
                'updated_at', 'created_at'
            ]
            
            extractions = []
            for row in result:
                item = dict(zip(columns, row))
                if isinstance(item.get('extraction_result'), str):
                    try:
                        item['extraction_result'] = json.loads(item['extraction_result'])
                    except:
                        pass
                extractions.append(item)
            return extractions
        except Exception as e:
            logger.error(f"Error getting extractions: {str(e)}")
            return []

    def get_extraction(self, gmail_id):
        # ... (Same as your existing code) ...
        try:
            result = self.connection.execute(
                "SELECT * FROM email_extractions WHERE gmail_id = ?", 
                [gmail_id]
            ).fetchone()
            
            if not result:
                return None
            
            columns = [
                'id', 'gmail_id', 'sender', 'received_at', 'subject', 
                'body_text', 'extraction_result', 'extraction_status', 
                'updated_at', 'created_at'
            ]
            item = dict(zip(columns, result))
            if isinstance(item.get('extraction_result'), str):
                try:
                    item['extraction_result'] = json.loads(item['extraction_result'])
                except:
                    pass
            return item
        except Exception as e:
            logger.error(f"Error getting extraction: {str(e)}")
            return None

    def update_extraction(self, gmail_id, extraction_result):
        # ... (Same as your existing code) ...
        try:
            extraction_result_json = json.dumps(extraction_result)
            current_time = datetime.now()
            self.connection.execute("""
                UPDATE email_extractions 
                SET extraction_result = ?, extraction_status = 'VALID', updated_at = ?
                WHERE gmail_id = ?
            """, [extraction_result_json, current_time, gmail_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating extraction: {str(e)}")
            return False

    # --- Auth Token Methods ---
    def save_user_token(self, user_id, token_json_str):
        # ... (Same as your existing code) ...
        try:
            current_time = datetime.now()
            query = """
                INSERT INTO user_tokens (user_id, token_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    token_json = EXCLUDED.token_json,
                    updated_at = EXCLUDED.updated_at
            """
            self.connection.execute(query, [user_id, token_json_str, current_time])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving user token: {str(e)}")
            return False

    def get_user_token(self, user_id):
        # ... (Same as your existing code) ...
        try:
            result = self.connection.execute("""
                SELECT token_json FROM user_tokens WHERE user_id = ?
            """, [user_id]).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error retrieving user token: {str(e)}")
            return None

    def delete_user_token(self, user_id):
        # ... (Same as your existing code) ...
        try:
            self.connection.execute("DELETE FROM user_tokens WHERE user_id = ?", [user_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting user token: {str(e)}")
            return False

    # --- Company Gmail Token Methods (Single Account) ---
    
    def get_company_token(self):
        """Retrieve the single company Gmail OAuth token."""
        return self.get_user_token(Config.COMPANY_GMAIL_ID)

    def save_company_token(self, token_json_str):
        """Save the single company Gmail OAuth token."""
        return self.save_user_token(Config.COMPANY_GMAIL_ID, token_json_str)

    def get_all_users(self):
        """Get all registered users for admin view."""
        try:
            result = self.connection.execute("SELECT id, username, role, created_at FROM users").fetchall()
            users = []
            for row in result:
                users.append({
                    "id": str(row[0]),
                    "username": row[1],
                    "role": row[2],
                    "created_at": row[3]
                })
            return users
        except Exception as e:
            logger.error(f"Error getting all users: {str(e)}")
            return []
        
        # --- Company Gmail Token Methods ---

def save_company_token(self, token_json: str):
    try:
        self.connection.execute("""
            INSERT INTO company_tokens (company_id, token_json)
            VALUES ('COMPANY', ?)
            ON CONFLICT(company_id)
            DO UPDATE SET
                token_json = excluded.token_json,
                updated_at = CURRENT_TIMESTAMP
        """, [token_json])
        self.connection.commit()
        return True
    except Exception as e:
        logger.error(f"Error saving company token: {str(e)}")
        return False


def get_company_token(self):
    try:
        row = self.connection.execute("""
            SELECT token_json FROM company_tokens
            WHERE company_id = 'COMPANY'
        """).fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"Error fetching company token: {str(e)}")
        return None


def delete_company_token(self):
    try:
        self.connection.execute("""
            DELETE FROM company_tokens
            WHERE company_id = 'COMPANY'
        """)
        self.connection.commit()
        return True
    except Exception as e:
        logger.error(f"Error deleting company token: {str(e)}")
        return False
