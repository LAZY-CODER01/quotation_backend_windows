import duckdb
import os
import logging
import json
from datetime import datetime

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
        """Create necessary tables (Emails + Auth Tokens)."""
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

            # 2. Auto-repair: Add updated_at if missing (resilience for old schemas)
            try:
                self.connection.execute("SELECT updated_at FROM email_extractions LIMIT 1")
            except:
                logger.info("🛠️ Column 'updated_at' missing. Adding it now...")
                self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")

            # 3. Create User Tokens Table (Fixes logout on deploy)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS user_tokens (
                    user_id VARCHAR PRIMARY KEY,
                    token_json JSON,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("✅ Database tables initialized (Emails + Tokens)")
            return True
        except Exception as e:
            logger.error(f"❌ Table creation error: {str(e)}")
            return False

    # --- Email Methods ---
    
    def insert_extraction(self, email_data, extraction_result):
        """
        Insert or Update extraction.
        Fixes 'CURRENT_TIMESTAMP' error by passing time from Python.
        """
        try:
            # 1. Prepare data
            extraction_result_json = json.dumps(extraction_result)
            status = extraction_result.get('status', 'VALID')
            current_time = datetime.now()  # ✅ Calc time here to avoid binder errors
            
            # 2. Query
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
            
            # 3. Execute
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
            # Auto-repair attempt for missing column
            if 'updated_at' in str(e) or 'Binder Error' in str(e):
                logger.warning("Attempting to fix missing column 'updated_at'...")
                try:
                    self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")
                    return self.insert_extraction(email_data, extraction_result)
                except:
                    pass
            
            logger.error(f"Error inserting extraction: {str(e)}")
            return False

    def get_all_extractions(self, limit=100):
        try:
            # Execute query
            result = self.connection.execute(
                "SELECT * FROM email_extractions ORDER BY received_at DESC LIMIT ?", 
                [limit]
            ).fetchall()
            
            # ✅ EXPLICITLY define columns to match the DB schema (10 columns)
            # This prevents the "zip" mismatch crash
            columns = [
                'id', 'gmail_id', 'sender', 'received_at', 'subject', 
                'body_text', 'extraction_result', 'extraction_status', 
                'updated_at', 'created_at'
            ]
            
            extractions = []
            for row in result:
                # Zip row data with column names
                # We slice row[:len(columns)] just in case the DB has extra columns we don't know about
                item = dict(zip(columns, row))
                
                # Parse JSON string back to dict
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
        try:
            result = self.connection.execute(
                "SELECT * FROM email_extractions WHERE gmail_id = ?", 
                [gmail_id]
            ).fetchone()
            
            if not result:
                return None
                
            # ✅ EXPLICITLY define columns here as well
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
        try:
            extraction_result_json = json.dumps(extraction_result)
            # Use current_time from Python to be safe
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
        """Saves or updates a user's Google OAuth token in the DB."""
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
        """Retrieves a user's Google OAuth token from the DB."""
        try:
            result = self.connection.execute("""
                SELECT token_json FROM user_tokens WHERE user_id = ?
            """, [user_id]).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error retrieving user token: {str(e)}")
            return None

    def delete_user_token(self, user_id):
        """Deletes a user's token (Logout)."""
        try:
            self.connection.execute("DELETE FROM user_tokens WHERE user_id = ?", [user_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting user token: {str(e)}")
            return False