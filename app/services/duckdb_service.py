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
            # 'snapquote_db' is the name of your database in the cloud
            self.db_path = f'md:snapquote_db?motherduck_token={self.token}'
            self.is_cloud = True
            logger.info("🔌 Configured for MotherDuck Cloud Database")
        else:
            # ⚠️ Fallback for local development if no token
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
        try:
            # 1. Base table creation
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
                    updated_at TIMESTAMP,  -- ✅ Ensure this exists
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 2. ✅ Auto-Fix: Add updated_at if it was missing from an old version
            try:
                # We try to select the column. If it fails, we add it.
                self.connection.execute("SELECT updated_at FROM email_extractions LIMIT 1")
            except:
                logger.info("🛠️ Column 'updated_at' missing. Adding it now...")
                self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")

            # 3. User Tokens Table
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS user_tokens (
                    user_id VARCHAR PRIMARY KEY,
                    token_json JSON,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("✅ Database tables initialized")
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
            import json
            from datetime import datetime
            
            # 1. Prepare data
            extraction_result_json = json.dumps(extraction_result)
            status = extraction_result.get('status', 'VALID')
            current_time = datetime.now()  # ✅ Calc time here
            
            # 2. Query - Note we pass updated_at into the INSERT
            # and use EXCLUDED.updated_at for the UPDATE.
            query = """
                INSERT INTO email_extractions (
                    gmail_id, sender, received_at, subject, body_text, 
                    extraction_result, extraction_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (gmail_id) DO UPDATE SET
                    extraction_result = EXCLUDED.extraction_result,
                    extraction_status = EXCLUDED.extraction_status,
                    updated_at = EXCLUDED.updated_at
                RETURNING id
            """
            
            # 3. Execute - Pass current_time as the last argument
            result = self.connection.execute(query, [
                email_data.get('gmail_id'),
                email_data.get('sender', ''),
                email_data.get('received_at'),
                email_data.get('subject', ''),
                email_data.get('body_text', ''),
                extraction_result_json,
                status,
                current_time  # ✅ Passing Python datetime
            ]).fetchone()
            
            self.connection.commit()
            return result[0] if result else True

        except Exception as e:
            # Handle case where column might still be missing
            if 'updated_at' in str(e) or 'Binder Error' in str(e):
                logger.warning("Attempting to fix missing column 'updated_at'...")
                try:
                    self.connection.execute("ALTER TABLE email_extractions ADD COLUMN updated_at TIMESTAMP")
                    # Retry the insert recursively once
                    return self.insert_extraction(email_data, extraction_result)
                except:
                    pass
            
            logger.error(f"Error inserting extraction: {str(e)}")
            return False
    def get_all_extractions(self, limit=100):
        try:
            result = self.connection.execute(
                "SELECT * FROM email_extractions ORDER BY received_at DESC LIMIT ?", 
                [limit]
            ).fetchall()
            
            columns = ['id', 'gmail_id', 'sender', 'received_at', 'subject', 'body_text', 'extraction_result', 'extraction_status', 'created_at']
            extractions = []
            
            for row in result:
                item = dict(zip(columns, row))
                # Parse JSON string back to dict
                if isinstance(item['extraction_result'], str):
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
                
            columns = ['id', 'gmail_id', 'sender', 'received_at', 'subject', 'body_text', 'extraction_result', 'extraction_status', 'created_at']
            item = dict(zip(columns, result))
            
            if isinstance(item['extraction_result'], str):
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
            self.connection.execute("""
                UPDATE email_extractions 
                SET extraction_result = ?, extraction_status = 'VALID'
                WHERE gmail_id = ?
            """, [extraction_result_json, gmail_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating extraction: {str(e)}")
            return False

    # --- ✅ NEW: Auth Token Methods ---
    def save_user_token(self, user_id, token_json_str):
        """Saves or updates a user's Google OAuth token in the DB."""
        try:
            logger.info(f"Saving token for user_id: {user_id}, token_json length: {len(token_json_str) if token_json_str else 0}")
            
            # Use explicit timestamp for better compatibility with MotherDuck
            from datetime import datetime
            current_timestamp = datetime.now()
            
            query = """
                INSERT INTO user_tokens (user_id, token_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    token_json = EXCLUDED.token_json,
                    updated_at = EXCLUDED.updated_at
            """
            
            logger.debug(f"Executing query with user_id={user_id}, timestamp={current_timestamp}")
            self.connection.execute(query, [user_id, token_json_str, current_timestamp])
            self.connection.commit()
            
            logger.info(f"✅ Token saved successfully for user_id: {user_id}")
            
            # Verify the save
            verify_result = self.connection.execute(
                "SELECT user_id FROM user_tokens WHERE user_id = ?", 
                [user_id]
            ).fetchone()
            
            if verify_result:
                logger.info(f"✅ Verified: Token exists in database for user_id: {user_id}")
            else:
                logger.warning(f"⚠️ Warning: Token save committed but not found on verification for user_id: {user_id}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Error saving user token for user_id {user_id}: {str(e)}", exc_info=True)
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def get_user_token(self, user_id):
        """Retrieves a user's Google OAuth token from the DB."""
        try:
            logger.info(f"Querying user_tokens for user_id: {user_id}")
            result = self.connection.execute("""
                SELECT token_json FROM user_tokens WHERE user_id = ?
            """, [user_id]).fetchone()
            
            if result:
                token_json = result[0]
                logger.info(f"✅ Token found for user_id: {user_id} (length: {len(token_json) if token_json else 0})")
                return token_json
            else:
                logger.warning(f"⚠️ No token found for user_id: {user_id}")
                return None
        except Exception as e:
            logger.error(f"❌ Error retrieving user token for user_id {user_id}: {str(e)}", exc_info=True)
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