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
            self.create_table() 
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
            # 1. Create Email Extractions Table with Ticket & Priority Fields
            self.connection.execute("""
                CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;
                CREATE TABLE IF NOT EXISTS email_extractions (
                    id INTEGER DEFAULT nextval('id_sequence'),
                    gmail_id VARCHAR PRIMARY KEY,
                    ticket_number VARCHAR,            -- Format: DBQ-2025-01-001
                    ticket_status VARCHAR DEFAULT 'INBOX',
                    ticket_priority VARCHAR DEFAULT 'NORMAL',
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

            # 2. Auto-repair: Add columns if they are missing (for existing DBs)
            self._ensure_column_exists("email_extractions", "updated_at", "TIMESTAMP")
            self._ensure_column_exists("email_extractions", "ticket_number", "VARCHAR")
            self._ensure_column_exists("email_extractions", "ticket_status", "VARCHAR DEFAULT 'INBOX'")
            self._ensure_column_exists("email_extractions", "ticket_priority", "VARCHAR DEFAULT 'NORMAL'")

            # 3. Create User Tokens Table (Google OAuth)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS user_tokens (
                    user_id VARCHAR PRIMARY KEY,
                    token_json JSON,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 4. Create Users Table (App Authentication)
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
                    id INTEGER PRIMARY KEY,
                    token_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("✅ Database tables initialized (Emails, Tokens, Users)")
            return True
        except Exception as e:
            logger.error(f"❌ Table creation error: {str(e)}")
            return False

    def _ensure_column_exists(self, table, column, data_type):
        """Helper to safely add columns to existing tables."""
        try:
            self.connection.execute(f"SELECT {column} FROM {table} LIMIT 1")
        except:
            try:
                logger.info(f"🛠️ Column '{column}' missing in {table}. Adding it now...")
                self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {data_type}")
            except Exception as e:
                logger.error(f"Failed to add column {column}: {e}")

    # --- Ticket Logic ---

    def _generate_next_ticket_number(self):
        """
        Generates the next ticket number in the format DBQ-YYYY-MM-XXX.
        Resets sequence for every new month.
        """
        now = datetime.now()
        year = now.year
        month = f"{now.month:02d}"
        prefix = f"DBQ-{year}-{month}-"

        try:
            # Find the highest ticket number for the CURRENT month
            query = f"""
                SELECT ticket_number 
                FROM email_extractions 
                WHERE ticket_number LIKE '{prefix}%'
                ORDER BY ticket_number DESC 
                LIMIT 1
            """
            result = self.connection.execute(query).fetchone()

            if result and result[0]:
                # Extract the last 3 digits and increment
                last_ticket = result[0]
                try:
                    last_seq = int(last_ticket.split('-')[-1])
                    new_seq = last_seq + 1
                except ValueError:
                    new_seq = 1
            else:
                new_seq = 1

            return f"{prefix}{new_seq:03d}"
        except Exception as e:
            logger.error(f"Error generating ticket number: {e}")
            return f"{prefix}ERR-{int(datetime.now().timestamp())}"

    # --- Email Methods ---
    
    def insert_extraction(self, email_data, extraction_result):
        try:
            extraction_result_json = json.dumps(extraction_result)
            status = extraction_result.get('status', 'VALID')
            current_time = datetime.now()
            
            # 1. Generate Ticket Number
            ticket_number = self._generate_next_ticket_number()
            ticket_status = "INBOX"
            
            # 2. Determine Priority (Auto-detect)
            subject_lower = email_data.get('subject', '').lower()
            if any(x in subject_lower for x in ['urgent', 'asap', 'immediate', 'emergency']):
                ticket_priority = 'URGENT'
            else:
                ticket_priority = 'NORMAL'

            query = """
                INSERT INTO email_extractions (
                    gmail_id, ticket_number, ticket_status, ticket_priority,
                    sender, received_at, subject, body_text, 
                    extraction_result, extraction_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (gmail_id) DO UPDATE SET
                    extraction_result = EXCLUDED.extraction_result,
                    extraction_status = EXCLUDED.extraction_status,
                    updated_at = EXCLUDED.updated_at
            """
            
            self.connection.execute(query, [
                email_data.get('gmail_id'),
                ticket_number,
                ticket_status,
                ticket_priority,
                email_data.get('sender', ''),
                email_data.get('received_at'),
                email_data.get('subject', ''),
                email_data.get('body_text', ''),
                extraction_result_json,
                status,
                current_time
            ])
            self.connection.commit()
            logger.info(f"✅ Email saved: Ticket {ticket_number} [{ticket_priority}]")
            return True
        except Exception as e:
            logger.error(f"Error inserting extraction: {str(e)}")
            return False

    def update_ticket_status(self, ticket_number, new_status):
        """Update workflow status (OPEN -> CLOSED)."""
        try:
            self.connection.execute("""
                UPDATE email_extractions 
                SET ticket_status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE ticket_number = ?
            """, [new_status, ticket_number])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket status: {e}")
            return False

    def get_all_extractions(self, limit=100):
        try:
            # ✅ FIX: Explicitly select columns in the EXACT order of the python list below
            # DO NOT use SELECT *
            query_columns = """
                id, gmail_id, ticket_number, ticket_status, ticket_priority, 
                sender, received_at, subject, body_text, 
                extraction_result, extraction_status, updated_at, created_at
            """
            
            result = self.connection.execute(
                f"SELECT {query_columns} FROM email_extractions ORDER BY received_at DESC LIMIT ?", 
                [limit]
            ).fetchall()
            
            columns = [
                'id', 'gmail_id', 'ticket_number', 'ticket_status', 'ticket_priority', 
                'sender', 'received_at', 'subject', 'body_text', 
                'extraction_result', 'extraction_status', 'updated_at', 'created_at'
            ]
            
            extractions = []
            for row in result:
                # Create dict safely
                item = dict(zip(columns, row))
                
                # Parse JSON string if needed
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
            # ✅ FIX: Explicitly select columns here too
            query_columns = """
                id, gmail_id, ticket_number, ticket_status, ticket_priority, 
                sender, received_at, subject, body_text, 
                extraction_result, extraction_status, updated_at, created_at
            """

            result = self.connection.execute(
                f"SELECT {query_columns} FROM email_extractions WHERE gmail_id = ?", 
                [gmail_id]
            ).fetchone()
            
            if not result:
                return None
            
            columns = [
                'id', 'gmail_id', 'ticket_number', 'ticket_status', 'ticket_priority', 
                'sender', 'received_at', 'subject', 'body_text', 
                'extraction_result', 'extraction_status', 'updated_at', 'created_at'
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
        try:
            result = self.connection.execute("""
                SELECT token_json FROM user_tokens WHERE user_id = ?
            """, [user_id]).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error retrieving user token: {str(e)}")
            return None

    def delete_user_token(self, user_id):
        try:
            self.connection.execute("DELETE FROM user_tokens WHERE user_id = ?", [user_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting user token: {str(e)}")
            return False

    # --- User Authentication Methods ---

    def get_user_by_username(self, username):
        try:
            q = "SELECT id, username, password_hash, role FROM users WHERE username = ?"
            row = self.connection.execute(q, [username]).fetchone()
            if not row: return None
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
        try:
            q = """
                INSERT INTO users (username, password_hash, role) 
                VALUES (?, ?, ?)
                RETURNING id
            """
            result = self.connection.execute(q, [username, password_hash, role]).fetchone()
            self.connection.commit()
            if result: return str(result[0])
            return None
        except Exception as e:
            logger.error(f"Error creating user: {str(e)}")
            return None

    # --- Company Token Methods ---

    def save_company_token(self, token_json: str):
        try:
            self.connection.execute("""
                INSERT INTO company_tokens (id, token_json)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET
                    token_json = excluded.token_json
            """, [token_json])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving company token: {str(e)}")
            return False

    def get_company_token(self):
        try:
            row = self.connection.execute("SELECT token_json FROM company_tokens WHERE id = 1").fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Error fetching company token: {str(e)}")
            return None

    def delete_company_token(self):
        try:
            self.connection.execute("DELETE FROM company_tokens WHERE id = 1")
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting company token: {str(e)}")
            return False
    
    def update_ticket_priority(self, ticket_number, new_priority):
        """Update the priority of a ticket (NORMAL <-> URGENT)."""
        try:
            valid_priorities = ['NORMAL', 'URGENT']
            if new_priority not in valid_priorities:
                logger.warning(f"Invalid priority '{new_priority}'")
                return False

            self.connection.execute("""
                UPDATE email_extractions 
                SET ticket_priority = ?, updated_at = CURRENT_TIMESTAMP
                WHERE ticket_number = ?
            """, [new_priority, ticket_number])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket priority: {e}")
            return False    
    def update_ticket_status(self, ticket_number, new_status):
        """Update the workflow status of a ticket."""
        try:
            # 1. Define valid statuses
            valid_statuses = [
                'INBOX', 
                'SENT', 'ORDER_CONFIRMED', 'ORDER_COMPLETED', 'CLOSED', 
            ]
            
            if new_status not in valid_statuses:
                logger.warning(f"Invalid status '{new_status}' provided.")
                return False

            # 2. Update DB
            self.connection.execute("""
                UPDATE email_extractions 
                SET ticket_status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE ticket_number = ?
            """, [new_status, ticket_number])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket status: {e}")
            return False   