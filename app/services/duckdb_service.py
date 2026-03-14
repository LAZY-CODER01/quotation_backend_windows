import re
import duckdb
import os
import logging
import json
from datetime import datetime, timedelta
import duckdb
import os
import logging
import json
from datetime import datetime
import uuid
from config.settings import Config
from app.utils.helpers import get_uae_time, parse_date_string

import threading

logger = logging.getLogger(__name__)

# Global lock to prevent concurrent schema definition
_schema_lock = threading.Lock()

class DuckDBService:
    def __init__(self):
        """
        Initialize DuckDB connection.
        Connects to MotherDuck if token is present, otherwise falls back to local file.
        """
        self.token = os.getenv('MOTHERDUCK_TOKEN')
        
        if self.token:
            #   Connect to MotherDuck Cloud
            self.db_path = f'md:snapquote_db?motherduck_token={self.token}'
            self.is_cloud = True
            logger.info(" Configured for MotherDuck Cloud Database")
        else:
            # ⚠️ Fallback for local development
            self.db_path = 'local_dev.duckdb'
            self.is_cloud = False
            logger.warning(" MOTHERDUCK_TOKEN not found. Using local file 'local_dev.duckdb'")
            
        self.connection = None

    def connect(self):
        """Establish connection to the database."""
        try:
            self.connection = duckdb.connect(self.db_path)
            self.create_table() 
            return True
        except Exception as e:
            logger.error(f" Database connection error: {str(e)}")
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
        """Create necessary tables (Emails + Auth Tokens + Users + Files)."""
        with _schema_lock:
            try:
                # 1. Base Sequences
                self.connection.execute("CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;")

                # 2. Main Emails Table
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS email_extractions (
                        id INTEGER DEFAULT nextval('id_sequence'),
                        gmail_id VARCHAR PRIMARY KEY,
                        ticket_number VARCHAR UNIQUE,
                        ticket_status VARCHAR DEFAULT 'INBOX',
                        ticket_priority VARCHAR DEFAULT 'NORMAL',
                        quotation_files JSON DEFAULT '[]',
                        cpo_files JSON DEFAULT '[]',
                        activity_logs JSON DEFAULT '[]',
                        quotation_amount VARCHAR,
                        sender VARCHAR,
                        company_name VARCHAR,
                        received_at TIMESTAMP,
                        subject VARCHAR,
                        body_text TEXT,
                        extraction_result JSON,
                        extraction_status VARCHAR,
                        updated_at TIMESTAMP,
                        assigned_to VARCHAR,
                        internal_notes JSON DEFAULT '[]',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP,
                        confirmed_at TIMESTAMP,
                        closed_at TIMESTAMP
                    );
                """)

                # Migration: add status timestamp columns if missing
                try:
                    existing_cols = [r[0] for r in self.connection.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name='email_extractions'"
                    ).fetchall()]
                    for col in ['sent_at', 'confirmed_at', 'closed_at']:
                        if col not in existing_cols:
                            self.connection.execute(f"ALTER TABLE email_extractions ADD COLUMN {col} TIMESTAMP")
                            logger.info(f"Added column {col} to email_extractions")
                except Exception as col_err:
                    logger.warning(f"Column migration check: {col_err}")

                # 3. Normalized File Tables
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS quotations (
                        id INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'),
                        ticket_number VARCHAR,
                        reference_id VARCHAR,
                        file_name VARCHAR,
                        file_url VARCHAR,
                        amount VARCHAR,
                        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (ticket_number) REFERENCES email_extractions(ticket_number)
                    );
                """)

                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS cpo_orders (
                        id INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'),
                        ticket_number VARCHAR,
                        reference_id VARCHAR,
                        file_name VARCHAR,
                        file_url VARCHAR,
                        amount VARCHAR,
                        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (ticket_number) REFERENCES email_extractions(ticket_number)
                    );
                """)

                # 4. Auth Tables
                self.connection.execute("CREATE TABLE IF NOT EXISTS user_tokens (user_id VARCHAR PRIMARY KEY, token_json JSON, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
                self.connection.execute("CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), username VARCHAR UNIQUE, password_hash VARCHAR, employee_code VARCHAR UNIQUE, role VARCHAR DEFAULT 'user', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
                self.connection.execute("CREATE TABLE IF NOT EXISTS company_tokens (id INTEGER PRIMARY KEY, token_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")

                # 5. Clients Table
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS clients (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR,
                        business_name VARCHAR,
                        email VARCHAR UNIQUE,
                        phone VARCHAR,
                        tags JSON DEFAULT '[]',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

                logger.info("Database tables initialized (Emails, Tokens, Users, Quotations, CPO)")
            except Exception as e:
                logger.error(f"Table creation error: {str(e)}")
                return False

        #   Run ALTER TABLE migrations OUTSIDE the schema lock so they don't
        # cause write-write conflicts with other threads' CREATE TABLE statements.
        self._ensure_column_exists("users", "employee_code", "VARCHAR")
        self._ensure_column_exists("email_extractions", "company_name", "VARCHAR")
        return True

    def _ensure_column_exists(self, table, column, data_type):
        """Helper to safely add a column to an existing table if it's missing."""
        try:
            self.connection.execute(f"SELECT {column} FROM {table} LIMIT 1")
        except Exception:
            try:
                logger.info(f"️ Column '{column}' missing in {table}. Adding it now...")
                self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {data_type}")
            except Exception as e:
                # Silently ignore write-write conflicts (another worker already added it)
                err_str = str(e)
                if 'write-write conflict' in err_str or 'already exists' in err_str.lower():
                    logger.warning(f" Skipping ALTER for '{column}' on '{table}': {err_str}")
                else:
                    logger.error(f"Failed to add column {column} to {table}: {e}")

    # --- Ticket Logic ---

    def _generate_next_ticket_number(self):
        """
        Generates the next ticket number in the format DBQ-YYYY-MM-XXX.
        Resets sequence for every new month.
        """
        now = get_uae_time()
        year = now.year
        month = f"{now.month:02d}"
        prefix = f"TKT-{year}-{month}-"

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
            
            ticket_number = self._generate_next_ticket_number()
            # Auto-detect urgency
            priority = 'URGENT' if any(x in email_data.get('subject', '').lower() for x in ['urgent', 'asap']) else 'NORMAL'

            # -----------------------------------------------------------
            #  AUTO-ASSIGNMENT LOGIC
            # -----------------------------------------------------------
            assigned_to_user = None
            body_text = email_data.get('body_text', '')
            if body_text:
                # Regex to find EMP/DBSQ code safely
                # \b(EMP|DBSQ) : Word boundary + Prefix (EMP or DBSQ)
                # (?: ... | ... ) : Alternatives
                # 1. [\s-]*([a-zA-Z0-9]+) : Optional Separator + Alphanumeric ID (e.g. DBSQ001, DBSQ-001)
                emp_pattern = r'(?i)\b(EMP|DBSQ)[\s-]*([a-zA-Z0-9]+)' 
                match = re.search(emp_pattern, body_text)
                
                if match:
                    # Prefix is group 1
                    prefix = match.group(1).upper()
                    # ID is group 2
                    emp_id_part = match.group(2)
                    full_code_str = match.group(0)
                    
                    # Search for the user using variations of the ID with the found prefix
                    candidates = [
                        full_code_str,          # Literal match found
                        f"{prefix}{emp_id_part}",    # No hyphen (DBSQ001)
                        f"{prefix}-{emp_id_part}",   # Standard hyphenated (DBSQ-001)
                        emp_id_part             # Just the ID (Weakest match)
                    ]
                    
                    placeholders = ','.join(['?'] * len(candidates))
                    user_query = f"SELECT username FROM users WHERE employee_code IN ({placeholders})"
                    user_res = self.connection.execute(user_query, candidates).fetchone()
                    
                    if user_res:
                        assigned_to_user = user_res[0]
                        logger.info(f" Auto-assigned ticket {ticket_number} to {assigned_to_user} (Found EMP code: {full_code_str})")

            # Extract metadata from AI result
            extracted_company = extraction_result.get('company_name', '')
            extracted_sender = extraction_result.get('sender_name', '')
            
            # Use extracted sender name if available, otherwise fallback to email data
            final_sender = extracted_sender if extracted_sender else email_data.get('sender', '')

            query = """
                INSERT INTO email_extractions (
                    gmail_id, ticket_number, ticket_status, ticket_priority,
                    sender, received_at, subject, body_text, 
                    extraction_result, extraction_status, updated_at, assigned_to, created_at,
                    company_name
                ) VALUES (?, ?, 'INBOX', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (gmail_id) DO UPDATE SET
                    extraction_result = EXCLUDED.extraction_result,
                    extraction_status = EXCLUDED.extraction_status,
                    updated_at = EXCLUDED.updated_at,
                    company_name = COALESCE(EXCLUDED.company_name, email_extractions.company_name)
            """
            self.connection.execute(query, [
                email_data.get('gmail_id'), ticket_number, priority,
                final_sender, email_data.get('received_at'),
                email_data.get('subject', ''), body_text,
                extraction_result_json, status, get_uae_time(), assigned_to_user, get_uae_time(),
                extracted_company
            ])
            self.connection.commit()

            # Auto-add company as client from AI extraction if not already present
            self.ensure_client_from_extraction(email_data, extraction_result)

            return True
        except Exception as e:
            logger.error(f"Insert Error: {e}")
            return False

    def create_manual_ticket(self, ticket_data, user):
        """
        Creates a new ticket manually (not from email).
        """
        try:
            ticket_number = self._generate_next_ticket_number()
            gmail_id = f"manual_{uuid.uuid4().hex}" # Generate pseudo-Gmail ID
            
            # Combine Sender Name and Email
            sender_str = f"{ticket_data.get('sender_name')} <{ticket_data.get('sender_email')}>"
            
            # Default extraction result for manual tickets
            # Summary is now just the subject since description is removed
            extraction_result = {
                "summary": ticket_data.get('subject', ''),
                "priority": ticket_data.get('priority', 'NORMAL'),
                "status": "VALID"
            }
            
            query = """
                INSERT INTO email_extractions (
                    gmail_id, ticket_number, ticket_status, ticket_priority,
                    sender, received_at, subject, body_text, 
                    extraction_result, extraction_status, updated_at, assigned_to, created_at,
                    quotation_files, cpo_files, activity_logs, internal_notes, company_name
                ) VALUES (?, ?, 'INBOX', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', ?)
            """
            
            self.connection.execute(query, [
                gmail_id, 
                ticket_number, 
                ticket_data.get('priority', 'NORMAL'),
                sender_str,
                get_uae_time(),
                ticket_data.get('subject'),
                '', # Body text is empty as description is removed
                json.dumps(extraction_result),
                'VALID',
                get_uae_time(),
                user.get('username'), 
                get_uae_time(),
                ticket_data.get('company_name')
            ])
            
            self.connection.commit()
            
            # Log Activity
            self.add_activity_log(gmail_id, "TICKET_CREATED", "Ticket created manually", user.get('username'))
            
            return ticket_number
        except Exception as e:
            logger.error(f"Manual Ticket Creation Error: {e}")
            return None
    def update_ticket_status(self, ticket_number, new_status):
        """Update workflow status and set status timestamp."""
        try:
            now = get_uae_time()
            status_upper = (new_status or '').upper()

            # Set the appropriate timestamp column
            ts_col = None
            if status_upper == 'SENT':
                ts_col = 'sent_at'
            elif status_upper == 'ORDER_CONFIRMED':
                ts_col = 'confirmed_at'
            elif status_upper in ('CLOSED', 'ORDER_COMPLETED'):
                ts_col = 'closed_at'

            if ts_col:
                self.connection.execute(f"""
                    UPDATE email_extractions 
                    SET ticket_status = ?, updated_at = ?, {ts_col} = ?
                    WHERE ticket_number = ?
                """, [new_status, now, now, ticket_number])
            else:
                self.connection.execute("""
                    UPDATE email_extractions 
                    SET ticket_status = ?, updated_at = ?
                    WHERE ticket_number = ?
                """, [new_status, now, ticket_number])

            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket status: {e}")
            return False

    def get_gmail_id_from_ticket(self, ticket_number):
        """Helper to get gmail_id from ticket_number."""
        try:
            result = self.connection.execute(
                "SELECT gmail_id FROM email_extractions WHERE ticket_number = ?", 
                [ticket_number]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting gmail_id from ticket: {e}")
            return None

    def get_all_extractions(self, limit=1000, status_filter=None, user_role='user', username=None, days=None, before_date=None, since=None, start_date=None, end_date=None):
        try:
            #   Added quotation_files and quotation_amount to the list
            cols = """
                id, gmail_id, ticket_number, ticket_status, ticket_priority, 
                quotation_files, cpo_files, quotation_amount,
                sender, company_name, received_at, subject, body_text, internal_notes, activity_logs,
                extraction_result, extraction_status, updated_at, created_at, assigned_to
            """
            
            query = f"SELECT {cols} FROM email_extractions WHERE 1=1"
            params = []

            # 1. Delta Sync (Highest Priority)
            if since:
                # Fetch records UPDATED since 'since' timestamp
                query += " AND updated_at > ?"
                try:
                    # Ensure format matches DB timestamp if needed, or rely on flexible parsing
                    params.append(since)
                except:
                    pass

            # 2. Date Range Filtering (Only if not doing a pure delta sync)
            else:
                #   Explicit Date Range (New Feature)
                if start_date and end_date:
                    query += " AND received_at >= ? AND received_at <= ?"
                    params.append(start_date)
                    params.append(end_date)
                
                elif days:
                    try:
                        # Postgres/DuckDB syntax: CURRENT_TIMESTAMP - INTERVAL 'X days'
                        # But parameterizing the number of days safely:
                        query += f" AND received_at >= CURRENT_DATE - INTERVAL {int(days)} DAY"
                    except:
                        pass
                
                if before_date:
                    query += " AND received_at < ?"
                    params.append(before_date)

            if status_filter:
                query += " AND ticket_status = ?"
                params.append(status_filter)
            
            #   RBAC: Users only see assigned tickets (Unassigned tickets hidden)
            if user_role != 'ADMIN':
                query += " AND assigned_to = ?"
                params.append(username) # Current User

            query += " ORDER BY received_at DESC LIMIT ?"
            params.append(limit)

            result = self.connection.execute(query, params).fetchall()
            
            col_names = [c.strip() for c in cols.split(',')]
            extractions = []
            
            for row in result:
                item = dict(zip(col_names, row))
                
                # Parse JSON fields safely
                for field in ['extraction_result', 'internal_notes', 'activity_logs']:
                    if isinstance(item.get(field), str):
                        try:
                            item[field] = json.loads(item[field])
                        except:
                            item[field] = [] if field in ['internal_notes', 'activity_logs'] else {}
                
                # --- Fetch Normalized Files ---
                try:
                    ticket_number = item.get('ticket_number')
                    if ticket_number:
                        # Fetch Quotations
                        #   FIX: Fetch file_name/file_url and alias to name/url for frontend compatibility
                        q_files = self.connection.execute("""
                            SELECT id, file_name, file_url, amount, uploaded_at, reference_id 
                            FROM quotations WHERE ticket_number = ?
                        """, [ticket_number]).fetchall()
                        
                        item['quotation_files'] = [
                            {
                                'id': q[0], 
                                'name': q[1], 
                                'url': q[2], 
                                'amount': q[3], 
                                'uploaded_at': q[4].isoformat() if q[4] else None, 
                                'reference_id': q[5]
                            }
                            for q in q_files
                        ]

                        # Fetch CPO Files
                        c_files = self.connection.execute("""
                            SELECT id, file_name, file_url, amount, uploaded_at, reference_id 
                            FROM cpo_orders WHERE ticket_number = ?
                        """, [ticket_number]).fetchall()
                        
                        item['cpo_files'] = [
                            {
                                'id': c[0], 
                                'name': c[1], 
                                'url': c[2], 
                                'amount': c[3], 
                                'uploaded_at': c[4].isoformat() if c[4] else None, 
                                'reference_id': c[5]
                            }
                            for c in c_files
                        ]
                    else:
                        item['quotation_files'] = []
                        item['cpo_files'] = []
                except Exception as e:
                    #   FIX: Log specific error but don't crash
                    logger.warning(f"Failed to fetch linked files for {ticket_number}: {e}")
                    item['quotation_files'] = []
                    item['cpo_files'] = []

                extractions.append(item)
            return extractions
        except Exception as e:
            logger.error(f"Error getting extractions: {str(e)}")
            return []

    def get_extraction(self, gmail_id):
        try:
            #   FIX: Added quotation_files and quotation_amount here too
            cols = """
                id, gmail_id, ticket_number, ticket_status, ticket_priority, 
                quotation_files, quotation_amount,
                sender, company_name, received_at, subject, body_text, internal_notes, activity_logs,
                extraction_result, extraction_status, updated_at, created_at,assigned_to
            """


            result = self.connection.execute(
                f"SELECT {cols} FROM email_extractions WHERE gmail_id = ?", 
                [gmail_id]
            ).fetchone()
            
            if not result: return None
            
            col_names = [c.strip() for c in cols.split(',')]
            item = dict(zip(col_names, result))
            
            # Parse JSON fields safely
            for field in ['extraction_result', 'internal_notes', 'activity_logs']:
                if isinstance(item.get(field), str):
                    try:
                        item[field] = json.loads(item[field])
                    except:
                        item[field] = [] if field in ['internal_notes', 'activity_logs'] else {}
            
            # --- Fetch Normalized Files ---
            try:
                ticket_number = item.get('ticket_number')
                if ticket_number:
                    # Fetch Quotations
                    q_files = self.connection.execute("""
                        SELECT id, file_name, file_url, amount, uploaded_at, reference_id 
                        FROM quotations WHERE ticket_number = ?
                    """, [ticket_number]).fetchall()
                    item['quotation_files'] = [
                        {'id': q[0], 'name': q[1], 'url': q[2], 'amount': q[3], 'uploaded_at': q[4].isoformat() if q[4] else None, 'reference_id': q[5]}
                        for q in q_files
                    ]

                    # Fetch CPO Files
                    c_files = self.connection.execute("""
                        SELECT id, file_name, file_url, amount, uploaded_at, reference_id 
                        FROM cpo_orders WHERE ticket_number = ?
                    """, [ticket_number]).fetchall()
                    item['cpo_files'] = [
                        {'id': c[0], 'name': c[1], 'url': c[2], 'amount': c[3], 'uploaded_at': c[4].isoformat() if c[4] else None, 'reference_id': c[5]}
                        for c in c_files
                    ]
                else:
                    item['quotation_files'] = []
                    item['cpo_files'] = []
            except Exception as e:
                    logger.warning(f"Failed to fetch linked files for {ticket_number}: {e}")
                    item['quotation_files'] = []
                    item['cpo_files'] = []

            return item
        except Exception as e:
            logger.error(f"Error getting extraction: {str(e)}")
            return None

    def update_extraction(self, gmail_id, extraction_result):
        try:
            self.connection.execute("UPDATE email_extractions SET extraction_result = ?, updated_at = ? WHERE gmail_id = ?", 
                                  [json.dumps(extraction_result), get_uae_time(), gmail_id])
            self.connection.commit()
            return True
        except: return False

    # --- Auth Token Methods ---
    
    def save_user_token(self, user_id, token_json_str):
        try:
            current_time = get_uae_time()
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
            #   Fetch employee_code as well
            q = "SELECT id, username, password_hash, role, employee_code FROM users WHERE username = ?"
            row = self.connection.execute(q, [username]).fetchone()
            if not row: return None
            return {
                "id": str(row[0]),
                "username": row[1],
                "password_hash": row[2],
                "role": row[3],
                "employee_code": row[4] 
            }
        except Exception as e:
            logger.error(f"Error getting user by username: {str(e)}")
            return None

    def get_user_by_id(self, user_id):
        try:
            q = "SELECT id, username, password_hash, role, employee_code FROM users WHERE id = ?"
            row = self.connection.execute(q, [user_id]).fetchone()
            if not row: return None
            return {
                "id": str(row[0]),
                "username": row[1],
                "password_hash": row[2],
                "role": row[3],
                "employee_code": row[4]
            }
        except Exception as e:
            logger.error(f"Error getting user by ID: {str(e)}")
            return None

    def update_user_password(self, user_id, new_password):
        try:
            self.connection.execute("UPDATE users SET password_hash = ? WHERE id = ?", [new_password, user_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating password: {e}")
            return False

    def update_user_details(self, user_id, username=None, password=None, role=None, employee_code=None):
        try:
            # Build query dynamically
            fields = []
            params = []
            
            if username:
                fields.append("username = ?")
                params.append(username)
            if password:
                fields.append("password_hash = ?")
                params.append(password)
            if role:
                fields.append("role = ?")
                params.append(role)
            if employee_code:
                fields.append("employee_code = ?")
                params.append(employee_code)
                
            if not fields:
                return True # Nothing to update
                
            params.append(user_id)
            query = f"UPDATE users SET {', '.join(fields)} WHERE id = ?"
            
            self.connection.execute(query, params)
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating user details: {e}")
            return False

    def delete_user(self, user_id):
        try:
            # Also delete associated token
            self.connection.execute("DELETE FROM user_tokens WHERE user_id = ?", [user_id])
            self.connection.execute("DELETE FROM users WHERE id = ?", [user_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            return False

    def create_user(self, username, password_hash, employee_code=None, role='user'):
        try:
            # Generate employee_code if not provided
            if not employee_code:
                employee_code = self._generate_next_employee_code()
            
            # Ensure it starts with DBSQ- if manually entered (optional enforcement)
            if employee_code and not employee_code.startswith('DBSQ-'):
               # We can either enforce it or just let it be. Stick to autogen for consistency usually.
               pass

            # Check if username or employee_code already exists
            check = self.connection.execute(
                "SELECT 1 FROM users WHERE username = ? OR employee_code = ?", 
                [username, employee_code]
            ).fetchone()
            
            if check:
                logger.warning(f"User creation failed: Username '{username}' or Code '{employee_code}' already exists.")
                return None

            q = """
                INSERT INTO users (username, password_hash, employee_code, role, created_at) 
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
            """
            result = self.connection.execute(q, [username, password_hash, employee_code, role, get_uae_time()]).fetchone()
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
    
    def update_ticket_priority(self, gmail_id, new_priority):
        """Update the priority of a ticket (NORMAL <-> URGENT)."""
        try:
            valid_priorities = ['NORMAL', 'URGENT']
            if new_priority not in valid_priorities:
                logger.warning(f"Invalid priority '{new_priority}'")
                return False

            self.connection.execute("""
                UPDATE email_extractions 
                SET ticket_priority = ?, updated_at = ?
                WHERE gmail_id = ?
            """, [new_priority, get_uae_time(), gmail_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket priority: {e}")
            return False   
    def _generate_next_id(self, prefix, table_name, column_name):
        """
        Generates a globally sequential ID in the format PREFIX-YYYY-MM-XXX.
        e.g., DBQ-2025-02-001, PO-2025-02-005
        """
        try:
            now = get_uae_time()
            year = now.year
            month = f"{now.month:02d}"
            base_pattern = f"{prefix}-{year}-{month}-" # DBQ-2025-02-

            # Query to find the highest ID for this month
            # We filter by LIKE pattern and order descending
            query = f"""
                SELECT {column_name}
                FROM {table_name}
                WHERE {column_name} LIKE '{base_pattern}%'
                ORDER BY {column_name} DESC
                LIMIT 1
            """
            result = self.connection.execute(query).fetchone()

            if result and result[0]:
                last_id = result[0] # e.g., DBQ-2025-02-005
                try:
                    # Extract last part
                    last_seq = int(last_id.split('-')[-1])
                    new_seq = last_seq + 1
                except ValueError:
                    new_seq = 1
            else:
                new_seq = 1

            return f"{base_pattern}{new_seq:03d}"
        except Exception as e:
            logger.error(f"Error generating ID for {prefix}: {e}")
            return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _generate_next_employee_code(self):
        """
        Generates the next employee code in the format DBSQXXX (No hyphen).
        """
        try:
            prefix = "DBSQ"
            # Find the highest employee code
            query = f"""
                SELECT employee_code 
                FROM users 
                WHERE employee_code LIKE '{prefix}%'
                ORDER BY employee_code DESC 
                LIMIT 1
            """
            result = self.connection.execute(query).fetchone()

            if result and result[0]:
                last_code = result[0]
                try:
                    # Remove prefix and parse int
                    # Handle both legacy (hyphen) and new (no hyphen) if mixed
                    code_part = last_code.replace(prefix, '').replace('-', '')
                    last_seq = int(code_part)
                    new_seq = last_seq + 1
                except ValueError:
                    new_seq = 1
            else:
                new_seq = 1

            return f"{prefix}{new_seq:03d}"
        except Exception as e:
            logger.error(f"Error generating employee code: {e}")
            return f"DBSQ{uuid.uuid4().hex[:4]}"

    def add_quotation_file(self, gmail_id, file_metadata):
        """
        Inserts a file record into the quotations table.
        """
        try:
            # 1. Fetch ticket_number
            row = self.connection.execute("SELECT ticket_number FROM email_extractions WHERE gmail_id = ?", [gmail_id]).fetchone()
            
            if not row:
                return False, f"Email with ID {gmail_id} not found"
            
            ticket_number = row[0]

            #   FIX: Handle missing ticket number by generating one
            if not ticket_number:
                logger.info(f"Ticket number missing for {gmail_id}. Generating new one...")
                ticket_number = self._generate_next_ticket_number()
                self.connection.execute("UPDATE email_extractions SET ticket_number = ? WHERE gmail_id = ?", [ticket_number, gmail_id])
                # No commit needed here as we commit at the end

            # 2. Global Sequential Reference ID (DBQ-YYYY-MM-XXX)
            # Replaces ticket-dependent logic with global logic
            reference_id = self._generate_next_id("DBQ", "quotations", "reference_id")

            # 3. Insert into quotations table
            #   FIX: Use file_name, file_url
            self.connection.execute("""
                INSERT INTO quotations (id, ticket_number, reference_id, file_name, file_url, amount, uploaded_at)
                VALUES (nextval('id_sequence'), ?, ?, ?, ?, ?, ?)
            """, [ticket_number, reference_id, file_metadata.get('name'), file_metadata.get('url'), file_metadata.get('amount'), get_uae_time()])
            
            # 4. Update Status and Timestamp on Main Ticket
            self.connection.execute(
                "UPDATE email_extractions SET ticket_status = 'SENT', updated_at = ? WHERE gmail_id = ?", 
                [get_uae_time(), gmail_id]
            )
            self.connection.commit()
            return True, "Success"
        except Exception as e:
            logger.error(f"Error adding quotation file: {e}")
            return False, str(e)

    def update_file_amount(self, gmail_id, file_id, amount):
        try:
            # Update amount in quotations table directly using the ID
            # We ignore gmail_id since ID is unique, but we could verify if needed.
            # Assuming file_id is the integer ID from the database
            self.connection.execute("UPDATE quotations SET amount = ? WHERE id = ?", [amount, file_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating file amount: {e}")
            return False    
    def add_cpo_file(self, gmail_id, file_metadata):
        try:
            # 1. Fetch ticket_number
            row = self.connection.execute("SELECT ticket_number FROM email_extractions WHERE gmail_id = ?", [gmail_id]).fetchone()
            
            if not row:
                return False, f"Email with ID {gmail_id} not found"
                
            ticket_number = row[0]
            
            #   FIX: Handle missing ticket number by generating one
            if not ticket_number:
                logger.info(f"Ticket number missing for {gmail_id}. Generating new one...")
                ticket_number = self._generate_next_ticket_number()
                self.connection.execute("UPDATE email_extractions SET ticket_number = ? WHERE gmail_id = ?", [ticket_number, gmail_id])

            # 2. Global Sequential PO Reference ID (PO-YYYY-MM-XXX)
            reference_id = self._generate_next_id("PO", "cpo_orders", "reference_id")

            # 3. Insert into cpo_orders table
            #   FIX: Use file_name, file_url
            self.connection.execute("""
                INSERT INTO cpo_orders (id, ticket_number, reference_id, file_name, file_url, amount, uploaded_at)
                VALUES (nextval('id_sequence'), ?, ?, ?, ?, ?, ?)
            """, [ticket_number, reference_id, file_metadata.get('name'), file_metadata.get('url'), file_metadata.get('amount'), get_uae_time()])
            
            # 4. Update Status and Timestamp on Main Ticket
            self.connection.execute(
                "UPDATE email_extractions SET ticket_status = 'ORDER_CONFIRMED', updated_at = ? WHERE gmail_id = ?", 
                [get_uae_time(), gmail_id]
            )
            self.connection.commit()
            return True, "Success"
        except Exception as e:
            logger.error(f"Error adding CPO file: {e}")
            return False, str(e)
    def add_internal_note(self, gmail_id, note_data):
        try:
            row = self.connection.execute("SELECT internal_notes FROM email_extractions WHERE gmail_id = ?", [gmail_id]).fetchone()
            if not row: return False
            
            existing_notes = []
            if row[0]:
                try:
                    existing_notes = json.loads(row[0])
                    if not isinstance(existing_notes, list): existing_notes = []
                except:
                    existing_notes = []

            # Append new note
            existing_notes.append(note_data)
            
            self.connection.execute(
                "UPDATE email_extractions SET internal_notes = ?, updated_at = ? WHERE gmail_id = ?", 
                [json.dumps(existing_notes), get_uae_time(), gmail_id]
            )
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding note: {e}")
            return False  

    def add_activity_log(self, gmail_id, action, description, user, metadata=None):
        """
        Appends an activity log to the ticket.
        """
        try:
            row = self.connection.execute("SELECT activity_logs FROM email_extractions WHERE gmail_id = ?", [gmail_id]).fetchone()
            if not row: return False
            
            logs = []
            if row[0]:
                try:
                    logs = json.loads(row[0])
                    if not isinstance(logs, list): logs = []
                except:
                    logs = []

            new_log = {
                "id": str(uuid.uuid4()),
                "action": action,
                "description": description,
                "user": user,
                "timestamp": get_uae_time().isoformat(),
                "metadata": metadata or {}
            }
            
            logs.append(new_log)
            
            self.connection.execute(
                "UPDATE email_extractions SET activity_logs = ? WHERE gmail_id = ?", 
                [json.dumps(logs), gmail_id]
            )
            self.connection.commit()
            return new_log
        except Exception as e:
            logger.error(f"Error adding log: {e}")
            return False

    def update_ticket_details(self, gmail_id, updates):
        """
        Update generic ticket details (Subject, Sender, Date).
        updates dict: { 'subject': ..., 'sender': ..., 'received_at': ... }
        """
        try:
            # Build dynamic update query
            fields = []
            values = []
            
            if 'subject' in updates:
                fields.append("subject = ?")
                values.append(updates['subject'])
                
            if 'sender' in updates:
                fields.append("sender = ?")
                values.append(updates['sender'])
                
            if 'company_name' in updates:
                fields.append("company_name = ?")
                values.append(updates['company_name'])

            if 'received_at' in updates:
                fields.append("received_at = ?")
                values.append(updates['received_at'])
                
            if not fields:
                return False
                
            fields.append("updated_at = ?")
            values.append(get_uae_time())
            
            values.append(gmail_id) # For WHERE clause
            
            query = f"UPDATE email_extractions SET {', '.join(fields)} WHERE gmail_id = ?"
            
            self.connection.execute(query, values)
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating ticket details: {e}")
            return False 
    def get_all_users_list(self):
        """Fetch a list of all usernames for the dropdown."""
        try:
            result = self.connection.execute("SELECT username FROM users ORDER BY username ASC").fetchall()
            # Return a simple list of strings: ['admin', 'john_doe', etc.]
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting user list: {e}")
            return []

    def get_all_users_full(self):
        """Fetch a list of all users with full details (excluding password hash)."""
        try:
            result = self.connection.execute("SELECT id, username, employee_code, role,password_hash FROM users ORDER BY created_at DESC").fetchall()
            users = []
            for row in result:
                users.append({
                    "id": str(row[0]),
                    "username": row[1],
                    "employee_code": row[2],
                    "role": row[3],
                    "password_hash": row[4]
                })
            return users
        except Exception as e:
            logger.error(f"Error getting full user list: {e}")
            return []    
    def assign_ticket(self, gmail_id, username):
        try:
            self.connection.execute("""
                UPDATE email_extractions 
                SET assigned_to = ?, updated_at = ? 
                WHERE gmail_id = ?
            """, [username, get_uae_time(), gmail_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error assigning ticket: {e}")
            return False

    def delete_quotation_file(self, file_id):
        """Delete a quotation file from the database."""
        try:
            self.connection.execute("DELETE FROM quotations WHERE id = ?", [file_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting quotation file: {e}")
            return False

    def delete_cpo_file(self, file_id):
        """Delete a CPO file from the database."""
        try:
            self.connection.execute("DELETE FROM cpo_orders WHERE id = ?", [file_id])
            self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting CPO file: {e}")
            return False

    def get_employee_stats(self, time_range='all', start_date_str=None, end_date_str=None):
        """
        Aggregates performance stats for all employees.
        Returns a list of dicts with:
        - employee: {name, email, avatar_initials}
        - role
        - active_tickets
        - quotations_count
        - orders_count
        - avg_turnaround (hours)
        """
        try:
            # 1. Get all users
            users = self.get_all_users_full()
            
            # 2. Pre-fetch all tickets with necessary fields
            # We need: ticket_number, assigned_to, ticket_status, created_at, activity_logs, received_at
            tickets_query = """
                SELECT ticket_number, assigned_to, ticket_status, created_at, activity_logs, received_at 
                FROM email_extractions
            """
            all_tickets = self.connection.execute(tickets_query).fetchall()
            
            # 3. Pre-fetch counts for Quotations and Orders grouped by ticket_number
            # Quotations
            q_counts = self.connection.execute("SELECT ticket_number, COUNT(*) FROM quotations GROUP BY ticket_number").fetchall()
            quotations_map = {row[0]: row[1] for row in q_counts}
            
            # Orders
            o_counts = self.connection.execute("SELECT ticket_number, COUNT(*) FROM cpo_orders GROUP BY ticket_number").fetchall()
            orders_map = {row[0]: row[1] for row in o_counts}

            # --- Date Filtering Setup ---
            start_date = None
            end_date = None
            now = get_uae_time()
            
            # Prioritize custom range
            if start_date_str and start_date_str.lower() != 'undefined':
                start_date = parse_date_string(start_date_str)
            
            if end_date_str and end_date_str.lower() != 'undefined':
                end_date = parse_date_string(end_date_str)
                # If end date is provided, set it to end of that day
                if end_date:
                    end_date = end_date.replace(hour=23, minute=59, second=59)

            # Fallback to presets if no custom start date
            if not start_date and time_range != 'custom':
                if time_range == '24h':
                    start_date = now - timedelta(hours=24)
                elif time_range == '7d':
                    start_date = now - timedelta(days=7)
                elif time_range == '30d':
                    start_date = now - timedelta(days=30)
            
            # Ensure timezone awareness for comparison
            if start_date and start_date.tzinfo is None:
                if now.tzinfo:
                    start_date = start_date.replace(tzinfo=now.tzinfo)

            if end_date and end_date.tzinfo is None:
                 if now.tzinfo:
                    end_date = end_date.replace(tzinfo=now.tzinfo)
            # 'all' implies no start_date filter

            stats = []
            
            for user in users:
                username = user.get('username')
                
                # Filter tickets for this user AND time range
                user_tickets = []
                for t in all_tickets:
                    if t[1] != username:
                        continue
                        
                    # Time range check
                    created_at = t[3]
                    received_at = t[5]
                    
                    check_date = None
                    if received_at:
                        if isinstance(received_at, datetime):
                            check_date = received_at
                        else:
                            check_date = parse_date_string(received_at)
                    
                    if not check_date:
                        check_date = created_at
                    
                    if check_date:
                         # timezone fix for check_date
                        if now.tzinfo and check_date.tzinfo is None:
                             try:
                                check_date = check_date.replace(tzinfo=now.tzinfo)
                             except: pass

                        if start_date and check_date < start_date:
                            continue
                        if end_date and check_date > end_date:
                            continue
                            
                    user_tickets.append(t)
                
                inbox_count = 0
                sent_count = 0
                confirmed_count = 0
                completed_count = 0
                closed_count = 0

                sent_count = 0
                confirmed_count = 0
                completed_count = 0
                closed_count = 0
                active_tickets = 0 # Re-added due to UnboundLocalError
                
                turnaround_times = []
                
                for t in user_tickets:
                    ticket_number = t[0]
                    # Status normalization
                    raw_status = t[2] or 'INBOX'
                    status = raw_status.upper()
                    
                    created_at = t[3]
                    activity_logs_json = t[4]
                    
                    # --- Status Counting ---
                    if status in ['INBOX', 'OPEN']:
                        inbox_count += 1
                    elif status == 'SENT':
                        sent_count += 1
                    elif status == 'ORDER_CONFIRMED':
                        confirmed_count += 1
                    elif status == 'ORDER_COMPLETED':
                        completed_count += 1
                    elif status == 'CLOSED':
                        closed_count += 1
                    
                    # --- Active Tickets (All except CLOSED) ---
                    if status != 'CLOSED':
                        active_tickets += 1
                    
                    # --- Turnaround Time Calculation ---
                    # Only for CLOSED or ORDER_COMPLETED tickets
                    if status in ['CLOSED', 'ORDER_COMPLETED'] and created_at:
                        completion_time = None
                        
                        try:
                            logs = json.loads(activity_logs_json) if activity_logs_json else []
                            if isinstance(logs, list):
                                # Find the first log where status changed to CLOSED or ORDER_COMPLETED
                                for log in logs:
                                    if log.get('action') == 'STATUS_CHANGE' and \
                                       any(s in log.get('description', '').upper() for s in ['CLOSED', 'ORDER_COMPLETED']):
                                        # Parse timestamp
                                        try:
                                            completion_time = datetime.fromisoformat(log.get('timestamp'))
                                            break
                                        except:
                                            pass
                        except:
                            pass
                        
                        if completion_time and created_at:
                            diff = completion_time - created_at
                            hours = diff.total_seconds() / 3600
                            if hours > 0:
                                turnaround_times.append(hours)

                # Avg Turnaround
                avg_turnaround = 0.0
                if turnaround_times:
                    avg_turnaround = sum(turnaround_times) / len(turnaround_times)
                
                stats.append({
                    "id": user.get('id'),
                    "employee": {
                        "name": username,
                        "email": f"{username.lower().replace(' ', '.')}@dbest.com", 
                        "avatar": username[:2].upper() if username else "??"
                    },
                    "role": user.get('role'),
                    "active_tickets": active_tickets, # Legacy field, can keep for now
                    # New granular fields
                    "inbox_count": inbox_count,
                    "sent_count": sent_count,
                    "confirmed_count": confirmed_count,
                    "completed_count": completed_count,
                    "closed_count": closed_count,
                    
                    "avg_turnaround": round(avg_turnaround, 1)
                })
                
            return stats
        except Exception as e:
            logger.error(f"Error calculating employee stats: {e}")
            return []

    def get_employee_analytics(self, user_id, start_date_str=None, end_date_str=None):
        """
        Gathers detailed analytics for a single employee or all employees.
        Returns format compatible with frontend EmployeeAnalytics:
        kpis, funnel, workload, tickets
        """
        try:
            if user_id == 'all':
                username = None
            else:
                user = self.get_user_by_id(user_id)
                if not user: return None
                username = user['username']

            date_filter = ""
            params = []
            now = get_uae_time()

            if username:
                date_filter += " AND assigned_to = ?"
                params.append(username)

            if start_date_str and start_date_str.lower() != 'undefined':
                sd = parse_date_string(start_date_str)
                if sd:
                    date_filter += " AND received_at >= ?"
                    params.append(sd)

            if end_date_str and end_date_str.lower() != 'undefined':
                ed = parse_date_string(end_date_str)
                if ed:
                    ed = ed.replace(hour=23, minute=59, second=59)
                    date_filter += " AND received_at <= ?"
                    params.append(ed)

            cols = "gmail_id, ticket_number, company_name, sender, ticket_status, assigned_to, received_at, updated_at, activity_logs"
            query = f"SELECT {cols} FROM email_extractions WHERE 1=1 {date_filter} ORDER BY received_at DESC"
            rows = self.connection.execute(query, params).fetchall()

            status_counts = {'INBOX': 0, 'OPEN':0, 'SENT': 0, 'ORDER_CONFIRMED': 0, 'ORDER_COMPLETED': 0, 'CLOSED': 0}
            
            quote_val_total = 0.0
            order_val_total = 0.0
            line_items_total = 0
            tickets_list = []

            for r in rows:
                gmail_id, t_num, comp, sender, t_status, assigned_to, rec_at, upd_at, logs = r
                status = (t_status or 'INBOX').upper()
                if status in status_counts:
                    status_counts[status] += 1
                elif status in ['COMPLETION_REQUESTED', 'CLOSURE_REQUESTED']: # treat as OPEN or SENT appropriately
                    status_counts['OPEN'] += 1
                else:
                    status_counts['INBOX'] += 1

                # Fetch quotes and cpos
                q_files = []
                c_files = []
                if t_num:
                    q_files = self.connection.execute("SELECT amount, reference_id FROM quotations WHERE ticket_number = ?", [t_num]).fetchall()
                    c_files = self.connection.execute("SELECT amount, reference_id FROM cpo_orders WHERE ticket_number = ?", [t_num]).fetchall()

                def parse_amt(val):
                    if not val: return 0.0
                    try: return float(str(val).replace(',', '').replace('AED ', '').strip())
                    except: return 0.0

                q_amt_sum = sum(parse_amt(q[0]) for q in q_files) if q_files else 0.0
                c_amt_sum = sum(parse_amt(c[0]) for c in c_files) if c_files else 0.0

                quote_val_total += q_amt_sum
                order_val_total += c_amt_sum
                line_items_total += (len(q_files) + len(c_files))

                # Simple time extraction
                sent_time = "N/A"
                conf_time = "N/A"
                closed_time = "N/A"
                try:
                    logs_json = json.loads(logs) if logs else []
                    for log in logs_json:
                        desc = log.get('description', '').upper()
                        ts = log.get('timestamp', '')[:10]
                        if 'SENT' in desc: sent_time = ts
                        if 'CONFIRMED' in desc: conf_time = ts
                        if 'CLOSED' in desc or 'COMPLETED' in desc: closed_time = ts
                except:
                    pass

                # Ticket mapped correctly to UI
                statusColor = "blue"
                if status in ["CLOSED", "ORDER_COMPLETED"]: statusColor = "emerald"
                elif status == "ORDER_CONFIRMED": statusColor = "amber"
                elif status == "SENT": statusColor = "blue"
                else: statusColor = "yellow"

                tickets_list.append({
                    "id": t_num or gmail_id[:8],
                     "gmail_id": gmail_id,
                    "company": comp or (sender.split('<')[0] if sender and '<' in sender else sender) or 'Unknown',
                    "email": sender.split('<')[-1].replace('>','') if sender and '<' in sender else sender,
                    "status": status.replace('_', ' '),
                    "statusColor": statusColor,
                    "quoteRef": q_files[0][1] if q_files else "N/A",
                    "cpoRef": c_files[0][1] if c_files else "N/A",
                    "lines": len(q_files) + len(c_files),
                    "quoteAmt": f"AED {q_amt_sum:,.2f}",
                    "cpoAmt": f"AED {c_amt_sum:,.2f}",
                    "assigned": assigned_to,
                    "sent": sent_time,
                    "confirmed": conf_time,
                    "closed": closed_time
                })

            total_tickets = len(rows)
            sent_plus = status_counts['SENT'] + status_counts['ORDER_CONFIRMED'] + status_counts['ORDER_COMPLETED'] + status_counts['CLOSED']
            conf_plus = status_counts['ORDER_CONFIRMED'] + status_counts['ORDER_COMPLETED'] + status_counts['CLOSED']
            closed_plus = status_counts['ORDER_COMPLETED'] + status_counts['CLOSED']

            kpis = {
                "ticketsCameIn": total_tickets,
                "quotesSent": sent_plus,
                "ordersConfirmed": conf_plus,
                "closedDelivered": closed_plus,
                "lineItemsQuoted": line_items_total,
                "quoteValue": f"AED {quote_val_total:,.2f}",
                "orderValue": f"AED {order_val_total:,.2f}",
                "sentRate": f"{(sent_plus/total_tickets*100):.1f}%" if total_tickets else "0%",
                "convRate": "0%"
            }

            funnel = [
                {"label": "Tickets In", "value": total_tickets, "sub": "100%", "color": "blue"},
                {"label": "Quotes Sent", "value": sent_plus, "sub": f"{(sent_plus/total_tickets*100):.1f}%" if total_tickets else "0%", "color": "amber"},
                {"label": "Orders Confirmed", "value": conf_plus, "sub": f"{(conf_plus/total_tickets*100):.1f}%" if total_tickets else "0%", "color": "emerald"},
                {"label": "Closed / Delivered", "value": closed_plus, "sub": f"{(closed_plus/total_tickets*100):.1f}%" if total_tickets else "0%", "color": "emerald"}
            ]

            workload = {
                "lineItems": { "value": str(line_items_total), "avg": f"{(line_items_total/total_tickets if total_tickets else 0):.1f} avg/ticket" },
                "quoteValue": { "value": f"AED {quote_val_total:,.2f}", "avg": f"AED {(quote_val_total/total_tickets if total_tickets else 0):,.2f} avg/tkt" },
                "orderValue": { "value": f"AED {order_val_total:,.2f}", "avg": f"AED {(order_val_total/total_tickets if total_tickets else 0):,.2f} avg/tkt" }
            }

            return {
                "kpis": kpis,
                "funnel": funnel,
                "workload": workload,
                "tickets": tickets_list
            }
        except Exception as e:
            logger.error(f"Error getting employee analytics: {e}")
            return None
    # --- Client Management Methods ---

    def ensure_client_from_extraction(self, email_data, extraction_result):
        """
        If the ticket was created from AI extraction, add the company as a client
        if not already present (matched by email). Uses extraction_result and email_data.
        """
        try:
            # Email: prefer AI-extracted, else parse from sender header
            email = (extraction_result.get('email') or '').strip()
            if not email and email_data.get('sender'):
                sender = email_data.get('sender', '')
                if '<' in sender and '>' in sender:
                    email = sender.split('<')[-1].split('>')[0].strip()
                elif '@' in sender:
                    email = sender.strip()
            if not email or '@' not in email:
                return

            # Already exists?
            existing = self.connection.execute(
                "SELECT id FROM clients WHERE email = ?", [email]
            ).fetchone()
            if existing:
                logger.info(f"Client already exists for email {email}, skipping add")
                return

            # Build client payload from extraction
            company_name = (extraction_result.get('company_name') or '').strip()
            sender_name = (extraction_result.get('sender_name') or '').strip()
            if not sender_name and email_data.get('sender'):
                sender = email_data.get('sender', '')
                if '<' in sender:
                    sender_name = sender.split('<')[0].strip().replace('"', '').strip()
                else:
                    sender_name = sender.split('@')[0] if '@' in sender else ''
            phone = (extraction_result.get('mobile') or '').strip()

            self.add_client({
                'name': sender_name or email.split('@')[0],
                'business_name': company_name or email.split('@')[1].split('.')[0].upper() if '@' in email else '',
                'email': email,
                'phone': phone,
                'tags': []
            })
            logger.info(f"Auto-added client from extraction: {company_name or email}")
        except Exception as e:
            logger.warning(f"Could not auto-add client from extraction: {e}")

    def add_client(self, client_data):
        """
        Adds a new client to the database.
        """
        try:
            # Check if email already exists
            existing = self.connection.execute("SELECT id FROM clients WHERE email = ?", [client_data.get('email')]).fetchone()
            if existing:
                return {"success": False, "error": "Client with this email already exists"}

            query = """
                INSERT INTO clients (name, business_name, email, phone, tags)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id, name, email
            """
            tags_json = json.dumps(client_data.get('tags', []))
            
            # Using DuckDB's returning clause if supported, else fetch
            self.connection.execute(query, [
                client_data.get('name'),
                client_data.get('business_name'),
                client_data.get('email'),
                client_data.get('phone'),
                tags_json
            ])
            
            # Fetch the inserted row (assuming RETURNING works or we select it back)
            # DuckDB supports RETURNING via fetchone() on the result of execute()
            # BUT python client might differ. Let's use specific fetch.
            # actually DuckDB python API execute() returns a connection/cursor, so fetchone() works if RETURNING is valid.
            # Safe bet: Select back by email
            result = self.connection.execute("SELECT id, name, email FROM clients WHERE email = ?", [client_data.get('email')]).fetchone()
            
            return {
                "success": True, 
                "client": {
                    "id": str(result[0]),
                    "name": result[1],
                    "email": result[2]
                }
            }
        except Exception as e:
            logger.error(f"Error adding client: {e}")
            return {"success": False, "error": str(e)}

    def get_client_stats(self, time_range='all', start_date_str=None, end_date_str=None):
        """
        Aggregates stats for all clients with date filtering.
        Correlates with email_extractions via sender email.
        """
        try:
            # 1. Fetch all clients
            clients_res = self.connection.execute("SELECT id, name, business_name, email, phone, tags, created_at FROM clients ORDER BY created_at DESC").fetchall()
            
            # 2. Pre-fetch relevant tickets
            # filtering happens in memory or via SQL. Let's do SQL for efficiency if possible, 
            # but since we need to match fuzzy emails, maybe fetch all tickets and filter in python is safer for now 
            # OR better: usage of LIKE in SQL.
            
            # Let's prepare the date filter for the SQL query to optimize
            date_filter_sql = ""
            params = []
            
            start_date = None
            end_date = None
            now = get_uae_time()

            # Prioritize custom range
            if start_date_str and start_date_str.lower() != 'undefined':
                start_date = parse_date_string(start_date_str)
            
            if end_date_str and end_date_str.lower() != 'undefined':
                end_date = parse_date_string(end_date_str)
                if end_date:
                    end_date = end_date.replace(hour=23, minute=59, second=59)

            if not start_date and time_range != 'custom':
                if time_range == '24h':
                    start_date = now - timedelta(hours=24)
                elif time_range == '7d':
                    start_date = now - timedelta(days=7)
                elif time_range == '30d':
                    start_date = now - timedelta(days=30)

            # Ensure timezone awareness
            if start_date and start_date.tzinfo is None and now.tzinfo:
                start_date = start_date.replace(tzinfo=now.tzinfo)
            if end_date and end_date.tzinfo is None and now.tzinfo:
                end_date = end_date.replace(tzinfo=now.tzinfo)

            # Build SQL for date filtering
            if start_date:
                date_filter_sql += " AND received_at >= ?"
                params.append(start_date)
            if end_date:
                date_filter_sql += " AND received_at <= ?"
                params.append(end_date)
                
            clients_stats = []
            
            for row in clients_res:
                client_id, name, business, email, phone, tags_json, created_at = row
                
                try:
                    tags = json.loads(tags_json) if tags_json else []
                except:
                    tags = []

                # Count tickets by status for this client
                # Match logic: sender email contains client email OR company name matches
                
                # Note: We are now filtering the tickets by date as well
                
                ticket_query = f"""
                    SELECT ticket_status
                    FROM email_extractions 
                    WHERE (sender ILIKE ? OR (company_name IS NOT NULL AND company_name ILIKE ?)) {date_filter_sql}
                """
                
                # Copy params for this iteration
                # If business is empty, use a string that won't match to avoid false positives
                business_match = business if business and len(business) > 1 else "IMPOSSIBLE_MATCH_STRING_XYZ"
                
                current_params = [f"%{email}%", business_match] + params
                
                tickets = self.connection.execute(ticket_query, current_params).fetchall()
                
                # DEBUG LOGGING
                if len(tickets) > 0:
                     logger.info(f"DEBUG: Found {len(tickets)} tickets for client {email}/{business}. Statuses: {[t[0] for t in tickets]}")
                
                sent_count = 0
                confirmed_count = 0
                completed_count = 0
                
                for t in tickets:
                    # status is tuple (status,)
                    status = (t[0] or 'INBOX').upper()
                    
                    if status == 'SENT':
                        sent_count += 1
                    elif status == 'ORDER_CONFIRMED':
                        confirmed_count += 1
                    elif status == 'ORDER_COMPLETED':
                        completed_count += 1
                
                # Format Since date
                since_date = created_at.strftime("%b %Y") if created_at else "N/A"

                clients_stats.append({
                    "id": str(client_id),
                    "name": name,
                    "company": business,
                    "contact": {
                        "email": email,
                        "phone": phone
                    },
                    "tags": tags,
                    "stats": {
                        "sent_count": sent_count,
                        "confirmed_count": confirmed_count,
                        "completed_count": completed_count
                    },
                    "since": since_date
                })

            return clients_stats

        except Exception as e:
            logger.error(f"Error fetching client stats: {e}")
            return []

    # ------------------------------------------------------------------
    # Employee Analytics (Single Employee Deep Dive)
    # ------------------------------------------------------------------

    def _safe_float(self, val):
        """Convert a VARCHAR amount to float, stripping currency symbols and commas."""
        if val is None:
            return 0.0
        try:
            cleaned = re.sub(r'[^\d.\-]', '', str(val).replace(',', ''))
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            return 0.0

    def get_single_employee_analytics(self, username, start_date_str=None, end_date_str=None):
        """
        Returns KPIs, funnel, workload and ticket drilldown for a single employee.
        """
        try:
            # --- Date filtering ---
            start_date = parse_date_string(start_date_str) if start_date_str else None
            end_date = parse_date_string(end_date_str) if end_date_str else None
            if end_date:
                end_date = end_date.replace(hour=23, minute=59, second=59)

            now = get_uae_time()
            if start_date and start_date.tzinfo is None and now.tzinfo:
                start_date = start_date.replace(tzinfo=now.tzinfo)
            if end_date and end_date.tzinfo is None and now.tzinfo:
                end_date = end_date.replace(tzinfo=now.tzinfo)

            # --- Fetch tickets for this employee ---
            query = """
                SELECT gmail_id, ticket_number, ticket_status, ticket_priority,
                       quotation_amount, sender, company_name, subject,
                       created_at, updated_at, received_at,
                       sent_at, confirmed_at, closed_at, activity_logs
                FROM email_extractions
                WHERE assigned_to = ?
            """
            params = [username]

            if start_date:
                query += " AND COALESCE(received_at, created_at) >= ?"
                params.append(start_date)
            if end_date:
                query += " AND COALESCE(received_at, created_at) <= ?"
                params.append(end_date)

            query += " ORDER BY created_at DESC"
            rows = self.connection.execute(query, params).fetchall()

            # --- Build ticket list with joined file data ---
            tickets = []
            total = 0
            sent_count = 0
            confirmed_count = 0
            closed_count = 0
            total_quote_value = 0.0
            total_order_value = 0.0
            total_line_items = 0

            for row in rows:
                gmail_id, ticket_number, status_raw, priority, quot_amt, \
                    sender, company, subject, created_at, updated_at, received_at, \
                    sent_at_col, confirmed_at_col, closed_at_col, activity_logs_raw = row

                status = (status_raw or 'INBOX').upper()
                total += 1

                if status == 'SENT':
                    sent_count += 1
                elif status == 'ORDER_CONFIRMED':
                    confirmed_count += 1
                elif status in ('CLOSED', 'ORDER_COMPLETED'):
                    closed_count += 1

                # --- Resolve status timestamps with fallback to activity_logs ---
                def _parse_logs_for_status(logs_json, target_status):
                    """Search activity_logs for the first STATUS_CHANGE matching target."""
                    if not logs_json:
                        return None
                    try:
                        logs = json.loads(logs_json) if isinstance(logs_json, str) else logs_json
                        if not isinstance(logs, list):
                            return None
                        for log in logs:
                            if log.get('action') == 'STATUS_CHANGE':
                                desc = log.get('description', '')
                                if target_status.lower() in desc.lower():
                                    ts = log.get('timestamp')
                                    if ts:
                                        return parse_date_string(ts)
                    except Exception:
                        pass
                    return None

                # Resolve sent_at
                ticket_sent_at = sent_at_col
                if not ticket_sent_at:
                    ticket_sent_at = _parse_logs_for_status(activity_logs_raw, 'SENT')
                    if ticket_sent_at and gmail_id:
                        try:
                            self.connection.execute(
                                "UPDATE email_extractions SET sent_at = ? WHERE gmail_id = ?",
                                [ticket_sent_at, gmail_id]
                            )
                        except Exception:
                            pass

                # Resolve confirmed_at
                ticket_confirmed_at = confirmed_at_col
                if not ticket_confirmed_at:
                    ticket_confirmed_at = _parse_logs_for_status(activity_logs_raw, 'ORDER_CONFIRMED')
                    if ticket_confirmed_at and gmail_id:
                        try:
                            self.connection.execute(
                                "UPDATE email_extractions SET confirmed_at = ? WHERE gmail_id = ?",
                                [ticket_confirmed_at, gmail_id]
                            )
                        except Exception:
                            pass

                # Resolve closed_at
                ticket_closed_at = closed_at_col
                if not ticket_closed_at:
                    ticket_closed_at = _parse_logs_for_status(activity_logs_raw, 'CLOSED')
                    if not ticket_closed_at:
                        ticket_closed_at = _parse_logs_for_status(activity_logs_raw, 'ORDER_COMPLETED')
                    if ticket_closed_at and gmail_id:
                        try:
                            self.connection.execute(
                                "UPDATE email_extractions SET closed_at = ? WHERE gmail_id = ?",
                                [ticket_closed_at, gmail_id]
                            )
                        except Exception:
                            pass

                # Quotation files for this ticket
                quote_ref = '—'
                quote_amount = '—'
                quote_amount_num = 0.0
                line_items = 0
                if ticket_number:
                    try:
                        q_rows = self.connection.execute(
                            "SELECT reference_id, amount FROM quotations WHERE ticket_number = ?",
                            [ticket_number]
                        ).fetchall()
                        if q_rows:
                            quote_ref = q_rows[0][0] or '—'
                            amounts = [self._safe_float(q[1]) for q in q_rows]
                            quote_amount_num = sum(amounts)
                            if quote_amount_num > 0:
                                quote_amount = f"AED {quote_amount_num:,.0f}"
                            line_items = len(q_rows)
                    except Exception:
                        pass

                # CPO files for this ticket
                cpo_ref = '—'
                cpo_amount = '—'
                cpo_amount_num = 0.0
                if ticket_number:
                    try:
                        c_rows = self.connection.execute(
                            "SELECT reference_id, amount FROM cpo_orders WHERE ticket_number = ?",
                            [ticket_number]
                        ).fetchall()
                        if c_rows:
                            cpo_ref = c_rows[0][0] or '—'
                            amounts = [self._safe_float(c[1]) for c in c_rows]
                            cpo_amount_num = sum(amounts)
                            if cpo_amount_num > 0:
                                cpo_amount = f"AED {cpo_amount_num:,.0f}"
                    except Exception:
                        pass

                # Also consider quotation_amount on the ticket itself
                ticket_quot_amt = self._safe_float(quot_amt)
                effective_quote_val = quote_amount_num if quote_amount_num > 0 else ticket_quot_amt
                total_quote_value += effective_quote_val
                total_order_value += cpo_amount_num
                total_line_items += line_items

                # Status color mapping
                status_color_map = {
                    'INBOX': 'blue', 'OPEN': 'blue',
                    'SENT': 'yellow',
                    'ORDER_CONFIRMED': 'amber',
                    'CLOSED': 'emerald', 'ORDER_COMPLETED': 'emerald',
                }

                def fmt_date(d):
                    if d is None:
                        return '—'
                    try:
                        if isinstance(d, datetime):
                            return d.strftime('%d/%m/%Y')
                        return str(d)
                    except:
                        return '—'

                tickets.append({
                    'id': ticket_number or gmail_id,
                    'gmail_id': gmail_id,
                    'company': company or '—',
                    'email': sender or '—',
                    'status': status_raw or 'INBOX',
                    'statusColor': status_color_map.get(status, 'blue'),
                    'quoteRef': quote_ref,
                    'cpoRef': cpo_ref,
                    'lines': line_items,
                    'quoteAmt': quote_amount,
                    'cpoAmt': cpo_amount,
                    'assigned': fmt_date(created_at),
                    'sent': fmt_date(ticket_sent_at),
                    'confirmed': fmt_date(ticket_confirmed_at),
                    'closed': fmt_date(ticket_closed_at),
                })

            # Commit any backfilled timestamps
            try:
                self.connection.commit()
            except Exception:
                pass

            # --- KPIs ---
            def fmt_value(val):
                if val >= 1_000_000:
                    return f"AED {val/1_000_000:.1f}M"
                elif val >= 1_000:
                    return f"AED {val/1_000:.1f}K"
                elif val > 0:
                    return f"AED {val:,.0f}"
                return "AED 0"

            sent_rate = f"{sent_count / total * 100:.1f}%" if total > 0 else "0%"
            conv_rate = f"{confirmed_count / sent_count * 100:.1f}%" if sent_count > 0 else "0%"

            kpis = {
                'ticketsCameIn': total,
                'quotesSent': sent_count,
                'ordersConfirmed': confirmed_count,
                'closedDelivered': closed_count,
                'lineItemsQuoted': total_line_items,
                'quoteValue': fmt_value(total_quote_value),
                'orderValue': fmt_value(total_order_value),
                'sentRate': sent_rate,
                'convRate': conv_rate,
            }

            # --- Funnel ---
            funnel = [
                {'label': 'Tickets Came In', 'value': total, 'sub': None, 'color': 'emerald'},
                {
                    'label': 'Quotes Sent', 'value': sent_count,
                    'sub': f"{sent_count} of {total} ({round(sent_count/total*100) if total else 0}%)" if total else None,
                    'color': 'blue'
                },
                {
                    'label': 'Orders Confirmed', 'value': confirmed_count,
                    'sub': f"{confirmed_count} of {sent_count} ({round(confirmed_count/sent_count*100) if sent_count else 0}%)" if sent_count else None,
                    'color': 'amber'
                },
                {
                    'label': 'Closed / Delivered', 'value': closed_count,
                    'sub': f"{closed_count} of {confirmed_count} ({round(closed_count/confirmed_count*100) if confirmed_count else 0}%)" if confirmed_count else None,
                    'color': 'emerald'
                },
            ]

            # --- Workload ---
            avg_lines = round(total_line_items / sent_count, 1) if sent_count else 0
            avg_quote = fmt_value(total_quote_value / sent_count) if sent_count else "AED 0"
            avg_order = fmt_value(total_order_value / confirmed_count) if confirmed_count else "AED 0"

            workload = {
                'lineItems': {'value': str(total_line_items), 'avg': f"Avg {avg_lines} per quotation"},
                'quoteValue': {'value': fmt_value(total_quote_value), 'avg': f"Avg {avg_quote} per quotation"},
                'orderValue': {'value': fmt_value(total_order_value), 'avg': f"Avg {avg_order} per order"},
            }

            return {
                'kpis': kpis,
                'funnel': funnel,
                'workload': workload,
                'tickets': tickets,
            }

        except Exception as e:
            logger.error(f"Error in get_single_employee_analytics: {e}")
            return {'kpis': {}, 'funnel': [], 'workload': {}, 'tickets': []}

    def get_all_employees_analytics(self, start_date_str=None, end_date_str=None):
        """
        Returns aggregated team KPIs + per-employee KPI breakdown.
        Used by the Team Analytics dashboard.
        """
        try:
            # --- Date filtering ---
            start_date = parse_date_string(start_date_str) if start_date_str else None
            end_date = parse_date_string(end_date_str) if end_date_str else None
            if end_date:
                end_date = end_date.replace(hour=23, minute=59, second=59)

            now = get_uae_time()
            if start_date and start_date.tzinfo is None and now.tzinfo:
                start_date = start_date.replace(tzinfo=now.tzinfo)
            if end_date and end_date.tzinfo is None and now.tzinfo:
                end_date = end_date.replace(tzinfo=now.tzinfo)

            # --- Get all users ---
            users = self.get_all_users_full()

            # --- Fetch ALL tickets with relevant fields ---
            query = """
                SELECT gmail_id, ticket_number, ticket_status, assigned_to,
                       quotation_amount, sender, company_name, subject,
                       created_at, received_at
                FROM email_extractions
                WHERE 1=1
            """
            params = []
            if start_date:
                query += " AND COALESCE(received_at, created_at) >= ?"
                params.append(start_date)
            if end_date:
                query += " AND COALESCE(received_at, created_at) <= ?"
                params.append(end_date)

            all_tickets = self.connection.execute(query, params).fetchall()

            # --- Pre-fetch quotation and CPO amounts grouped by ticket_number ---
            q_data = self.connection.execute(
                "SELECT ticket_number, COUNT(*), COALESCE(SUM(CAST(NULLIF(REGEXP_REPLACE(amount, '[^0-9.]', '', 'g'), '') AS DOUBLE)), 0) FROM quotations GROUP BY ticket_number"
            ).fetchall()
            quotation_map = {row[0]: {'count': row[1], 'total': row[2]} for row in q_data}

            c_data = self.connection.execute(
                "SELECT ticket_number, COUNT(*), COALESCE(SUM(CAST(NULLIF(REGEXP_REPLACE(amount, '[^0-9.]', '', 'g'), '') AS DOUBLE)), 0) FROM cpo_orders GROUP BY ticket_number"
            ).fetchall()
            cpo_map = {row[0]: {'count': row[1], 'total': row[2]} for row in c_data}

            # --- Group tickets by assigned user ---
            user_tickets_map = {}
            for t in all_tickets:
                assigned = t[3] or 'Unassigned'
                if assigned not in user_tickets_map:
                    user_tickets_map[assigned] = []
                user_tickets_map[assigned].append(t)

            # --- Build per-employee analytics ---
            employees = []
            team_total = 0
            team_sent = 0
            team_confirmed = 0
            team_closed = 0
            team_quote_value = 0.0
            team_order_value = 0.0

            def fmt_value(val):
                if val >= 1_000_000:
                    return f"AED {val/1_000_000:.1f}M"
                elif val >= 1_000:
                    return f"AED {val/1_000:.1f}K"
                elif val > 0:
                    return f"AED {val:,.0f}"
                return "AED 0"

            for user in users:
                username = user.get('username')
                tickets = user_tickets_map.get(username, [])

                total = len(tickets)
                sent_count = 0
                confirmed_count = 0
                closed_count = 0
                inbox_count = 0
                emp_quote_value = 0.0
                emp_order_value = 0.0

                for t in tickets:
                    ticket_number = t[1]
                    status = (t[2] or 'INBOX').upper()

                    if status in ('INBOX', 'OPEN'):
                        inbox_count += 1
                    elif status == 'SENT':
                        sent_count += 1
                    elif status == 'ORDER_CONFIRMED':
                        confirmed_count += 1
                    elif status in ('CLOSED', 'ORDER_COMPLETED'):
                        closed_count += 1

                    # Quote value from quotations table
                    if ticket_number and ticket_number in quotation_map:
                        emp_quote_value += quotation_map[ticket_number]['total']
                    else:
                        # Fallback to quotation_amount on ticket
                        emp_quote_value += self._safe_float(t[4])

                    # CPO value
                    if ticket_number and ticket_number in cpo_map:
                        emp_order_value += cpo_map[ticket_number]['total']

                sent_rate = f"{sent_count / total * 100:.1f}%" if total > 0 else "0%"
                total_converted = confirmed_count + closed_count
                conv_rate = f"{total_converted / sent_count * 100:.1f}%" if sent_count > 0 else "0%"

                employees.append({
                    'id': user.get('id'),
                    'username': username,
                    'employee_code': user.get('employee_code', ''),
                    'role': user.get('role', ''),
                    'ticketsIn': total,
                    'inbox': inbox_count,
                    'quotesSent': sent_count,
                    'ordersConfirmed': confirmed_count,
                    'closedDelivered': closed_count,
                    'quoteValue': fmt_value(emp_quote_value),
                    'quoteValueRaw': emp_quote_value,
                    'orderValue': fmt_value(emp_order_value),
                    'orderValueRaw': emp_order_value,
                    'sentRate': sent_rate,
                    'convRate': conv_rate,
                })

                team_total += total
                team_sent += sent_count
                team_confirmed += confirmed_count
                team_closed += closed_count
                team_quote_value += emp_quote_value
                team_order_value += emp_order_value

            # --- Team KPIs ---
            team_sent_rate = f"{team_sent / team_total * 100:.1f}%" if team_total > 0 else "0%"
            team_total_converted = team_confirmed + team_closed
            team_conv_rate = f"{team_total_converted / team_sent * 100:.1f}%" if team_sent > 0 else "0%"

            team_kpis = {
                'ticketsCameIn': team_total,
                'quotesSent': team_sent,
                'ordersConfirmed': team_confirmed,
                'closedDelivered': team_closed,
                'quoteValue': fmt_value(team_quote_value),
                'orderValue': fmt_value(team_order_value),
                'sentRate': team_sent_rate,
                'convRate': team_conv_rate,
            }

            return {
                'team_kpis': team_kpis,
                'employees': employees,
            }

        except Exception as e:
            logger.error(f"Error in get_all_employees_analytics: {e}")
            return {'team_kpis': {}, 'employees': []}