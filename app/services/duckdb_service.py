import re
import duckdb
import os
import logging
import json
from datetime import datetime
import duckdb
import os
import logging
import json
from datetime import datetime
import uuid
from config.settings import Config
from app.utils.helpers import get_uae_time, parse_date_string

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
        """Create necessary tables (Emails + Auth Tokens + Users + Files)."""
        try:
            # -----------------------------------------------------------
            # 🚨 HARD RESET FOR SCHEMA MIGRATION
            # This drops the old tables to ensure they are recreated with the correct UNIQUE constraints.
            # Since you deleted your data, this is safe and necessary.
            # -----------------------------------------------------------
            # try:
            #     # 1. Drop Child Tables (to remove FK dependencies)
            #     self.connection.execute("DROP TABLE IF EXISTS quotations")
            #     self.connection.execute("DROP TABLE IF EXISTS cpo_orders")
                
            #     # 2. Check if email_extractions exists and verify constraint
            #     # If we are in a broken state, we drop the main table to rebuild it fresh.
            #     self.connection.execute("DROP TABLE IF EXISTS email_extractions")
            #     logger.info("♻️  Dropped old tables to force schema update.")
            # except Exception as e:
            #     logger.warning(f"⚠️ Reset warning: {e}")
            # # -----------------------------------------------------------


            # 1. Base Sequences
            self.connection.execute("CREATE SEQUENCE IF NOT EXISTS id_sequence START 1;")

            # 2. Main Emails Table 
            # (Now this will definitely run because we dropped the old one above)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS email_extractions (
                    id INTEGER DEFAULT nextval('id_sequence'),
                    gmail_id VARCHAR PRIMARY KEY,
                    ticket_number VARCHAR UNIQUE, -- 👈 This is the critical fix
                    ticket_status VARCHAR DEFAULT 'INBOX',
                    ticket_priority VARCHAR DEFAULT 'NORMAL',
                    quotation_files JSON DEFAULT '[]',
                    cpo_files JSON DEFAULT '[]',
                    activity_logs JSON DEFAULT '[]',
                    quotation_amount VARCHAR,
                    sender VARCHAR,
                    company_name VARCHAR, -- 👈 Added company_name
                    received_at TIMESTAMP,
                    subject VARCHAR,
                    body_text TEXT,
                    extraction_result JSON,
                    extraction_status VARCHAR,
                    updated_at TIMESTAMP,
                    assigned_to VARCHAR,
                    internal_notes JSON DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 3. Create Normalized Tables for Files
            # These will now succeed because ticket_number is guaranteed to be UNIQUE
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

            # 4. Auth Tables (Users, Tokens)
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
            
            # Helper to ensure columns exist (in case we didn't drop tables above)
            
            self._ensure_column_exists("users", "employee_code", "VARCHAR")
            self._ensure_column_exists("email_extractions", "company_name", "VARCHAR")

            logger.info("✅ Database tables initialized (Emails, Tokens, Users, Quotations, CPO)")
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
            # 🤖 AUTO-ASSIGNMENT LOGIC
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
                        logger.info(f"🤖 Auto-assigned ticket {ticket_number} to {assigned_to_user} (Found EMP code: {full_code_str})")

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
        """Update workflow status (OPEN -> CLOSED)."""
        try:
            self.connection.execute("""
                UPDATE email_extractions 
                SET ticket_status = ?, updated_at = ?
                WHERE ticket_number = ?
            """, [new_status, get_uae_time(), ticket_number])
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
            # ✅ Added quotation_files and quotation_amount to the list
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
                # ✅ Explicit Date Range (New Feature)
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
            
            # ✅ RBAC: Users only see assigned tickets (Unassigned tickets hidden)
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
                        # ✅ FIX: Fetch file_name/file_url and alias to name/url for frontend compatibility
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
                    # ✅ FIX: Log specific error but don't crash
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
            # ✅ FIX: Added quotation_files and quotation_amount here too
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
            # ✅ Fetch employee_code as well
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

            # ✅ FIX: Handle missing ticket number by generating one
            if not ticket_number:
                logger.info(f"Ticket number missing for {gmail_id}. Generating new one...")
                ticket_number = self._generate_next_ticket_number()
                self.connection.execute("UPDATE email_extractions SET ticket_number = ? WHERE gmail_id = ?", [ticket_number, gmail_id])
                # No commit needed here as we commit at the end

            # 2. Global Sequential Reference ID (DBQ-YYYY-MM-XXX)
            # Replaces ticket-dependent logic with global logic
            reference_id = self._generate_next_id("DBQ", "quotations", "reference_id")

            # 3. Insert into quotations table
            # ✅ FIX: Use file_name, file_url
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
            
            # ✅ FIX: Handle missing ticket number by generating one
            if not ticket_number:
                logger.info(f"Ticket number missing for {gmail_id}. Generating new one...")
                ticket_number = self._generate_next_ticket_number()
                self.connection.execute("UPDATE email_extractions SET ticket_number = ? WHERE gmail_id = ?", [ticket_number, gmail_id])

            # 2. Global Sequential PO Reference ID (PO-YYYY-MM-XXX)
            reference_id = self._generate_next_id("PO", "cpo_orders", "reference_id")

            # 3. Insert into cpo_orders table
            # ✅ FIX: Use file_name, file_url
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

    def get_employee_stats(self, time_range='all'):
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
            now = get_uae_time()
            
            if time_range == '24h':
                start_date = now - timedelta(hours=24)
            elif time_range == '7d':
                start_date = now - timedelta(days=7)
            elif time_range == '30d':
                start_date = now - timedelta(days=30)
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
                    
                    check_date = created_at
                    if received_at:
                        if isinstance(received_at, datetime):
                            check_date = received_at
                        else:
                            parsed_received = parse_date_string(received_at)
                            if parsed_received:
                                check_date = parsed_received
                    
                    if start_date and check_date:
                        # Ensure offset-aware/naive compatibility if needed
                         # (Assuming start_date and check_date are compatible or comparable)
                        try:
                            if check_date < start_date:
                                continue
                        except TypeError:
                            # Fallback if mixed offsets: convert both to timestamp or ignore tz
                            # Simplest: make check_date naive if start_date is naive
                            if start_date.tzinfo is None and check_date.tzinfo is not None:
                                check_date = check_date.replace(tzinfo=None)
                            elif start_date.tzinfo is not None and check_date.tzinfo is None:
                                check_date = check_date.replace(tzinfo=start_date.tzinfo)
                            
                            if check_date < start_date:
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

    def get_client_stats(self):
        """
        Aggregates stats for all clients.
        Correlates with email_extractions via sender email (or domain matching in future).
        """
        try:
            # 1. Fetch all clients
            clients_res = self.connection.execute("SELECT id, name, business_name, email, phone, tags, created_at FROM clients ORDER BY created_at DESC").fetchall()
            
            clients_stats = []
            
            for row in clients_res:
                client_id, name, business, email, phone, tags_json, created_at = row
                
                try:
                    tags = json.loads(tags_json) if tags_json else []
                except:
                    tags = []

                # 2. Get stats from email_extractions matching this client's email
                # We assume sender email matches client email for simplicity now
                
                stats_query = """
                    SELECT 
                        COUNT(*),
                        COUNT(CASE WHEN quotation_files != '[]' THEN 1 END),
                        COUNT(CASE WHEN cpo_files != '[]' THEN 1 END),
                        MAX(created_at)
                    FROM email_extractions 
                    WHERE sender ILIKE ?
                """
                # Use %email% to match "Name <email>" format
                email_pattern = f"%{email}%"
                
                stats = self.connection.execute(stats_query, [email_pattern]).fetchone()
                
                total_tickets = stats[0] or 0
                quotations = stats[1] or 0
                orders = stats[2] or 0
                last_active = stats[3]
                
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
                        "quotations": quotations,
                        "orders": orders
                    },
                    "since": since_date,
                    "last_active": last_active.isoformat() if last_active else None
                })

            return clients_stats

        except Exception as e:
            logger.error(f"Error fetching client stats: {e}")
            return []