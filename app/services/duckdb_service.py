"""
DuckDB service for SnapQuote application.

This module handles all DuckDB operations for storing
AI extraction results and managing email processing data.
"""

import duckdb
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class DuckDBService:
    """
    Service class for handling DuckDB operations.
    """
    
    def __init__(self, db_path: str = "database/snapquote.duckdb"):
        """
        Initialize DuckDB service with database path.
        
        Args:
            db_path (str): Path to DuckDB database file
        """
        self.db_path = db_path
        self.connection = None
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        logger.info(f"DuckDB service initialized with path: {db_path}")
    
    def connect(self) -> bool:
        """
        Establish connection to DuckDB database.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.connection = duckdb.connect(self.db_path)
            logger.info("DuckDB connection established")
            return True
        except Exception as e:
            logger.error(f"DuckDB connection failed: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Close database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed")
    
    def create_table(self) -> bool:
        """
        Create the email_extractions table.
        
        Returns:
            bool: True if table created successfully
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return False
        
        try:
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS email_extractions (
                    id INTEGER PRIMARY KEY,
                    gmail_id VARCHAR UNIQUE NOT NULL,
                    subject VARCHAR,
                    sender VARCHAR,
                    received_at TIMESTAMP,
                    extraction_status VARCHAR, -- 'VALID' or 'IRRELEVANT'
                    extraction_result JSON,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create index for faster lookups
            self.connection.execute('''
                CREATE INDEX IF NOT EXISTS idx_gmail_id ON email_extractions (gmail_id)
            ''')
            
            logger.info("DuckDB table created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating DuckDB table: {str(e)}")
            return False
    
    def insert_extraction(self, email_data: Dict, extraction_result: Dict) -> Optional[int]:
        """
        Insert a new email extraction record.
        
        Args:
            email_data (Dict): Email metadata
            extraction_result (Dict): AI extraction result
            
        Returns:
            Optional[int]: Record ID if inserted successfully, None otherwise
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return None
        
        try:
            # Check if record with this gmail_id already exists
            existing = self.connection.execute(
                'SELECT id FROM email_extractions WHERE gmail_id = ?', 
                (email_data.get('gmail_id'),)
            ).fetchone()
            
            if existing:
                logger.info(f"Email with gmail_id {email_data.get('gmail_id')} already exists, updating instead")
                success = self.update_extraction(email_data.get('gmail_id'), extraction_result)
                return existing[0] if success else None
            
            # Determine extraction status
            status = "IRRELEVANT" if extraction_result.get("status") == "NOT_VALID" else "VALID"
            
            # Generate a simple ID based on current max ID + 1
            max_id_result = self.connection.execute('SELECT COALESCE(MAX(id), 0) FROM email_extractions').fetchone()
            next_id = (max_id_result[0] if max_id_result else 0) + 1
            
            self.connection.execute('''
                INSERT INTO email_extractions (
                    id, gmail_id, subject, sender, received_at, extraction_status, extraction_result
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                next_id,
                email_data.get('gmail_id'),
                email_data.get('subject'),
                email_data.get('sender'),
                email_data.get('received_at'),
                status,
                json.dumps(extraction_result)
            ))
            
            logger.info(f"Email extraction inserted with ID: {next_id}")
            return next_id
            
        except Exception as e:
            logger.error(f"Error inserting email extraction: {str(e)}")
            return None
    
    def update_extraction(self, gmail_id: str, extraction_result: Dict) -> bool:
        """
        Update an existing email extraction record.
        
        Args:
            gmail_id (str): Gmail message ID
            extraction_result (Dict): New AI extraction result
            
        Returns:
            bool: True if updated successfully
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return False
        
        try:
            status = "IRRELEVANT" if extraction_result.get("status") == "NOT_VALID" else "VALID"
            
            self.connection.execute('''
                UPDATE email_extractions 
                SET extraction_status = ?, extraction_result = ?, updated_at = CURRENT_TIMESTAMP
                WHERE gmail_id = ?
            ''', (status, json.dumps(extraction_result), gmail_id))
            
            logger.info(f"Email extraction updated for gmail_id: {gmail_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating email extraction: {str(e)}")
            return False
    
    def get_extraction(self, gmail_id: str) -> Optional[Dict]:
        """
        Get extraction record by Gmail ID.
        
        Args:
            gmail_id (str): Gmail message ID
            
        Returns:
            Optional[Dict]: Extraction record if found, None otherwise
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return None
        
        try:
            result = self.connection.execute('''
                SELECT id, gmail_id, subject, sender, received_at, 
                       extraction_status, extraction_result, processed_at, updated_at
                FROM email_extractions 
                WHERE gmail_id = ?
            ''', (gmail_id,)).fetchone()
            
            if result:
                return {
                    'id': result[0],
                    'gmail_id': result[1],
                    'subject': result[2],
                    'sender': result[3],
                    'received_at': result[4],
                    'extraction_status': result[5],
                    'extraction_result': json.loads(result[6]),
                    'processed_at': result[7],
                    'updated_at': result[8]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving email extraction: {str(e)}")
            return None
    
    def get_all_extractions(self, limit: int = 100) -> List[Dict]:
        """
        Get all extraction records with pagination.
        
        Args:
            limit (int): Maximum number of records to return
            
        Returns:
            List[Dict]: List of extraction records
        """
        if not self.connection:
            logger.error("No DuckDB connection available")
            return []
        
        try:
            results = self.connection.execute('''
                SELECT id, gmail_id, subject, sender, received_at, 
                       extraction_status, extraction_result, processed_at, updated_at
                FROM email_extractions 
                ORDER BY processed_at DESC
                LIMIT ?
            ''', (limit,)).fetchall()
            
            extractions = []
            for result in results:
                extractions.append({
                    'id': result[0],
                    'gmail_id': result[1],
                    'subject': result[2],
                    'sender': result[3],
                    'received_at': result[4],
                    'extraction_status': result[5],
                    'extraction_result': json.loads(result[6]),
                    'processed_at': result[7],
                    'updated_at': result[8]
                })
            
            return extractions
            
        except Exception as e:
            logger.error(f"Error retrieving email extractions: {str(e)}")
            return []