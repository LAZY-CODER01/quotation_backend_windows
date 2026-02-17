import sys
import os
from app.services.duckdb_service import DuckDBService

# Ensure we can import app
sys.path.append(os.getcwd())

def inspect_tickets():
    try:
        db = DuckDBService()
        if db.connect():
            print("Connected to DB")
            # Get tickets for 'Avi' or any
            tickets = db.connection.execute("""
                SELECT ticket_number, created_at, assigned_to, received_at
                FROM email_extractions 
                LIMIT 1
            """).fetchall()
            
            print(f"Found {len(tickets)} tickets:")
            for t in tickets:
                print(f"Ticket: {t[0]}, Created: {t[1]}, Received: {t[3]}")
                
            db.disconnect()
        else:
            print("Failed to connect")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_tickets()
