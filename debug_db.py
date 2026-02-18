from app.services.duckdb_service import DuckDBService
import json

def debug():
    db = DuckDBService()
    if not db.connect():
        print("Failed to connect")
        return

    print("--- CLIENTS (Top 5) ---")
    try:
        clients = db.connection.execute("SELECT name, business_name, email FROM clients LIMIT 5").fetchall()
        for c in clients:
            print(c)
    except Exception as e:
        print(f"Error fetching clients: {e}")
        
    print("\n--- EMAILS (Top 5) ---")
    try:
        emails = db.connection.execute("SELECT id, sender, company_name, ticket_status FROM email_extractions LIMIT 5").fetchall()
        for e in emails:
            print(e)
    except Exception as e:
        print(f"Error fetching emails: {e}")

    # Test match
    if 'clients' in locals() and len(clients) > 0:
        client = clients[0]
        name, business, email = client
        print(f"\n--- TESTING QUERY FOR Client: {name}, Co: {business}, Email: {email} ---")
        
        # Combined logic check
        query = """
            SELECT ticket_status, sender, company_name 
            FROM email_extractions 
            WHERE sender ILIKE ? OR (company_name IS NOT NULL AND company_name ILIKE ?)
        """
        params = [f"%{email}%", business if business else "IMPOSSIBLE_STRING"]
        
        tickets = db.connection.execute(query, params).fetchall()
        print(f"Found {len(tickets)} tickets.")
        for t in tickets:
             print(f" - {t}")

    db.disconnect()

if __name__ == "__main__":
    debug()
