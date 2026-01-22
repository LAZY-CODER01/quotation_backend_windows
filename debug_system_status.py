
import os
import json
import logging
from dotenv import load_dotenv
from app.services.gmail_service import GmailService
from app.services.duckdb_service import DuckDBService
from config.settings import Config

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_system():
    print("🕵️‍♂️ Starting Deep System Debug...")

    # 1. Check DB Connection and Token
    print("\n1️⃣ Checking Database & Token...")
    db = DuckDBService()
    if not db.connect():
        print("   ❌ DB Connection Failed")
        return
    
    token_json = db.get_company_token()
    if not token_json:
        print("   ❌ No Company Token found in DB.")
        return # Cannot proceed
    else:
        print("   ✅ Company Token exists.")

    db.disconnect()

    # 2. Check Gmail Service
    print("\n2️⃣ Checking Gmail Status for DEMO Account...")
    gmail = GmailService(credentials_path=Config.GMAIL_CREDENTIALS_FILE)
    
    try:
        gmail.authenticate_from_info(json.loads(token_json))
        print(f"   ✅ Authenticated as: {gmail.service.users().getProfile(userId='me').execute().get('emailAddress')}")
        
        # 3. Check Pending Emails (Standard Logic)
        print("\n3️⃣ Checking Standard 'Pending' Logic...")
        emails = gmail._check_for_new_emails()
        print(f"   🔎 _check_for_new_emails found {len(emails)} pending emails.")
        
        # ALSO Check Reprocess Logic
        print("\n3️⃣.5️⃣ Checking 'Reprocess' Logic...")
        reprocess_emails = gmail._check_for_reprocess_emails()
        print(f"   🔎 _check_for_reprocess_emails found {len(reprocess_emails)} pending emails.")
        emails.extend(reprocess_emails)
        
        for e in emails:
            print(f"      - ID: {e['gmail_id']} | Subj: {e['subject'][:30]}... | Reprocess: {e.get('is_reprocess', False)}")
            
        if emails:
            print("\n4️⃣ Attempting to PROCESS the first email MANUALLY to test EXTRACTION...")
            target_email = emails[0]
            db_proc = DuckDBService()
            db_proc.connect()
            try:
                print(f"   ▶️ Processing {target_email['gmail_id']}...")
                gmail._process_single_email(target_email, db_proc)
                print("   ✅ _process_single_email completed.")
            except Exception as pe:
                print(f"   ❌ Error processing email: {pe}")
                import traceback
                traceback.print_exc()
            finally:
                db_proc.disconnect()

        # 4. Check INBOX (No Filters)
        print("\n4️⃣ Investigating RAW Inbox Content (No Filters)...")
        results = gmail.service.users().messages().list(userId='me', q='in:inbox', maxResults=5).execute()
        messages = results.get('messages', [])
        
        print(f"   🔎 'in:inbox' found {len(messages)} messages.")
        
        for m in messages:
            msg = gmail.service.users().messages().get(userId='me', id=m['id'], format='minimal').execute()
            labels = msg.get('labelIds', [])
            snippet = msg.get('snippet', '')[:50]
            print(f"      - ID: {m['id']}")
            print(f"        Labels: {labels}")
            print(f"        Snippet: {snippet}...")
            
    except Exception as e:
        print(f"   ❌ Gmail Error: {e}")

if __name__ == "__main__":
    debug_system()
