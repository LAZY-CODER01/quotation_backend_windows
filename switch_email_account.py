#!/usr/bin/env python3
"""
Helper script to switch Gmail account for QuoteSnap monitoring.

This script will:
1. Clear all existing authentication tokens
2. Clear the session (if using frontend)
3. Provide instructions for re-authentication

Usage:
    python switch_email_account.py
"""

import os
import shutil
from pathlib import Path

def clear_tokens():
    """Clear all authentication tokens."""
    token_dir = Path('tokens')
    
    if not token_dir.exists():
        print("✅ No tokens directory found. You're ready to authenticate with a new account.")
        return 0
    
    # Count token files
    token_files = list(token_dir.glob('token_*.json'))
    
    if not token_files:
        print("✅ No token files found. You're ready to authenticate with a new account.")
        return 0
    
    print(f"📧 Found {len(token_files)} token file(s) to delete:")
    for token_file in token_files:
        print(f"   - {token_file.name}")
    
    # Ask for confirmation
    response = input("\n⚠️  This will log out all users. Continue? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        print("❌ Cancelled. No changes made.")
        return 0
    
    # Delete all token files
    deleted_count = 0
    for token_file in token_files:
        try:
            token_file.unlink()
            deleted_count += 1
            print(f"   ✅ Deleted: {token_file.name}")
        except Exception as e:
            print(f"   ❌ Error deleting {token_file.name}: {e}")
    
    print(f"\n✅ Successfully deleted {deleted_count} token file(s).")
    print("\n📝 Next steps:")
    print("   1. Restart your backend server (if running)")
    print("   2. Go through the login flow again")
    print("   3. When prompted, select your NEW Gmail account")
    print("   4. The system will now monitor the new account's emails")
    
    return deleted_count

def main():
    """Main function."""
    print("=" * 60)
    print("🔄 QuoteSnap - Switch Gmail Account")
    print("=" * 60)
    print()
    
    try:
        count = clear_tokens()
        print()
        print("=" * 60)
        if count > 0:
            print("✅ Account switch ready! Follow the steps above.")
        else:
            print("✅ Ready for new account authentication.")
        print("=" * 60)
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == '__main__':
    main()

