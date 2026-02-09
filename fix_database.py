#!/usr/bin/env python3
"""
Database Migration Script
Fixes the 'no such table: user_stats' error by adding missing columns
"""

import sqlite3
import os

DB_PATH = "resume1.db"  # Change this to your database path if different

def fix_database():
    """Add missing columns to user_stats table"""
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database file '{DB_PATH}' not found!")
        print("   Please update DB_PATH in this script to match your database file location.")
        return
    
    print(f"🔧 Fixing database: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Check if user_stats table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_stats'")
        table_exists = c.fetchone() is not None
        
        if not table_exists:
            print("📋 Creating user_stats table (table didn't exist)...")
            c.execute('''CREATE TABLE user_stats (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                total_runs INTEGER DEFAULT 0,
                total_balls_faced INTEGER DEFAULT 0,
                total_wickets INTEGER DEFAULT 0,
                total_balls_bowled INTEGER DEFAULT 0,
                total_runs_conceded INTEGER DEFAULT 0,
                matches_played INTEGER DEFAULT 0,
                matches_won INTEGER DEFAULT 0,
                highest_score INTEGER DEFAULT 0,
                total_hundreds INTEGER DEFAULT 0,
                total_fifties INTEGER DEFAULT 0,
                five_wicket_hauls INTEGER DEFAULT 0,
                player_of_match INTEGER DEFAULT 0
            )''')
            print("✅ user_stats table created successfully!")
        else:
            print("📋 Table user_stats exists. Checking for missing columns...")
            
            # Get existing columns
            c.execute("PRAGMA table_info(user_stats)")
            existing_columns = [row[1] for row in c.fetchall()]
            print(f"   Current columns: {', '.join(existing_columns)}")
            
            # Add missing columns
            columns_to_add = {
                'five_wicket_hauls': 'INTEGER DEFAULT 0',
                'player_of_match': 'INTEGER DEFAULT 0'
            }
            
            for column_name, column_type in columns_to_add.items():
                if column_name not in existing_columns:
                    try:
                        c.execute(f"ALTER TABLE user_stats ADD COLUMN {column_name} {column_type}")
                        print(f"   ✅ Added column: {column_name}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" in str(e):
                            print(f"   ℹ️  Column {column_name} already exists")
                        else:
                            raise
                else:
                    print(f"   ℹ️  Column {column_name} already exists")
        
        conn.commit()
        
        # Verify the fix
        c.execute("PRAGMA table_info(user_stats)")
        columns = [row[1] for row in c.fetchall()]
        
        print("\n✅ Database fixed successfully!")
        print(f"\nFinal user_stats columns ({len(columns)}):")
        for col in columns:
            print(f"   - {col}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error fixing database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_database()