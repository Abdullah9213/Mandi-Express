import pandas as pd
import os
from datetime import datetime

FILE_PATH = r"../data/processed/processed_combined.csv"
TODAY = datetime.now().strftime("%Y-%m-%d")

def verify_today_data():
    print(f"🔍 Checking {FILE_PATH} for date: {TODAY}...")
    
    if not os.path.exists(FILE_PATH):
        print("❌ File not found.")
        return

    # Read the last 2000 rows only (Fast & Safe)
    try:
        with open(FILE_PATH, 'rb') as f:
            f.seek(0, 2)  # Seek to end
            file_size = f.tell()
            f.seek(max(file_size - 100000, 0), 0)  # Read last 100KB
            lines = f.readlines()
        
        # Decode and parse last lines
        last_rows = [line.decode('utf-8').strip().split(',') for line in lines[1:]]
        
        # Check for today's date in the first column (Date)
        found_count = 0
        for row in last_rows:
            if len(row) > 0 and TODAY in row[0]:
                found_count += 1
                
        if found_count > 0:
            print(f"✅ SUCCESS: Found {found_count} records for {TODAY} in the file.")
            print("   Sample record:", [r for r in last_rows if TODAY in r[0]][0])
        else:
            print(f"❌ FAILURE: No records found for {TODAY}.")
            print("   Last recorded date:", last_rows[-1][0] if last_rows else "Unknown")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_today_data()
