import pandas as pd
from datetime import datetime
import re

# URL for Daily Market Changes
URL = "http://www.amis.pk/daily%20market%20changes.aspx"

def clean_crop_name(name):
    """
    Removes Urdu characters and extra parentheses.
    Input: "Lady Finger/Okra (بھنڈی توری)"
    Output: "Lady Finger/Okra"
    """
    if not isinstance(name, str):
        return str(name)
    
    # Remove anything that is NOT ASCII (a-z, A-Z, 0-9, punctuation)
    # This strips out Urdu/Arabic script
    clean_name = re.sub(r'[^\x00-\x7F]+', '', name)
    
    # Cleanup extra brackets "()" left behind, including those with spaces inside like "( )"
    clean_name = re.sub(r'\(\s*\)', '', clean_name).strip()
    
    return clean_name

def test_scrape_daily_changes():
    print(f"🌍 Connecting to: {URL}")
    
    try:
        dfs = pd.read_html(URL)
        print(f"✅ Found {len(dfs)} tables.")
        
        target_df = None
        for i, df in enumerate(dfs):
            df.columns = [str(c).strip() for c in df.columns]
            if 'CityName' in df.columns and 'CropName' in df.columns:
                target_df = df
                break
        
        if target_df is None:
            if len(dfs) > 0:
                target_df = max(dfs, key=len)
                if target_df.shape[1] >= 3:
                    target_df.columns = ['CityName', 'CropName', "Today's FQP/Average Price"] + list(target_df.columns[3:])
            else:
                print("❌ No tables found.")
                return

        if 'CityName' in target_df.columns:
            target_df.rename(columns={'CityName': 'City', 'CropName': 'Crop', "Today's FQP/Average Price": 'Price'}, inplace=True)

        # 1. Clean Price
        target_df['Price'] = target_df['Price'].astype(str).str.replace(',', '', regex=False)
        target_df['Price'] = pd.to_numeric(target_df['Price'], errors='coerce')
        target_df.dropna(subset=['Price'], inplace=True)

        # 2. Clean Crop Name (Remove Urdu)
        target_df['Crop'] = target_df['Crop'].apply(clean_crop_name)

        print(f"📊 Extracted {len(target_df)} valid rows.")
        
        print("\n--- 🥬 SAMPLE DATA (Clean English) ---")
        print(target_df[['City', 'Crop', 'Price']].head(10))
        
        # Check Lahore
        lahore_data = target_df[target_df['City'] == 'Lahore']
        if not lahore_data.empty:
            print("\n✅ Found updates for Lahore:")
            print(lahore_data[['Crop', 'Price']])

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_scrape_daily_changes()
