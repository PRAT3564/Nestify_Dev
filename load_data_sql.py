import os
import traceback
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError
import time
from dotenv import load_dotenv
import mysql.connector

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------------------------
# MySQL Engine
# ---------------------------
engine = create_engine(
    connection_string,
    poolclass=NullPool,
    pool_pre_ping=True,  # Verify connections before using
    echo=False  # Set to True for debugging SQL
)

# ---------------------------
# Data Cleaning Function
# ---------------------------
def clean_dataframe_for_insertion(df):
    """
    Final data cleaning before database insertion
    """
    df_clean = df.copy()
    
    # Truncate location to 255 characters
    if 'location' in df_clean.columns:
        df_clean['location'] = df_clean['location'].apply(
            lambda x: str(x)[:255] if pd.notna(x) and not pd.isnull(x) else None
        )
    
    # List of numeric columns that should be floats/ints
    numeric_columns = ['bedrooms', 'balconies', 'floor_num', 'total_floor', 
                       'area_sqft', 'price_total', 'price_per_sqft', 'landmark_count']
    
    # Convert numeric columns, coercing errors to NaN
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Convert integer columns (replace NaN with None for NULL in DB)
    int_columns = ['bedrooms', 'balconies', 'floor_num', 'total_floor', 'landmark_count']
    for col in int_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: int(x) if pd.notna(x) and not pd.isnull(x) else None
            )
    
    # Text columns - ensure they're strings, replace NaN with None
    text_columns = ['property_id', 'prop_heading', 'description', 'property_type', 
                    'transact_type', 'ownership_type', 'preference', 'city',
                    'society_name', 'building_name', 'project_name', 'area_raw', 
                    'price_raw', 'furnish', 'facing', 'property_age', 'amenities',
                    'features', 'secondary_tags', 'landmark_details', 'map_details',
                    'listing_source']
    
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: str(x) if pd.notna(x) and not pd.isnull(x) else None
            )
    
    # Replace any remaining NaN/NaT with None
    df_clean = df_clean.replace({np.nan: None, pd.NA: None})
    
    return df_clean
# ---------------------------
# Batch Insert Function
# ---------------------------
def insert_in_batches(df, table_name, batch_size=100, max_retries=3):
    """
    Insert DataFrame into database in batches with retry logic
    """
    total_rows = len(df)
    successful_rows = 0
    failed_batches = []
    
    print(f"Starting insertion of {total_rows} rows in batches of {batch_size}")
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df.iloc[start_idx:end_idx]
        
        batch_num = start_idx // batch_size + 1
        total_batches = (total_rows + batch_size - 1) // batch_size
        
        print(f"Processing batch {batch_num}/{total_batches} (rows {start_idx}-{end_idx})")
        
        # Convert batch to list of dictionaries for insertion
        records = batch.to_dict('records')
        
        # Try inserting with retries
        for attempt in range(max_retries):
            try:
                with engine.connect() as conn:
                    # Start transaction
                    trans = conn.begin()
                    
                    for record in records:
                        # Build INSERT statement
                        columns = ', '.join(record.keys())
                        placeholders = ', '.join([f':{col}' for col in record.keys()])
                        
                        stmt = text(f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                        """)
                        
                        conn.execute(stmt, record)
                    
                    # Commit transaction
                    trans.commit()
                    
                successful_rows += len(batch)
                print(f"  ✅ Batch {batch_num} inserted successfully")
                break  # Success, exit retry loop
                
            except SQLAlchemyError as e:
                print(f"  ❌ Attempt {attempt + 1} failed for batch {batch_num}: {str(e)[:100]}...")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"  Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"  ❌ Batch {batch_num} failed after {max_retries} attempts")
                    failed_batches.append({
                        'batch_num': batch_num,
                        'start_idx': start_idx,
                        'end_idx': end_idx,
                        'error': str(e)
                    })
                    
                    # Save failed batch to CSV for manual inspection
                    failed_file = f"failed_batch_{batch_num}.csv"
                    batch.to_csv(failed_file, index=False)
                    print(f"  Saved failed batch to {failed_file}")
    
    # Summary
    print("\n" + "="*50)
    print("INSERTION SUMMARY")
    print("="*50)
    print(f"Total rows: {total_rows}")
    print(f"Successfully inserted: {successful_rows}")
    print(f"Failed batches: {len(failed_batches)}")
    
    if failed_batches:
        print("\nFailed batches details:")
        for fb in failed_batches:
            print(f"  Batch {fb['batch_num']} (rows {fb['start_idx']}-{fb['end_idx']}): {fb['error']}")
    
    return successful_rows, failed_batches

# ---------------------------
# Main Execution
# ---------------------------
def main():
    try:
        # Load your unified dataframe
        # Replace this with however you load your final_df
        print("Loading unified dataset...")
        final_df = pd.read_csv("clean/unified_properties.csv")
        print(f"Loaded {len(final_df)} rows")
        
        # Clean the dataframe
        print("Cleaning data for insertion...")
        clean_df = clean_dataframe_for_insertion(final_df)
        
        # Insert in batches
        print("Starting database insertion...")
        successful, failed = insert_in_batches(
            df=clean_df,
            table_name='unified_properties',
            batch_size=100,  # Adjust based on your data size
            max_retries=3
        )
        
        if successful == len(clean_df):
            print("\n✅ ALL DATA INSERTED SUCCESSFULLY!")
        else:
            print(f"\n⚠️ Partial success: {successful}/{len(clean_df)} rows inserted")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        traceback.print_exc()
        
    finally:
        # Always dispose the engine
        if 'engine' in locals():
            engine.dispose()
            print("Database connections closed")

# ---------------------------
# Run the script
# ---------------------------
if __name__ == "__main__":
    main()