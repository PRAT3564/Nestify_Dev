import pandas as pd
import uuid
import re
import os

# ---------------------------
# Normalizers
# ---------------------------
def normalize_price(price):
    if pd.isna(price):
        return None
    
    price_str = str(price).lower().strip()
    
    # Handle empty strings, 'None', 'nan', etc.
    if not price_str or price_str in ['none', 'nan', 'null', '']:
        return None
    
    # Handle "Price on Request", "Price on Request", etc.
    if 'price on request' in price_str or 'price on req' in price_str:
        return None
    
    # Handle empty list/array representation
    if price_str in ['[]', '{}', '()']:
        return None
    
    # Remove ALL commas (including Indian format like 2,63,00,000)
    price_str = price_str.replace(",", "")
    
    try:
        # Check for Lakhs (l, lac, lakh)
        if "lac" in price_str or " lakh" in price_str or " l" in price_str:
            num = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return int(num * 100000)
        
        # Check for Crores (cr)
        if "cr" in price_str:
            num = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return int(num * 10000000)
        
        # Handle regular numbers
        # Extract just the number if there's any text mixed in
        numbers = re.findall(r"\d+\.?\d*", price_str)
        if numbers:
            return int(float(numbers[0]))
        
        return None
    except:
        return None


def normalize_float(value):
    """Handle float conversion for price_per_sqft and area_sqft"""
    if pd.isna(value):
        return None
    
    value_str = str(value).lower().strip()
    
    # Handle empty strings, 'None', 'nan', etc.
    if not value_str or value_str in ['none', 'nan', 'null', '']:
        return None
    
    # Handle "Price on Request" etc.
    if 'price on request' in value_str or 'price on req' in value_str:
        return None
    
    try:
        # Extract number if there's any text
        numbers = re.findall(r"\d+\.?\d*", value_str)
        if numbers:
            return float(numbers[0])
        return None
    except:
        return None


def normalize_area(area):
    if pd.isna(area):
        return None
    try:
        return float(re.findall(r"\d+\.?\d*", str(area))[0])
    except:
        return None


# ---------------------------
# Core Mapper
# ---------------------------
def map_to_unified(df, source_name):
    unified = pd.DataFrame()

    unified["property_id"] = df["PROP_ID"].astype(str)

    unified["prop_heading"] = df["PROP_HEADING"]
    unified["description"] = df["DESCRIPTION"]

    unified["property_type"] = df["PROPERTY_TYPE"]
    unified["transact_type"] = df["TRANSACT_TYPE"]
    unified["ownership_type"] = df["OWNTYPE"]
    unified["preference"] = df["PREFERENCE"]

    unified["city"] = df["CITY"]
    unified["location"] = df["location"]

    unified["society_name"] = df["SOCIETY_NAME"]
    unified["building_name"] = df["BUILDING_NAME"]
    unified["project_name"] = df["PROP_NAME"]

    unified["bedrooms"] = df["BEDROOM_NUM"]
    unified["balconies"] = df["BALCONY_NUM"]
    unified["floor_num"] = df["FLOOR_NUM"]
    unified["total_floor"] = df["TOTAL_FLOOR"]

    unified["area_raw"] = df["AREA"]
    unified["area_sqft"] = df["AREA"].apply(normalize_float)

    unified["price_raw"] = df["PRICE"]
    unified["price_total"] = df["PRICE"].apply(normalize_price)

    unified["price_per_sqft"] = df["PRICE_SQFT"].apply(normalize_float)

    unified["furnish"] = df["FURNISH"]
    unified["facing"] = df["FACING"]
    unified["property_age"] = df["AGE"]

    unified["amenities"] = df["AMENITIES"]
    unified["features"] = df["FEATURES"]
    unified["secondary_tags"] = df["SECONDARY_TAGS"]

    unified["landmark_count"] = df["TOTAL_LANDMARK_COUNT"]
    unified["landmark_details"] = df["FORMATTED_LANDMARK_DETAILS"]

    unified["map_details"] = df["MAP_DETAILS"]

    unified["listing_source"] = source_name

    return unified


# ---------------------------
# Load Datasets
# ---------------------------
gurgaon = pd.read_csv("datasets/gurgaon_10k.csv", low_memory=False)
hyderabad = pd.read_csv("datasets/hyderabad.csv", low_memory=False)
kolkata = pd.read_csv("datasets/kolkata.csv", low_memory=False)
mumbai = pd.read_csv("datasets/mumbai.csv", low_memory=False)

g1 = map_to_unified(gurgaon, "gurgaon")
g2 = map_to_unified(hyderabad, "hyderabad")
g3 = map_to_unified(kolkata, "kolkata")
g4 = map_to_unified(mumbai, "mumbai")

final_df = pd.concat([g1, g2, g3, g4], ignore_index=True)

# ---------------------------
# Save Output
# ---------------------------
os.makedirs("clean", exist_ok=True)
final_df.to_csv("clean/unified_properties.csv", index=False)

print("✅ Unified dataset created successfully")
print("Total rows:", len(final_df))
print(final_df.head())