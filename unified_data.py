"""
Unified Real Estate Data Processing Script
Processes and combines real estate data from multiple Indian cities
Based on 30 common columns across all datasets
"""

import pandas as pd
import numpy as np
import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
import warnings
import os
warnings.filterwarnings('ignore')


class UnifiedRealEstateProcessor:
    """
    A class to process and combine real estate data from multiple Indian cities.
    Works with the 30 common columns identified across Hyderabad, Mumbai, Kolkata, and Gurgaon.
    """
    
    def __init__(self):
        """Initialize the unified processor"""
        self.city_data = {}  # Dictionary to store individual city dataframes
        self.unified_data = None  # Combined unified dataframe
        
        # Define the 30 common columns that exist across all cities
        self.common_columns = [
            'PROP_ID',              # Property ID
            'PREFERENCE',           # Preference (S/P/R etc.)
            'DESCRIPTION',          # Property description
            'PROPERTY_TYPE',        # Type of property
            'CITY',                  # City name
            'TRANSACT_TYPE',        # Transaction type
            'OWNTYPE',               # Ownership type
            'BEDROOM_NUM',          # Number of bedrooms
            'PRICE_PER_UNIT_AREA',  # Price per unit area
            'FURNISH',               # Furnishing status
            'FACING',                # Direction facing
            'AGE',                   # Age of property
            'TOTAL_FLOOR',          # Total floors
            'FEATURES',              # Property features
            'PROP_NAME',            # Project name
            'PRICE_SQFT',           # Price per sq ft
            'MAP_DETAILS',          # Map/location details
            'AMENITIES',            # Available amenities
            'AREA',                  # Area description
            'PRICE',                 # Price
            'PROP_HEADING',         # Property heading
            'SECONDARY_TAGS',       # Secondary tags
            'TOTAL_LANDMARK_COUNT', # Count of landmarks
            'FORMATTED_LANDMARK_DETAILS',  # Landmark details
            'SOCIETY_NAME',         # Society name
            'BUILDING_NAME',        # Building name
            'location',              # Location
            'BALCONY_NUM',          # Number of balconies
            'FLOOR_NUM'              # Floor number
        ]
        
        # Define data types for each column
        self.column_dtypes = {
            'PROP_ID': 'object',
            'PREFERENCE': 'object',
            'DESCRIPTION': 'object',
            'PROPERTY_TYPE': 'object',
            'CITY': 'object',
            'TRANSACT_TYPE': 'float64',
            'OWNTYPE': 'float64',
            'BEDROOM_NUM': 'float64',
            'PRICE_PER_UNIT_AREA': 'float64',
            'FURNISH': 'object',
            'FACING': 'object',
            'AGE': 'float64',
            'TOTAL_FLOOR': 'float64',
            'FEATURES': 'object',
            'PROP_NAME': 'object',
            'PRICE_SQFT': 'float64',
            'MAP_DETAILS': 'object',
            'AMENITIES': 'object',
            'AREA': 'object',
            'PRICE': 'object',  # Will be parsed to numeric
            'PROP_HEADING': 'object',
            'SECONDARY_TAGS': 'object',
            'TOTAL_LANDMARK_COUNT': 'float64',
            'FORMATTED_LANDMARK_DETAILS': 'object',
            'SOCIETY_NAME': 'object',
            'BUILDING_NAME': 'object',
            'location': 'object',
            'BALCONY_NUM': 'float64',
            'FLOOR_NUM': 'float64'
        }
        
        # Derived columns that will be added
        self.derived_columns = [
            'PRICE_NUMERIC',        # Parsed price as number
            'PRICE_IN_CRORES',      # Price in crores
            'PRICE_IN_LAKHS',       # Price in lakhs
            'AREA_SQFT',            # Extracted area in sq ft
            'LOCALITY',              # Extracted locality
            'LATITUDE',              # Latitude from MAP_DETAILS
            'LONGITUDE',             # Longitude from MAP_DETAILS
            'PRICE_CATEGORY',        # Budget/Mid-range/Premium/Luxury
            'FURNISHING_CATEGORY',   # Standardized furnishing
            'PROPERTY_AGE_CATEGORY', # New/Under-construction/Ready-to-move
            'HAS_POOL',              # Boolean: has swimming pool
            'HAS_GYM',               # Boolean: has gym
            'HAS_PARKING',           # Boolean: has parking
            'HAS_LIFT',              # Boolean: has lift
            'DATA_COMPLETENESS'      # Score of how complete the record is
        ]
    
    def load_city_data(self, file_path: str, city_name: str, file_type: str = 'csv') -> pd.DataFrame:
        """
        Load data for a specific city.
        
        Args:
            file_path: Path to the data file
            city_name: Name of the city
            file_type: Type of file ('csv' or 'excel')
            
        Returns:
            Loaded dataframe for the city
        """
        try:
            if file_type.lower() == 'csv':
                df = pd.read_csv(file_path, low_memory=False, encoding='utf-8')
            elif file_type.lower() == 'excel':
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Store in city_data dictionary
            self.city_data[city_name] = {
                'data': df,
                'file_path': file_path,
                'shape': df.shape
            }
            
            print(f"✅ Loaded {city_name} data: {df.shape[0]:,} rows, {df.shape[1]} columns")
            return df
            
        except Exception as e:
            print(f"❌ Error loading {city_name} data: {e}")
            return None
    
    def load_multiple_cities(self, city_files: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Load data for multiple cities at once.
        
        Args:
            city_files: Dictionary with city names as keys and file paths as values
                       e.g., {'Hyderabad': 'hyderabad.csv', 'Mumbai': 'mumbai.csv'}
        
        Returns:
            Dictionary of loaded dataframes
        """
        loaded_dfs = {}
        for city_name, file_path in city_files.items():
            print(f"\n--- Loading {city_name} data ---")
            
            # Determine file type from extension
            file_ext = os.path.splitext(file_path)[1].lower()
            file_type = 'excel' if file_ext in ['.xlsx', '.xls'] else 'csv'
            
            df = self.load_city_data(file_path, city_name, file_type)
            if df is not None:
                loaded_dfs[city_name] = df
        
        print(f"\n✅ Successfully loaded {len(loaded_dfs)} cities")
        return loaded_dfs
    
    def _parse_json_field(self, field_value):
        """Helper function to parse JSON-like fields"""
        if pd.isna(field_value):
            return None
        if isinstance(field_value, (dict, list)):
            return field_value
        try:
            if isinstance(field_value, str):
                # Handle Python list/dict string representation
                if field_value.startswith('[') or field_value.startswith('{'):
                    # Safely evaluate
                    return eval(field_value, {'__builtins__': {}}, {})
                # Handle single-quoted JSON
                field_value = field_value.replace("'", '"')
                return json.loads(field_value)
        except:
            pass
        return field_value
    
    def _parse_price(self, price_str):
        """
        Parse price string to numeric value in rupees.
        Handles formats like "1.2 Cr", "85 L", "₹1.5 Cr", etc.
        """
        if pd.isna(price_str):
            return np.nan
        
        price_str = str(price_str).strip()
        
        # Remove ₹ symbol and commas
        price_str = price_str.replace('₹', '').replace(',', '').strip()
        
        # Check for Crores
        if 'cr' in price_str.lower():
            try:
                value = float(re.sub(r'[^\d.]', '', price_str))
                return value * 10000000  # 1 Cr = 10 million
            except:
                return np.nan
        
        # Check for Lakhs
        if 'lac' in price_str.lower() or 'lakh' in price_str.lower():
            try:
                value = float(re.sub(r'[^\d.]', '', price_str))
                return value * 100000  # 1 Lac = 100,000
            except:
                return np.nan
        
        # Try direct numeric conversion
        try:
            return float(price_str)
        except:
            return np.nan
    
    def _parse_area(self, area_str):
        """Extract area in sq ft from area string"""
        if pd.isna(area_str):
            return np.nan
        
        area_str = str(area_str).lower()
        
        # Look for sq ft pattern
        sqft_match = re.search(r'(\d+[.,]?\d*)\s*sq\.?\s*ft', area_str)
        if sqft_match:
            try:
                return float(sqft_match.group(1).replace(',', ''))
            except:
                pass
        
        # Look for sq m pattern and convert
        sqm_match = re.search(r'(\d+[.,]?\d*)\s*sq\.?\s*m', area_str)
        if sqm_match:
            try:
                sqm = float(sqm_match.group(1).replace(',', ''))
                return sqm * 10.764  # Convert to sq ft
            except:
                pass
        
        # Try to extract just numbers
        numbers = re.findall(r'(\d+[.,]?\d*)', area_str)
        if numbers:
            try:
                return float(numbers[0].replace(',', ''))
            except:
                pass
        
        return np.nan
    
    def _extract_from_location(self, loc_value):
        """Extract locality from location field"""
        if pd.isna(loc_value):
            return None
        
        if isinstance(loc_value, dict):
            return loc_value.get('LOCALITY_NAME', 
                                loc_value.get('LOCALITY', 
                                loc_value.get('CITY_NAME', None)))
        
        # If it's a string, try to extract locality name
        if isinstance(loc_value, str):
            # Look for patterns like "LOCALITY_NAME: 'Kompally'"
            match = re.search(r"'LOCALITY_NAME':\s*'([^']+)'", loc_value)
            if match:
                return match.group(1)
            
            # Or just return the string if it's short
            if len(loc_value) < 50:
                return loc_value
        
        return loc_value
    
    def _extract_coordinates(self, map_details):
        """Extract latitude and longitude from MAP_DETAILS"""
        lat, lon = np.nan, np.nan
        
        if pd.isna(map_details):
            return lat, lon
        
        if isinstance(map_details, dict):
            lat = map_details.get('LATITUDE', np.nan)
            lon = map_details.get('LONGITUDE', np.nan)
            try:
                lat = float(lat) if lat not in [None, ''] else np.nan
                lon = float(lon) if lon not in [None, ''] else np.nan
            except:
                lat, lon = np.nan, np.nan
        
        return lat, lon
    
    def _check_amenity(self, amenities, keyword):
        """Check if a specific amenity exists in amenities field"""
        if pd.isna(amenities):
            return False
        
        if isinstance(amenities, list):
            keyword_lower = keyword.lower()
            for item in amenities:
                if isinstance(item, str) and keyword_lower in item.lower():
                    return True
            return False
        
        if isinstance(amenities, str):
            return keyword.lower() in amenities.lower()
        
        return False
    
    def _standardize_furnishing(self, furnish_val):
        """Standardize furnishing status to categories"""
        if pd.isna(furnish_val):
            return 'Not Specified'
        
        furnish_str = str(furnish_val).lower()
        
        if 'semi' in furnish_str or furnish_val in [2, 4, '2', '4']:
            return 'Semi-Furnished'
        elif 'full' in furnish_str or 'fully' in furnish_str or furnish_val in [1, 3, '1', '3']:
            return 'Fully Furnished'
        elif 'un' in furnish_str or furnish_val in [0, 5, '0', '5']:
            return 'Unfurnished'
        else:
            return 'Not Specified'
    
    def _get_age_category(self, age_val):
        """Categorize property age"""
        if pd.isna(age_val):
            return 'Unknown'
        
        try:
            age = float(age_val)
            if age == 0 or age == 0.0:
                return 'New/Under Construction'
            elif age <= 5:
                return 'New (0-5 years)'
            elif age <= 10:
                return 'Moderate (5-10 years)'
            elif age <= 20:
                return 'Old (10-20 years)'
            else:
                return 'Very Old (20+ years)'
        except:
            return 'Unknown'
    
    def _get_price_category(self, price_val):
        """Categorize price into budget segments"""
        if pd.isna(price_val):
            return 'Unknown'
        
        try:
            price = float(price_val)
            if price < 5000000:  # < 50 Lakhs
                return 'Budget (< 50L)'
            elif price < 10000000:  # 50L - 1 Cr
                return 'Mid-Range (50L - 1Cr)'
            elif price < 20000000:  # 1 Cr - 2 Cr
                return 'Premium (1Cr - 2Cr)'
            elif price < 50000000:  # 2 Cr - 5 Cr
                return 'Luxury (2Cr - 5Cr)'
            else:  # > 5 Cr
                return 'Ultra-Luxury (> 5Cr)'
        except:
            return 'Unknown'
    
    def _calculate_completeness(self, row):
        """Calculate data completeness score for a row (0-100)"""
        total_fields = len(self.common_columns)
        non_null = row[self.common_columns].notna().sum()
        return round((non_null / total_fields) * 100, 2)
    
    def extract_city_columns(self, df: pd.DataFrame, city_name: str) -> pd.DataFrame:
        """
        Extract and standardize the 30 common columns for a city.
        
        Args:
            df: Raw dataframe for the city
            city_name: Name of the city
            
        Returns:
            Dataframe with only the 30 common columns
        """
        print(f"  Extracting common columns for {city_name}...")
        
        # Create empty dataframe with common columns
        extracted = pd.DataFrame(index=df.index)
        
        # For each common column, try to find it in the source data
        for col in self.common_columns:
            if col in df.columns:
                # Column exists directly
                extracted[col] = df[col]
            else:
                # Try case-insensitive match
                col_upper = col.upper()
                col_lower = col.lower()
                col_title = col.title()
                
                found = False
                for source_col in df.columns:
                    if source_col.upper() == col_upper or source_col.lower() == col_lower or source_col == col_title:
                        extracted[col] = df[source_col]
                        found = True
                        break
                
                if not found:
                    # Column doesn't exist, fill with NaN
                    extracted[col] = np.nan
        
        # Add source city for tracking
        extracted['_source_city'] = city_name
        
        print(f"  ✅ Extracted {len(self.common_columns)} columns")
        return extracted
    
    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns to the dataframe for better analysis.
        
        Args:
            df: Dataframe with common columns
            
        Returns:
            Enriched dataframe with additional derived columns
        """
        print("  Adding derived columns...")
        
        enriched = df.copy()
        
        # Parse price to numeric
        print("    • Parsing price values...")
        enriched['PRICE_NUMERIC'] = enriched['PRICE'].apply(self._parse_price)
        
        # Price in crores and lakhs
        enriched['PRICE_IN_CRORES'] = enriched['PRICE_NUMERIC'] / 10000000
        enriched['PRICE_IN_LAKHS'] = enriched['PRICE_NUMERIC'] / 100000
        
        # Extract area in sq ft
        print("    • Extracting area values...")
        enriched['AREA_SQFT'] = enriched['AREA'].apply(self._parse_area)
        
        # If AREA_SQFT is NaN, try to get from PRICE_SQFT and PRICE
        mask = enriched['AREA_SQFT'].isna() & enriched['PRICE_SQFT'].notna() & enriched['PRICE_NUMERIC'].notna()
        enriched.loc[mask, 'AREA_SQFT'] = enriched.loc[mask, 'PRICE_NUMERIC'] / enriched.loc[mask, 'PRICE_SQFT']
        
        # Extract locality from location
        print("    • Extracting locality information...")
        enriched['LOCALITY'] = enriched['location'].apply(self._extract_from_location)
        
        # Extract coordinates from MAP_DETAILS
        print("    • Extracting coordinates...")
        coords = enriched['MAP_DETAILS'].apply(self._extract_coordinates)
        enriched['LATITUDE'] = coords.apply(lambda x: x[0])
        enriched['LONGITUDE'] = coords.apply(lambda x: x[1])
        
        # Price category
        print("    • Creating price categories...")
        enriched['PRICE_CATEGORY'] = enriched['PRICE_NUMERIC'].apply(self._get_price_category)
        
        # Standardize furnishing
        enriched['FURNISHING_CATEGORY'] = enriched['FURNISH'].apply(self._standardize_furnishing)
        
        # Property age category
        enriched['PROPERTY_AGE_CATEGORY'] = enriched['AGE'].apply(self._get_age_category)
        
        # Parse amenities and create boolean columns
        print("    • Parsing amenities...")
        enriched['FEATURES_PARSED'] = enriched['FEATURES'].apply(self._parse_json_field)
        enriched['AMENITIES_PARSED'] = enriched['AMENITIES'].apply(self._parse_json_field)
        enriched['SECONDARY_TAGS_PARSED'] = enriched['SECONDARY_TAGS'].apply(self._parse_json_field)
        
        # Common amenity flags
        enriched['HAS_POOL'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'swimming pool'))
        enriched['HAS_GYM'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'gym'))
        enriched['HAS_PARKING'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'parking'))
        enriched['HAS_LIFT'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'lift'))
        enriched['HAS_SECURITY'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'security'))
        enriched['HAS_CLUBHOUSE'] = enriched['AMENITIES'].apply(lambda x: self._check_amenity(x, 'club'))
        
        # Data completeness score
        print("    • Calculating data completeness...")
        enriched['DATA_COMPLETENESS'] = enriched.apply(self._calculate_completeness, axis=1)
        
        print(f"  ✅ Added {len(self.derived_columns)} derived columns")
        return enriched
    
    def process_city(self, city_name: str) -> pd.DataFrame:
        """
        Process a single city's data.
        
        Args:
            city_name: Name of the city to process
            
        Returns:
            Processed dataframe for the city
        """
        if city_name not in self.city_data:
            print(f"❌ No data loaded for {city_name}")
            return None
        
        df = self.city_data[city_name]['data']
        
        print(f"\n🔄 Processing {city_name}...")
        
        # Step 1: Extract common columns
        extracted = self.extract_city_columns(df, city_name)
        print(f"  Extracted shape: {extracted.shape}")
        
        # Step 2: Enrich with derived columns
        enriched = self.enrich_dataframe(extracted)
        print(f"  Enriched shape: {enriched.shape}")
        
        return enriched
    
    def process_all_cities(self) -> pd.DataFrame:
        """
        Process all loaded city data and combine into unified dataset.
        
        Returns:
            Combined unified dataframe
        """
        if not self.city_data:
            print("❌ No city data loaded. Please load data first.")
            return None
        
        processed_dfs = []
        
        for city_name in self.city_data.keys():
            processed = self.process_city(city_name)
            if processed is not None and not processed.empty:
                processed_dfs.append(processed)
        
        if processed_dfs:
            # Combine all processed dataframes
            self.unified_data = pd.concat(processed_dfs, ignore_index=True, sort=False)
            
            print(f"\n{'='*60}")
            print(f"✅ UNIFIED DATASET CREATED SUCCESSFULLY!")
            print(f"{'='*60}")
            print(f"Total records: {len(self.unified_data):,}")
            print(f"Cities included: {self.unified_data['_source_city'].unique().tolist()}")
            print(f"Total columns: {len(self.unified_data.columns)}")
            print(f"Common columns: {len(self.common_columns)}")
            print(f"Derived columns: {len(self.derived_columns)}")
            
            return self.unified_data
        else:
            print("❌ No data processed successfully")
            return None
    
    def add_new_city(self, file_path: str, city_name: str, file_type: str = 'csv') -> pd.DataFrame:
        """
        Add a new city to the unified dataset.
        
        Args:
            file_path: Path to the new city data file
            city_name: Name of the new city
            file_type: Type of file ('csv' or 'excel')
            
        Returns:
            Updated unified dataframe
        """
        print(f"\n{'='*60}")
        print(f"➕ Adding new city: {city_name}")
        print(f"{'='*60}")
        
        # Load the data
        df = self.load_city_data(file_path, city_name, file_type)
        if df is None:
            return self.unified_data
        
        # Process the city
        processed = self.process_city(city_name)
        
        if processed is not None and not processed.empty:
            # Add to unified dataset
            if self.unified_data is None:
                self.unified_data = processed
            else:
                self.unified_data = pd.concat([self.unified_data, processed], ignore_index=True)
            
            print(f"\n✅ Successfully added {city_name} data")
            print(f"   Added {len(processed):,} records")
            print(f"   Total records now: {len(self.unified_data):,}")
        else:
            print(f"❌ Failed to process {city_name} data")
        
        return self.unified_data
    
    def save_unified_data(self, output_path: str, format: str = 'csv'):
        """
        Save the unified dataset to file.
        
        Args:
            output_path: Path to save the file
            format: 'csv' or 'excel'
        """
        if self.unified_data is None:
            print("❌ No unified data to save")
            return
        
        try:
            if format.lower() == 'csv':
                self.unified_data.to_csv(output_path, index=False, encoding='utf-8')
            elif format.lower() == 'excel':
                self.unified_data.to_excel(output_path, index=False, engine='openpyxl')
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            print(f"\n✅ Unified data saved to: {output_path}")
            print(f"   Shape: {self.unified_data.shape}")
        except Exception as e:
            print(f"❌ Error saving data: {e}")
    
    def get_city_summary(self) -> pd.DataFrame:
        """Get summary statistics by city"""
        if self.unified_data is None:
            print("❌ No unified data available")
            return None
        
        summary = self.unified_data.groupby('_source_city').agg({
            'PROP_ID': 'count',
            'PRICE_NUMERIC': ['mean', 'min', 'max'],
            'AREA_SQFT': 'mean',
            'BEDROOM_NUM': 'mean',
            'BALCONY_NUM': 'mean',
            'DATA_COMPLETENESS': 'mean',
            'HAS_POOL': 'mean',
            'HAS_GYM': 'mean'
        }).round(2)
        
        summary.columns = ['count', 'avg_price', 'min_price', 'max_price', 
                          'avg_area_sqft', 'avg_bedrooms', 'avg_balconies',
                          'data_completeness', 'pct_with_pool', 'pct_with_gym']
        
        # Format price in crores
        summary['avg_price_cr'] = summary['avg_price'] / 10000000
        summary['min_price_cr'] = summary['min_price'] / 10000000
        summary['max_price_cr'] = summary['max_price'] / 10000000
        
        return summary
    
    def print_summary(self):
        """Print comprehensive summary of the unified dataset"""
        if self.unified_data is None:
            print("❌ No unified data available")
            return
        
        print("\n" + "="*70)
        print("📊 UNIFIED REAL ESTATE DATASET SUMMARY")
        print("="*70)
        
        print(f"\n📌 Total Records: {len(self.unified_data):,}")
        print(f"📌 Total Columns: {len(self.unified_data.columns)}")
        print(f"   • Common columns: {len(self.common_columns)}")
        print(f"   • Derived columns: {len(self.derived_columns)}")
        
        # City distribution
        print("\n🏙️ City Distribution:")
        city_counts = self.unified_data['_source_city'].value_counts()
        for city, count in city_counts.items():
            pct = (count / len(self.unified_data)) * 100
            completeness = self.unified_data[self.unified_data['_source_city'] == city]['DATA_COMPLETENESS'].mean()
            print(f"  • {city}: {count:,} ({pct:.1f}%) - Data completeness: {completeness:.1f}%")
        
        # Property types
        if 'PROPERTY_TYPE' in self.unified_data.columns:
            print("\n🏠 Top Property Types:")
            type_counts = self.unified_data['PROPERTY_TYPE'].value_counts().head(5)
            for prop_type, count in type_counts.items():
                if pd.notna(prop_type):
                    print(f"  • {prop_type}: {count:,}")
        
        # Price summary
        print("\n💰 Price Summary (in Crores):")
        price_data = self.unified_data.groupby('_source_city')['PRICE_IN_CRORES'].mean()
        for city, avg_price in price_data.items():
            print(f"  • {city}: ₹{avg_price:.2f} Cr")
        
        # Price categories
        if 'PRICE_CATEGORY' in self.unified_data.columns:
            print("\n📦 Price Categories:")
            cat_counts = self.unified_data['PRICE_CATEGORY'].value_counts()
            for category, count in cat_counts.items():
                if pd.notna(category) and category != 'Unknown':
                    print(f"  • {category}: {count:,} ({count/len(self.unified_data)*100:.1f}%)")
        
        # Furnishing distribution
        if 'FURNISHING_CATEGORY' in self.unified_data.columns:
            print("\n🛋️ Furnishing Status:")
            furn_counts = self.unified_data['FURNISHING_CATEGORY'].value_counts()
            for furn, count in furn_counts.items():
                if pd.notna(furn):
                    print(f"  • {furn}: {count:,} ({count/len(self.unified_data)*100:.1f}%)")
        
        # Age categories
        if 'PROPERTY_AGE_CATEGORY' in self.unified_data.columns:
            print("\n📅 Property Age:")
            age_counts = self.unified_data['PROPERTY_AGE_CATEGORY'].value_counts()
            for age, count in age_counts.items():
                if pd.notna(age):
                    print(f"  • {age}: {count:,}")
        
        # Amenity prevalence
        print("\n🏊 Common Amenities:")
        amenities = ['HAS_POOL', 'HAS_GYM', 'HAS_PARKING', 'HAS_LIFT', 'HAS_SECURITY', 'HAS_CLUBHOUSE']
        for amenity in amenities:
            if amenity in self.unified_data.columns:
                pct = self.unified_data[amenity].mean() * 100
                print(f"  • {amenity.replace('HAS_', '').title()}: {pct:.1f}%")
        
        print("\n" + "="*70)


# Main execution function
def create_unified_dataset():
    """
    Main function to create unified dataset from multiple cities
    """
    print("\n" + "="*70)
    print("🏢 UNIFIED REAL ESTATE DATA PROCESSOR")
    print("="*70)
    print("\nProcessing 30 common columns across all cities")
    print("-" * 50)
    
    # Initialize processor
    processor = UnifiedRealEstateProcessor()
    
    # Define city files - UPDATE THESE PATHS WITH YOUR ACTUAL FILE PATHS
    city_files = {
        'Hyderabad': 'datasets/hyderabad.csv',
        'Mumbai': 'datasets/mumbai.csv',
        'Kolkata': 'datasets/kolkata.csv',
        'Gurgaon': 'datasets/gurgaon_10k.csv'
    }
    
    # Check which files exist
    available_files = {}
    for city, file_path in city_files.items():
        if os.path.exists(file_path):
            available_files[city] = file_path
            print(f"✅ Found: {file_path}")
        else:
            print(f"⚠️  Not found: {file_path} - skipping {city}")
    
    if not available_files:
        print("\n❌ No data files found. Please update the file paths.")
        return None
    
    # Load available city data
    processor.load_multiple_cities(available_files)
    
    # Process all cities and create unified dataset
    unified_df = processor.process_all_cities()
    
    if unified_df is not None:
        # Print summary
        processor.print_summary()
        
        # Show city-wise summary
        print("\n📈 City-wise Summary:")
        city_summary = processor.get_city_summary()
        print(city_summary)
        
        # Save unified dataset
        processor.save_unified_data('unified_real_estate_data.csv', 'csv')
        
        # Also save as Excel for better compatibility
        processor.save_unified_data('unified_real_estate_data.xlsx', 'excel')
        
        # Save sample of first 100 rows for quick inspection
        sample_df = unified_df.head(100)
        sample_df.to_csv('unified_sample.csv', index=False)
        print("\n✅ Sample saved to: unified_sample.csv")
    
    return processor


# Function to add a new city later
def add_new_city(processor: UnifiedRealEstateProcessor, file_path: str, city_name: str):
    """
    Add a new city to existing unified dataset
    """
    print(f"\n🔄 Adding {city_name} to existing dataset...")
    
    # Add the new city
    updated_df = processor.add_new_city(
        file_path=file_path,
        city_name=city_name,
        file_type='csv'  # or 'excel'
    )
    
    # Save updated dataset
    if updated_df is not None:
        processor.save_unified_data('unified_real_estate_data_updated.csv', 'csv')
        processor.save_unified_data('unified_real_estate_data_updated.xlsx', 'excel')
        print(f"\n✅ {city_name} added successfully!")
    
    return processor


# Quick analysis functions
def analyze_by_locality(processor: UnifiedRealEstateProcessor, city: str = None, top_n: int = 10):
    """Analyze properties by locality"""
    if processor.unified_data is None:
        print("❌ No unified data available")
        return
    
    df = processor.unified_data.copy()
    
    if city:
        df = df[df['_source_city'] == city]
        print(f"\n📍 Top {top_n} Localities in {city}:")
    else:
        print(f"\n📍 Top {top_n} Localities Overall:")
    
    locality_stats = df.groupby('LOCALITY').agg({
        'PROP_ID': 'count',
        'PRICE_NUMERIC': 'mean',
        'AREA_SQFT': 'mean',
        'BEDROOM_NUM': 'mean'
    }).round(2)
    
    locality_stats.columns = ['count', 'avg_price', 'avg_area_sqft', 'avg_bedrooms']
    locality_stats = locality_stats.sort_values('count', ascending=False).head(top_n)
    
    # Add price in crores
    locality_stats['avg_price_cr'] = locality_stats['avg_price'] / 10000000
    
    print(locality_stats)
    return locality_stats


def filter_properties(processor: UnifiedRealEstateProcessor, 
                     city: str = None,
                     min_bedrooms: int = None,
                     max_price_cr: float = None,
                     property_type: str = None):
    """Filter properties based on criteria"""
    if processor.unified_data is None:
        print("❌ No unified data available")
        return
    
    df = processor.unified_data.copy()
    
    # Apply filters
    if city:
        df = df[df['_source_city'] == city]
    
    if min_bedrooms:
        df = df[df['BEDROOM_NUM'] >= min_bedrooms]
    
    if max_price_cr:
        df = df[df['PRICE_IN_CRORES'] <= max_price_cr]
    
    if property_type:
        df = df[df['PROPERTY_TYPE'].str.contains(property_type, case=False, na=False)]
    
    print(f"\n🔍 Found {len(df)} properties matching criteria")
    return df


if __name__ == "__main__":
    # Create unified dataset
    processor = create_unified_dataset()
    
    # Example: Later add a new city
    # if processor:
    #     processor = add_new_city(processor, 'pune.csv', 'Pune')
    
    print("\n" + "="*70)
    print("✅ SCRIPT READY FOR USE")
    print("="*70)
    print("\nTo create unified dataset:")
    print("  processor = create_unified_dataset()")
    print("\nTo add a new city later:")
    print("  processor.add_new_city('path/to/city_data.csv', 'CityName')")
    print("\nTo analyze by locality:")
    print("  analyze_by_locality(processor, city='Hyderabad')")
    print("\nTo filter properties:")
    print("  filtered = filter_properties(processor, min_bedrooms=3, max_price_cr=1.5, property_type='Apartment')")