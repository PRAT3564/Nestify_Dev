CREATE TABLE unified_properties (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    property_id TEXT,

    prop_heading LONGTEXT,
    description LONGTEXT,

    property_type TEXT,
    transact_type TEXT,
    ownership_type TEXT,
    preference TEXT,

    city TEXT,
    location LONGTEXT,

    society_name LONGTEXT,
    building_name LONGTEXT,
    project_name LONGTEXT,

    bedrooms INT,
    balconies INT,
    floor_num INT,
    total_floor INT,

    area_raw TEXT,
    area_sqft FLOAT,

    price_raw TEXT,
    price_total BIGINT,
    price_per_sqft FLOAT,

    furnish TEXT,
    facing TEXT,
    property_age TEXT,

    amenities LONGTEXT,
    features LONGTEXT,
    secondary_tags LONGTEXT,

    landmark_count INT,
    landmark_details LONGTEXT,

    map_details LONGTEXT,

    listing_source TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);