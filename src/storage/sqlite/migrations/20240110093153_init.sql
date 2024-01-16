-- Create the 'units' table
CREATE TABLE units (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing primary key
    name TEXT NOT NULL UNIQUE, -- Unique name, cannot be null
    description TEXT -- Optional description
);

-- Create the 'sensors' table with both UUID and auto-incrementing sensor_id
CREATE TABLE sensors (
    sensor_id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing integer for relationships
    uuid TEXT NOT NULL UNIQUE, -- UUID as text for unique sensor identification, cannot be null
    name TEXT NOT NULL, -- Name of the sensor, cannot be null
    type TEXT NOT NULL, -- Type of the sensor (e.g., integer, float, etc.), cannot be null
    unit INTEGER, -- References 'units' (optional)
    FOREIGN KEY (unit) REFERENCES units(id) -- Foreign key to 'units' table
);

-- Create the 'labels' table
CREATE TABLE labels (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    named INTEGER NOT NULL, -- ID for the name in the dictionary, cannot be null
    description INTEGER, -- ID for the description in the dictionary (optional)
    PRIMARY KEY (sensor_id, named),
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);

-- Create the 'labels_name_dictionary' table
CREATE TABLE labels_name_dictionary (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing primary key
    name TEXT NOT NULL UNIQUE -- Unique name for label, cannot be null
);

-- Create the 'labels_description_dictionary' table
CREATE TABLE labels_description_dictionary (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing primary key
    description TEXT NOT NULL UNIQUE -- Unique description for label, cannot be null
);

-- Create the 'strings_values_dictionary' table
CREATE TABLE strings_values_dictionary (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-incrementing primary key
    value TEXT NOT NULL UNIQUE -- Unique text value, cannot be null
);

-- Create the 'integer_values' table
CREATE TABLE integer_values (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    value INTEGER NOT NULL, -- Integer value, cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);

-- Create the 'numeric_values' table
CREATE TABLE numeric_values (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    value NUMERIC NOT NULL, -- Numeric value, cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);

-- Create the 'float_values' table
CREATE TABLE float_values (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    value REAL NOT NULL, -- Real (float) value, cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);

-- Create the 'string_values' table
CREATE TABLE string_values (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    value INTEGER NOT NULL, -- References 'strings_values_dictionary', cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id), -- Foreign key to 'sensors' table
    FOREIGN KEY (value) REFERENCES strings_values_dictionary(id) -- Foreign key to 'strings_values_dictionary'
);

-- Create the 'boolean_values' table
CREATE TABLE boolean_values (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    value BOOLEAN NOT NULL, -- Boolean value, cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);

-- Create the 'localisations' table
CREATE TABLE localisations (
    sensor_id INTEGER NOT NULL, -- References 'sensors' (sensor_id), cannot be null
    timestamp_ms INTEGER NOT NULL, -- Unix timestamp in milliseconds, cannot be null
    latitude REAL NOT NULL, -- Latitude value, cannot be null
    longitude REAL NOT NULL, -- Longitude value, cannot be null
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) -- Foreign key to 'sensors' table
);