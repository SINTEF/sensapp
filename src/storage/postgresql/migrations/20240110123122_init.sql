-- Add migration script here
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id INTEGER PRIMARY KEY AUTOINCREMENT, --toto
    name TEXT NOT NULL UNIQUE,
    description TEXT
);
