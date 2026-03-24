CREATE TABLE IF NOT EXISTS speed_violations (
    id SERIAL PRIMARY KEY,
    timestamp VARCHAR(255),
    location VARCHAR(255),
    speed DOUBLE PRECISION,
    speed_limit DOUBLE PRECISION,
    vehicle_type VARCHAR(255),
    traffic_density VARCHAR(255),
    road_condition VARCHAR(255),
    weather_condition VARCHAR(255),
    incident_reported BOOLEAN,
    incident_details TEXT,
    vehicle_id VARCHAR(255),
    make VARCHAR(255),
    model VARCHAR(255),
    color VARCHAR(255),
    year VARCHAR(255)
);