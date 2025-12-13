-- GDPR Healthcare Pipeline - Redshift Schema
-- Creates patient_vitals table for storing pseudonymized health records

CREATE SCHEMA IF NOT EXISTS patient_data;

CREATE TABLE IF NOT EXISTS patient_data.patient_vitals (
    record_id VARCHAR(64) NOT NULL,
    patient_id_hash VARCHAR(64) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    heart_rate INTEGER,
    blood_pressure_systolic INTEGER,
    blood_pressure_diastolic INTEGER,
    temperature_celsius DECIMAL(4,2),
    oxygen_saturation DECIMAL(5,2),
    source VARCHAR(100),
    version VARCHAR(20),
    is_test BOOLEAN,
    processed_at TIMESTAMP,
    year VARCHAR(4) NOT NULL,
    month VARCHAR(2) NOT NULL,
    day VARCHAR(2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (record_id)
)
DISTSTYLE KEY
DISTKEY (patient_id_hash)
SORTKEY (timestamp, patient_id_hash);

-- Grant permissions for data access
GRANT USAGE ON SCHEMA patient_data TO PUBLIC;
GRANT SELECT, INSERT, DELETE ON patient_data.patient_vitals TO PUBLIC;
