CREATE TABLE IF NOT EXISTS studies
(
    study_instance_uid TEXT PRIMARY KEY,

    patient_id TEXT,
    patient_name TEXT,
    accession_number TEXT,
    study_id TEXT,

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_studies_patient_id
    ON studies (patient_id);

CREATE TABLE IF NOT EXISTS series
(
    series_instance_uid TEXT PRIMARY KEY,
    study_instance_uid TEXT NOT NULL,
    modality TEXT,
    series_number INTEGER,

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (study_instance_uid) REFERENCES studies (study_instance_uid)
);

CREATE INDEX IF NOT EXISTS idx_series_study_instance_uid
    ON series (study_instance_uid);

CREATE TABLE IF NOT EXISTS instances
(
    sop_instance_uid TEXT PRIMARY KEY,
    study_instance_uid TEXT NOT NULL,
    series_instance_uid TEXT NOT NULL,
    sop_class_uid TEXT NOT NULL,
    instance_number INTEGER,
    acquisition_date_time TEXT,
    transfer_syntax_uid TEXT,
    blob_key TEXT,
    blob_version TEXT,
    blob_size_bytes INTEGER,
    attributes TEXT NOT NULL,

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (study_instance_uid) REFERENCES studies (study_instance_uid),
    FOREIGN KEY (series_instance_uid) REFERENCES series (series_instance_uid)
);

CREATE INDEX IF NOT EXISTS idx_instances_study_instance_uid
    ON instances (study_instance_uid);

CREATE INDEX IF NOT EXISTS idx_instances_series_instance_uid
    ON instances (series_instance_uid);

CREATE INDEX IF NOT EXISTS idx_instances_sop_class_uid
    ON instances (sop_class_uid);

CREATE INDEX IF NOT EXISTS idx_instances_acquisition_date_time
    ON instances (acquisition_date_time);

CREATE INDEX IF NOT EXISTS idx_instances_attributes
    ON instances (attributes);
