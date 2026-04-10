CREATE TABLE studies
(
    study_instance_uid TEXT PRIMARY KEY,

    patient_id         TEXT,
    patient_name       TEXT,
    accession_number   TEXT,
    study_id           TEXT,

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_studies_patient_id ON studies (patient_id);

CREATE TABLE series
(
    series_instance_uid TEXT PRIMARY KEY,
    study_instance_uid  TEXT        NOT NULL,
    modality            TEXT,
    series_number       INTEGER,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fk_series_study
        FOREIGN KEY (study_instance_uid) REFERENCES studies (study_instance_uid)
);

CREATE INDEX idx_series_study_instance_uid ON series (study_instance_uid);

CREATE TABLE instances
(
    sop_instance_uid    TEXT PRIMARY KEY,
    study_instance_uid  TEXT        NOT NULL,
    series_instance_uid TEXT        NOT NULL,
    sop_class_uid       TEXT        NOT NULL,
    instance_number     INTEGER,
    acquisition_date_time TEXT,
    transfer_syntax_uid TEXT,
    blob_key            TEXT,
    blob_version        TEXT,
    blob_size_bytes     BIGINT,
    attributes          JSONB       NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fk_instances_study
        FOREIGN KEY (study_instance_uid) REFERENCES studies (study_instance_uid),
    CONSTRAINT fk_instances_series
        FOREIGN KEY (series_instance_uid) REFERENCES series (series_instance_uid)
);

CREATE INDEX idx_instances_study_instance_uid ON instances (study_instance_uid);
CREATE INDEX idx_instances_series_instance_uid ON instances (series_instance_uid);
CREATE INDEX idx_instances_sop_class_uid ON instances (sop_class_uid);
CREATE INDEX idx_instances_acquisition_date_time ON instances (acquisition_date_time);
CREATE INDEX idx_instances_attributes_gin ON instances USING GIN (attributes);
