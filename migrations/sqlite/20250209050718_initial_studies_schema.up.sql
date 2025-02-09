CREATE TABLE IF NOT EXISTS studies
(
    study_instance_uid       VARCHAR(64) PRIMARY KEY, -- Study Instance UID (0020,000D)
    study_date               VARCHAR(8),              -- Study Date (0008,0020) - DATE format
    study_time               VARCHAR(14),             -- Study Time (0008,0030) - TIME format
    accession_number         VARCHAR(16),             -- Accession Number (0008,0050) - Short String (SH)
    referring_physician_name VARCHAR(255),            -- Referring Physician Name (0008,0090) - Person Name (PN)
    patient_name             VARCHAR(255),            -- Patient Name (0010,0010) - Person Name (PN)
    patient_id               VARCHAR(64),             -- Patient ID (0010,0020) - Long String (LO)
    study_id                 VARCHAR(16)              -- Study ID (0020,0010) - Short String (SH)
);

CREATE TABLE IF NOT EXISTS study_series
(
    modality                            VARCHAR(16),                                         -- Modality (0008,0060) - Code String (CS)
    study_instance_uid                  VARCHAR(64) REFERENCES studies (study_instance_uid), -- Study Instance UID (0020,000D)
    series_instance_uid                 VARCHAR(64) PRIMARY KEY,                             -- Series Instance UID (0020,000E)
    series_number                       VARCHAR(12),                                         -- Series Number (0020,0011) - Integer String (IS)
    performed_procedure_step_start_date VARCHAR(8),                                          -- Performed Procedure Step Start Date (0040,0244) - Date (DA)
    performed_procedure_step_start_time VARCHAR(14)                                          -- Performed Procedure Step Start Time (0040,0245) - Time (TM)
);

CREATE TABLE IF NOT EXISTS sop_instances
(
    sop_class_uid       VARCHAR(64),                                               -- SOP Class UID (0008,0016) - UID (UI)
    sop_instance_uid    VARCHAR(64) PRIMARY KEY,                                   -- SOP Instance UID (0008,0018)
    study_instance_uid  VARCHAR(64) REFERENCES studies (study_instance_uid),       -- Study Instance UID (0020,000D)
    series_instance_uid VARCHAR(64) REFERENCES study_series (series_instance_uid), -- Series Instance UID (0020,000E)
    instance_number     VARCHAR(12),                                               -- Instance Number (0020,0013) - Integer String (IS)
    path                VARCHAR(64),                                               -- Resource path in the storage
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP               -- Created At timestamp
);

CREATE INDEX IF NOT EXISTS idx_sop_instances_created_at ON sop_instances (created_at);
