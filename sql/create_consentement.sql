CREATE TABLE IF NOT EXISTS osiris.consentement (
    ipp_ocr TEXT PRIMARY KEY,
    consentement TEXT,
    date_consentement DATE,
    CONSTRAINT fk_consentement_patient
        FOREIGN KEY (ipp_ocr)
        REFERENCES osiris.patient (ipp_ocr)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
