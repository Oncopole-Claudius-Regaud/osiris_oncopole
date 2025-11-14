SELECT DISTINCT
    pat.PAPMI_No                    AS ipp_ocr,
    adm.PAADM_ADMNo                 AS visit_episode_id,
    adm.PAADM_AdmDate               AS visit_start_date,
    adm.PAADM_AdmTime               AS visit_start_time,
    CAST(adm.PAADM_DischgDate AS DATE)            AS visit_end_date,
    adm.PAADM_DischgTime            AS visit_end_time,
    adm.PAADM_EstimDischargeDate    AS visit_estimated_end_date,
    adm.PAADM_EstimDischargeTime    AS visit_estimated_end_time,
    loc.CTLOC_Desc                  AS visit_functional_unit,
    loc.CTLOC_Code                  AS code_unit,
    doc.CTPCP_Desc                  AS visit_responsible_unit_desc,
    adm.PAADM_Type                  AS visit_type,
    adm.PAADM_VisitStatus           AS visit_status,
    CASE
        WHEN adm.PAADM_VisitStatus = 'A' THEN 'Courant'
        WHEN adm.PAADM_VisitStatus = 'P' THEN 'Préadmission'
        WHEN adm.PAADM_VisitStatus = 'D' THEN 'Sorti'
        WHEN adm.PAADM_VisitStatus = 'C' THEN 'Supprimé (Annulé)'
        WHEN adm.PAADM_VisitStatus = 'N' THEN 'Supprimé (Pas venu)'
        ELSE adm.PAADM_VisitStatus
    END AS visit_status_label,
    motif.REAENC_Comment            AS visit_reason,
    motif.REAENC_CreateDate         AS visit_reason_create_date,
    adm.PAADM_PreAdmitted           AS is_preadmission
FROM SQLUser.PA_ADM AS adm
LEFT JOIN SQLUser.PA_PATMAS AS pat
    ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MR_ADM AS mradm
    ON mradm.MRADM_ADM_DR = adm.PAADM_RowID
LEFT JOIN SQLUser.MR_ReasonForEnc AS motif
    ON motif.REAENC_ParRef = mradm.MRADM_RowId
LEFT JOIN SQLUser.CT_Loc AS loc
    ON adm.PAADM_DepCode_DR = loc.CTLOC_RowID
LEFT JOIN SQLUser.CT_CareProv AS doc
    ON adm.PAADM_AdmDocCodeDR = doc.CTPCP_RowID
WHERE
    (pat.PAPMI_No IS NULL OR pat.PAPMI_No <> '0200500024')
    AND UPPER(COALESCE(loc.CTLOC_Desc, '')) NOT LIKE '%INTERFACE%'

