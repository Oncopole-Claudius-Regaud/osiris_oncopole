SELECT
    pat.PAPMI_No AS ipp_ocr,
    adm.PAADM_ADMNo AS visit_episode_id,
    -- Dates et heures d'admission/sortie
    adm.PAADM_AdmDate AS visit_start_date,
    adm.PAADM_AdmTime AS visit_start_time,
    adm.PAADM_DischgDate AS visit_end_date,
    adm.PAADM_DischgTime AS visit_end_time,
    adm.PAADM_EstimDischargeDate AS visit_estimated_end_date,
    adm.PAADM_EstimDischargeTime AS visit_estimated_end_time,
    loc.CTLOC_Desc AS visit_functional_unit,
    -- Autres infos
    adm.PAADM_Type AS visit_type,
    adm.PAADM_VisitStatus AS visit_status,


    motif.REAENC_Comment AS visit_reason,
    motif.REAENC_CreateDate AS visit_reason_create_date,
    motif.REAENC_Deleted AS visit_reason_deleted_flag,
    adm.PAADM_PreAdmitted AS is_preadmission

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_ADM adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MR_ADM mradm ON mradm.MRADM_ADM_DR = adm.PAADM_RowID
LEFT JOIN SQLUser.MR_ReasonForEnc motif ON motif.REAENC_ParRef = mradm.MRADM_RowId
LEFT JOIN SQLUser.CT_Loc loc ON adm.PAADM_DepCode_DR = loc.CTLOC_RowID

WHERE pat.PAPMI_No IN ({patient_list})
