SELECT DISTINCT
    pat.PAPMI_No AS ipp_ocr,
    pe.PAPER_ResidentNumber AS ipp_chu,
    YEAR(pat.PAPMI_DOB) AS annee_naissance,

    CASE
        WHEN pat.PAPMI_Sex_DR = 2 THEN 'M'
        WHEN pat.PAPMI_Sex_DR = 3 THEN 'F'
        ELSE 'I'
    END AS sexe,

    pat.PAPMI_Deceased_Date AS death_of_death,

    diag.PROB_OnsetDate AS date_diagnostic,

    -- Diagnostic + CIM
    MRC.MRCID_Code AS code_cim,
    MRC.MRCID_Desc AS libelle_cim,
    MRC.MRCID_CreatedDate AS cim_created_at,
    MRC.MRCID_UpdatedDate AS cim_updated_at,
    MRC.MRCID_DateActiveFrom AS cim_active_from,
    MRC.MRCID_DateActiveTo AS cim_active_to,

    diag.PROB_EntryStatus AS diagnostic_status,
    diag.PROB_Deleted AS diagnostic_deleted_flag,
    diag.PROB_CreateDate AS date_diagnostic_created_at,
    diag.PROB_UpdateDate AS date_diagnostic_updated_at,
    diag.PROB_EndDate AS date_diagnostic_end

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Person pe ON pe.PAPER_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.PA_Problem diag ON diag.PROB_ParRef = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MRC_ICDDx MRC ON diag.PROB_ICDCode_DR = MRC.MRCID_RowId

WHERE pat.PAPMI_No IN ({patient_list})
