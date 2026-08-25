SELECT
    -- Identifiants
    pat.PAPMI_No AS ipp_ocr,

    -- Dates du diagnostic
    diag.PROB_OnsetDate AS date_diagnostic,
    diag.PROB_EndDate AS date_diagnostic_end,
    diag.PROB_CreateDate AS date_diagnostic_created_at,
    diag.PROB_UpdateDate AS date_diagnostic_updated_at,

    -- Statut du diagnostic
    diag.PROB_EntryStatus AS diagnostic_status,
    diag.PROB_Deleted AS diagnostic_deleted_flag,

    -- CIM10 principal
    MRC.MRCID_Code AS code_cim,
    MRC.MRCID_Code AS cancerdiagnosiscode,
    REPLACE(MRC.MRCID_Code, '.', '') AS code_cim_sans_point,
    SUBSTR(REPLACE(MRC.MRCID_Code, '.', ''), 1, 3) AS code_cim_3,
    MRC.MRCID_Desc AS libelle_cim,
    MRC.MRCID_CreatedDate AS cim_created_at,
    MRC.MRCID_UpdatedDate AS cim_updated_at,
    MRC.MRCID_DateActiveFrom AS cim_active_from,
    MRC.MRCID_DateActiveTo AS cim_active_to,

    -- Morphologie
    MRC_MORPH.MRCID_Code AS code_morphologique_source,
    SUBSTR(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MRC_MORPH.MRCID_Code,
        'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),
        'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),
        'U',''),'V',''),'W',''),'X',''),'Y',''),'Z',''),
        1,
        4
    ) AS code_morph_4,
    SUBSTR(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MRC_MORPH.MRCID_Code,
        'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),
        'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),
        'U',''),'V',''),'W',''),'X',''),'Y',''),'Z',''),
        1,
        4
    ) AS morphologygroup,
    SUBSTR(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MRC_MORPH.MRCID_Code,
        'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),
        'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),
        'U',''),'V',''),'W',''),'X',''),'Y',''),'Z',''),
        5,
        1
    ) AS code_morph_5,
    SUBSTR(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MRC_MORPH.MRCID_Code,
        'A',''),'B',''),'C',''),'D',''),'E',''),'F',''),'G',''),'H',''),'I',''),'J',''),
        'K',''),'L',''),'M',''),'N',''),'O',''),'P',''),'Q',''),'R',''),'S',''),'T',''),
        'U',''),'V',''),'W',''),'X',''),'Y',''),'Z',''),
        5,
        1
    ) AS morphologycode,
    MRC_MORPH.MRCID_Desc AS libelle_morphologique,

    -- Latéralité / correction
    %EXTERNAL(diag.PROB_Laterality) AS lateralite,
    %EXTERNAL(diag.PROB_Laterality) AS laterality,
    diag.PROB_ReasonForCorrection_DR->ENTERR_Desc AS raison_correction,

    -- Ancienne colonne conservée si tu l'utilises déjà
    MRC_MORPH.MRCID_Desc AS code_morphologique,

    (COALESCE(STG.STAGE_TumourSize_DR->SIZE_TumourSize,'')
      || ' ' ||
      COALESCE(STG.STAGE_LymphNode_DR->NODE_Stage,'')
      || ' ' ||
      COALESCE(STG.STAGE_Metastasis_DR->META_Stage,'')) AS tnm_code,

    STG.STAGE_CancerType_DR->CANT_Desc AS cancer_type,
    STG.STAGE_CancerSite_DR->BODP_Desc AS cancer_site,
    STG.STAGE_TumourSize_DR->SIZE_TumourSize AS t_stage_code,
    STG.STAGE_TumourSize_DR->SIZE_Desc AS t_stage_desc,
    STG.STAGE_TAfterPath AS stage_t_after_path,
    STG.STAGE_TAfterAdjuv AS stage_t_after_adjuv,
    STG.STAGE_TRecurrent AS stage_t_recurrent,

    STG.STAGE_LymphNode_DR->NODE_Stage AS n_stage_code,
    STG.STAGE_LymphNode_DR->NODE_Desc AS n_stage_desc,
    STG.STAGE_NAfterPath AS stage_n_after_path,
    STG.STAGE_NAfterAdjuv AS stage_n_after_adjuv,
    STG.STAGE_NRecurrent AS stage_n_recurrent,

    STG.STAGE_Metastasis_DR->META_Stage AS m_stage_code,
    STG.STAGE_Metastasis_DR->META_Desc AS m_stage_desc,
    STG.STAGE_MAfterPath AS stage_m_after_path,
    STG.STAGE_MAfterAdjuv AS stage_m_after_adjuv,
    STG.STAGE_MRecurrent AS stage_m_recurrent,
    STG.STAGE_StageDate AS stage_date

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Problem diag ON diag.PROB_ParRef = pat.PAPMI_RowId1
INNER JOIN SQLUser.MRC_ICDDx MRC ON MRC.MRCID_RowId = diag.PROB_ICDCode_DR

LEFT JOIN SQLUser.PA_ProblemCancerStage STG ON STG.STAGE_ParRef = diag.PROB_RowId

LEFT JOIN SQLUser.MRC_ICDDx MRC_MORPH
    ON MRC_MORPH.MRCID_RowId = CAST(diag.PROB_Morphological_DR AS BIGINT)

LEFT JOIN SQLUser.PA_Adm adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN questionnaire.QIUCTEMDSP q ON q.QUESPAAdmDR = adm.PAADM_RowID

WHERE
    diag.PROB_MasterVersion_DR IS NULL
    AND (diag.PROB_EndDate >= CURRENT_DATE OR diag.PROB_EndDate IS NULL)
    AND (diag.PROB_Deleted = 'N' OR diag.PROB_Deleted IS NULL)
    AND (
        MRC.MRCID_Code IS NULL
        OR (
            SUBSTR(MRC.MRCID_Code, 1, 1) = 'C'
            OR (
                SUBSTR(MRC.MRCID_Code, 1, 1) = 'D'
                AND TO_NUMBER(SUBSTR(MRC.MRCID_Code, 2, 3)) <= 48
            )
        )
    )
