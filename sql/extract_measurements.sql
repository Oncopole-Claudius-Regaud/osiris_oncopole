SELECT
    pat.PAPMI_No AS ipp_ocr,
    MRC.MRCID_Code AS code_cim,
    obs.OBS_Date AS measure_date,
    TO_CHAR(obs.OBS_Time, 'HH24:MI:SS') AS measure_time,
    obs.OBS_UpdateDate AS obs_updated_at,
    itm.ITM_Desc AS measure_type,
    obs.OBS_Value AS measure_value
FROM SQLUser.MR_Observations obs
JOIN SQLUser.MRC_ObservationItem itm ON obs.OBS_Item_DR = itm.ITM_RowId
JOIN SQLUser.MR_ADM madr ON obs.OBS_ParRef = madr.MRADM_RowId
JOIN SQLUser.PA_ADM adm ON madr.MRADM_ADM_DR = adm.PAADM_RowID
JOIN SQLUser.PA_PATMAS pat ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.PA_Problem prob ON prob.PROB_ParRef = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MRC_ICDDx MRC ON prob.PROB_ICDCode_DR = MRC.MRCID_RowId
WHERE pat.PAPMI_No IN ({patient_list})
AND (
    itm.ITM_Desc IN (
        'POIDS', 'TAILLE', 'INDICEDEMASSECORPORELLE',
        'Score OMS / Karnofsky', 'KARNOFSKY', 'OMS'
    )
)
AND obs.OBS_Time > 0
ORDER BY pat.PAPMI_No, obs.OBS_Date ASC, itm.ITM_Desc
