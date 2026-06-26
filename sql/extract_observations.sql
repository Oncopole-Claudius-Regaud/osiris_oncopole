SELECT
    TRIM(pat.PAPMI_No) AS ipp,
    obs.OBS_Date AS date_observation,
    obs.OBS_Time AS heure_observation,
    madr.MRADM_RowId AS source_admission_id,
    madr.MRADM_Date AS date_admission,
    itm.ITM_RowId AS item_id,
    itm.ITM_Code AS item_code,
    itm.ITM_Desc AS item_libelle,
    obs.OBS_Value AS valeur_brute,
    (
        SELECT MAX(lu.LU_Desc)
        FROM SQLUser.MRC_ObservationItemLookUp lu
        WHERE lu.LU_Code = obs.OBS_Value
          AND lu.LU_ParRef = itm.ITM_RowId
    ) AS valeur_libelle,
    CASE
        WHEN UPPER(itm.ITM_Desc) LIKE '%POIDS%' THEN 'POIDS'
        WHEN UPPER(itm.ITM_Desc) LIKE '%TAILLE%' THEN 'TAILLE'
        WHEN UPPER(itm.ITM_Desc) LIKE '%OMS%' THEN 'OMS'
        WHEN UPPER(itm.ITM_Desc) LIKE '%KARNOFSKY%' THEN 'KARNOFSKY'
        WHEN UPPER(itm.ITM_Desc) LIKE '%MASSE%CORPORELLE%' THEN 'IMC'
        ELSE 'AUTRE'
    END AS type_observation
FROM SQLUser.MR_Observations obs
JOIN SQLUser.MRC_ObservationItem itm
    ON itm.ITM_RowId = obs.OBS_Item_DR
JOIN SQLUser.MR_ADM madr
    ON obs.OBS_ParRef = madr.MRADM_RowId
JOIN SQLUser.PA_ADM adm
    ON madr.MRADM_ADM_DR = adm.PAADM_RowID
JOIN SQLUser.PA_PATMAS pat
    ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
WHERE obs.OBS_Time > 0
  AND TRIM(pat.PAPMI_No) IS NOT NULL
  AND TRIM(pat.PAPMI_No) <> ''
  AND TRIM(pat.PAPMI_No) <> '0200500024'
  AND (
      obs.OBS_Item_DR IN (
          SELECT gi.ITM_ObsItem_DR
          FROM SQLUser.MRC_ObservationGroupItems gi
          WHERE gi.ITM_ParRef IN (
              SELECT grp.GRP_RowId
              FROM SQLUser.MRC_ObservationGroup grp
              WHERE grp.GRP_Code = 'H/W'
          )
      )
      OR UPPER(itm.ITM_Desc) LIKE '%OMS%'
      OR UPPER(itm.ITM_Desc) LIKE '%KARNOFSKY%'
  )
ORDER BY pat.PAPMI_No, obs.OBS_Date, obs.OBS_Time, itm.ITM_Desc
