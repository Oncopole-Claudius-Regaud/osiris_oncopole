SELECT
  TRIM(pat.PAPMI_No)             AS ipp_ocr,
  per.PAPER_ResidentNumber       AS ipp_chu,
  CASE
    WHEN pat.PAPMI_Sex_DR = 2 THEN 'Masculin'
    WHEN pat.PAPMI_Sex_DR = 3 THEN 'Féminin'
    ELSE 'Indéterminé'
  END                            AS gender,
  pat.PAPMI_Deceased_Date        AS date_of_death,
  pat.PAPMI_Name                 AS nom,
  pat.PAPMI_Name2                AS prenom,
  per.PAPER_Dob                  AS date_of_birth,
  per.PAPER_FreeText3            AS birth_city
FROM SQLUser.PA_PatMas pat
JOIN SQLUser.PA_Person per
  ON per.PAPER_RowId = pat.PAPMI_RowId1
WHERE TRIM(pat.PAPMI_No) IS NOT NULL
  AND TRIM(pat.PAPMI_No) <> ''
  AND TRIM(pat.PAPMI_No) NOT LIKE '0%'
  AND EXISTS (
    SELECT 1
    FROM SQLUser.PA_ADM adm
    WHERE adm.PAADM_PAPMI_DR = pat.PAPMI_RowID
  )
