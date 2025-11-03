SELECT
  SQLUser.PA_PatMas.PAPMI_No              AS ipp_ocr,
  SQLUser.PA_Person.PAPER_ResidentNumber  AS ipp_chu,
  CASE
    WHEN SQLUser.PA_PatMas.PAPMI_Sex_DR = 2 THEN 'Masculin'
    WHEN SQLUser.PA_PatMas.PAPMI_Sex_DR = 3 THEN 'Féminin'
    ELSE 'Indéterminé'
  END                                     AS gender,
  SQLUser.PA_PatMas.PAPMI_Deceased_Date   AS date_of_death,
  SQLUser.PA_PatMas.PAPMI_Name            AS nom,
  SQLUser.PA_PatMas.PAPMI_Name2           AS prenom,
  SQLUser.PA_Person.PAPER_Dob             AS date_of_birth,
  SQLUser.PA_Person.PAPER_FreeText3       AS birth_city
FROM
  SQLUser.PA_PatMas,
  SQLUser.PA_Person
WHERE
  SQLUser.PA_Person.PAPER_RowId = SQLUser.PA_PatMas.PAPMI_RowId1
