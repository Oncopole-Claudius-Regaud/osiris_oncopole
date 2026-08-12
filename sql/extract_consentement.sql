SELECT
  TRIM(SQLUser.PA_PatMas.PAPMI_No) AS ipp_ocr,
  (
    SELECT questionnaire.QFRXXCONS.Q07
    FROM questionnaire.QFRXXCONS
    WHERE SQLUser.PA_PatMas.PAPMI_rowid1 = questionnaire.QFRXXCONS.QUESPAPatMasDR
      AND questionnaire.QFRXXCONS.Q04 = 4
      AND questionnaire.QFRXXCONS.ID = (
        SELECT MAX(Q2.ID)
        FROM questionnaire.QFRXXCONS Q2
        WHERE questionnaire.QFRXXCONS.QUESPAPatMasDR = Q2.QUESPAPatMasDR
          AND Q2.Q04 = 4
      )
  ) AS consentement,
  (
    SELECT questionnaire.QFRXXCONS.QUESDate
    FROM questionnaire.QFRXXCONS
    WHERE SQLUser.PA_PatMas.PAPMI_rowid1 = questionnaire.QFRXXCONS.QUESPAPatMasDR
      AND questionnaire.QFRXXCONS.Q04 = 4
      AND questionnaire.QFRXXCONS.ID = (
        SELECT MAX(Q2.ID)
        FROM questionnaire.QFRXXCONS Q2
        WHERE questionnaire.QFRXXCONS.QUESPAPatMasDR = Q2.QUESPAPatMasDR
          AND Q2.Q04 = 4
      )
  ) AS date_consentement
FROM SQLUser.PA_PatMas
WHERE TRIM(SQLUser.PA_PatMas.PAPMI_No) IS NOT NULL
  AND TRIM(SQLUser.PA_PatMas.PAPMI_No) <> ''
