SELECT
    pat.PAPMI_No AS ipp_ocr,
    rdv.APPT_DateComp AS date_rdv,
    rdv.APPT_BookedDate AS date_booked,
    rdv.APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_Desc AS libelle_examen
FROM
    SQLUser.PA_PatMas AS pat
JOIN
    SQLUser.PA_Adm AS adm
    ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
JOIN
    SQLUser.RB_Appointment AS rdv
    ON rdv.APPT_Adm_DR = adm.PAADM_RowID
WHERE
    rdv.APPT_DateComp IS NOT NULL
