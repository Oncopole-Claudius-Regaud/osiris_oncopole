SELECT
    SQLUser.PA_PatMas.PAPMI_No AS ipp_ocr,
    SQLUser.RB_Appointment.APPT_BookedDate AS contact_date
FROM
    SQLUser.PA_PatMas,
    SQLUser.RB_Appointment,
    SQLUser.PA_Adm
WHERE
    SQLUser.PA_Adm.PAADM_PAPMI_DR = SQLUser.PA_PatMas.PAPMI_RowId1
    AND SQLUser.RB_Appointment.APPT_Adm_DR = SQLUser.PA_Adm.PAADM_RowID
    AND SQLUser.RB_Appointment.APPT_BookedDate IS NOT NULL
