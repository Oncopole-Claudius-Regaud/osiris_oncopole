SELECT
  pat.PAPMI_No                                      AS ipp_ocr,
  per.PAPER_ResidentNumber                          AS ipp_chu,
  prob.PROB_OnsetDate                               AS date_diagnostic,
  CASE WHEN pcs.STAGE_TumourSize_DR->SIZE_TumourSize IS NULL THEN '' ELSE pcs.STAGE_TumourSize_DR->SIZE_TumourSize END
    || ' ' ||
  CASE WHEN pcs.STAGE_LymphNode_DR->NODE_Stage IS NULL THEN '' ELSE pcs.STAGE_LymphNode_DR->NODE_Stage END
    || ' ' ||
  CASE WHEN pcs.STAGE_Metastasis_DR->META_Stage IS NULL THEN '' ELSE pcs.STAGE_Metastasis_DR->META_Stage END
                                                    AS description_tnm,
  pcs.STAGE_CancerType_DR->CANT_Desc                AS cancer_type_desc,
  pcs.STAGE_CancerSite_DR->BODP_Desc                AS cancer_site_desc,
  pcs.STAGE_TumourSize_DR->SIZE_TumourSize          AS tumour_size,
  pcs.STAGE_TumourSize_DR->SIZE_Desc                AS tumour_size_desc,
  pcs.STAGE_TAfterPath                              AS t_after_path,
  pcs.STAGE_TAfterAdjuv                             AS t_after_adjuv,
  pcs.STAGE_TRecurrent                              AS t_recurrent,
  pcs.STAGE_LymphNode_DR->NODE_Stage                AS node_stage,
  pcs.STAGE_LymphNode_DR->NODE_Desc                 AS node_desc,
  pcs.STAGE_NAfterPath                              AS n_after_path,
  pcs.STAGE_NAfterAdjuv                             AS n_after_adjuv,
  pcs.STAGE_NRecurrent                              AS n_recurrent,
  pcs.STAGE_Metastasis_DR->META_Stage               AS meta_stage,
  pcs.STAGE_Metastasis_DR->META_Desc                AS meta_desc,
  pcs.STAGE_MAfterPath                              AS m_after_path,
  pcs.STAGE_MAfterAdjuv                             AS m_after_adjuv,
  pcs.STAGE_MRecurrent                              AS m_recurrent,
  pcs.STAGE_StageDate                               AS stage_date
FROM
  SQLUser.PA_PatMas pat,
  SQLUser.PA_Person per,
  SQLUser.PA_Problem prob,
  SQLUser.PA_ProblemCancerStage pcs
WHERE
  per.PAPER_RowId = pat.PAPMI_RowId1
  AND pat.PAPMI_RowId1 =* prob.PROB_ParRef
  AND pcs.STAGE_ParRef = prob.PROB_RowId
