WITH lignes_raw AS (
  SELECT
    inc.I0CLEUNIK AS treatment_line_id,
    pa.NOOBSPAT,
    inc.COMINCLUS AS treatment_comment,
    lt.LIGNETRAIT AS treatment_label,

    prot.NOMPROT AS protocol_name,
    prot.COMMENTPRO AS protocol_detail,
    prot.CATEG AS protocol_category,
    prot.TYPEPROT AS protocol_type,
    prot.CODELOCAL AS local_code,
    prot.VALIDPRO AS valid_protocol,

    rp.LIBELLE AS radiation,

    pr.NUMCYCLE,
    lp.DATEDEBADMR AS start_date_raw,
    lp.DATEFINADMR AS end_date_raw

  FROM CHIMIODATA.INCLUSIO inc
  JOIN CHIMIODATA.PATIENT pa ON pa.PACLEUNIK = inc.PACLEUNIK
  LEFT JOIN CHIMIODATA.LIGNETR lt ON lt.LTCLEUNIK = inc.LTCLEUNIK
  LEFT JOIN CHIMIODATA.PRESCRIP pr ON pr.I0CLEUNIK = inc.I0CLEUNIK
  LEFT JOIN CHIMIODATA.LIGNEPRE lp ON lp.P0CLEUNIK = pr.P0CLEUNIK
  LEFT JOIN CHIMIODATA.PROTOCOL prot ON prot.PRCLEUNIK = inc.PRCLEUNIK
  LEFT JOIN CHIMIODATA.RADIOPRO rp ON rp.RACLEUNIK = pr.RACLEUNIK
  WHERE pa.NOOBSPAT IN ({patient_list})
),

lignes_grouped AS (
  SELECT
    treatment_line_id,
    NOOBSPAT,
    treatment_comment,
    treatment_label,
    protocol_name,
    protocol_detail,
    protocol_category,
    protocol_type,
    local_code,
    valid_protocol,
    MAX(radiation) AS radiation,

    MIN(CASE WHEN LENGTH(TRIM(start_date_raw)) = 8 THEN TO_DATE(start_date_raw, 'YYYYMMDD') END) AS start_date,
    MAX(CASE WHEN LENGTH(TRIM(end_date_raw)) = 8 THEN TO_DATE(end_date_raw, 'YYYYMMDD') END) AS end_date,
    COUNT(DISTINCT NUMCYCLE) AS nb_cycles

  FROM lignes_raw
  GROUP BY
    treatment_line_id,
    NOOBSPAT,
    treatment_comment,
    treatment_label,
    protocol_name,
    protocol_detail,
    protocol_category,
    protocol_type,
    local_code,
    valid_protocol
),
numerotation AS (
  SELECT
    treatment_line_id,
    NOOBSPAT,
    treatment_comment,
    treatment_label,
    protocol_name,
    protocol_detail,
    protocol_category,
    protocol_type,
    local_code,
    valid_protocol,
    radiation,
    start_date,
    end_date,
    nb_cycles,
    ROW_NUMBER() OVER (
      PARTITION BY NOOBSPAT
      ORDER BY start_date NULLS LAST, end_date NULLS LAST
    ) AS treatment_line_number

  FROM lignes_grouped
)

SELECT *
FROM numerotation
ORDER BY NOOBSPAT, treatment_line_number
