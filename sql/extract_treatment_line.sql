WITH lignes_raw AS (
          SELECT
            inc.I0CLEUNIK AS treatment_line_id,
	    inc.codelocal as code_cim,
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
            lp.DOSEADM,
            lp.DATEDEBADMR AS start_date,
            lp.DATEFINADMR AS end_date,
            lp.ETAT AS etat_code,
            le.NUMETAPE AS numetape,
            le.LIBETAPE AS etat_label

          FROM CHIMIODATA.INCLUSIO inc
          JOIN CHIMIODATA.PATIENT pa ON pa.PACLEUNIK = inc.PACLEUNIK
          LEFT JOIN CHIMIODATA.LIGNETR lt ON lt.LTCLEUNIK = inc.LTCLEUNIK
          LEFT JOIN CHIMIODATA.PRESCRIP pr ON pr.I0CLEUNIK = inc.I0CLEUNIK
          LEFT JOIN CHIMIODATA.LIGNEPRE lp ON lp.P0CLEUNIK = pr.P0CLEUNIK
          LEFT JOIN CHIMIODATA.PROTOCOL prot ON prot.PRCLEUNIK = inc.PRCLEUNIK
          LEFT JOIN CHIMIODATA.RADIOPRO rp ON rp.RACLEUNIK = pr.RACLEUNIK
          LEFT JOIN CHIMIODATA.LIBETAPE le ON lp.ETAT = le.NUMETAPE
        )
        SELECT *
        FROM lignes_raw
