SELECT
    IPP AS ipp_ocr,
    DATE_ORDONNANCE AS date_ordonnance,
    TYPE_PRODUIT AS type_produit,
    LIBELLE_PRODUIT AS libelle_produit,
    POSOLOGIE AS posologie,
    VMP AS vmp,
    DUREE AS duree
FROM c3_xxxicr.ord_presc_med_v
WHERE IPP IS NOT NULL
