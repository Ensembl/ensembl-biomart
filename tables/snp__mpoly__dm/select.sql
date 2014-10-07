SELECT DISTINCT
  v.variation_id as variation_id_2025_key,
  i.name as name_2019,
  concat(igm.allele_1, "|", igm.allele_2) as allele
FROM
  VAR_DB.individual_genotype_multiple_bp igm INNER JOIN
  VAR_DB.individual i ON
    igm.individual_id = i.individual_id AND
    i.display IN ("REFERENCE","DEFAULT","DISPLAYABLE","MARTDISPLAYABLE") RIGHT OUTER JOIN
  VAR_DB.variation v ON igm.variation_id = v.variation_id
