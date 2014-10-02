SELECT DISTINCT
  v.variation_id as variation_id_2025_key,
  concat(igs.allele_1, "|", igs.allele_2) as allele,
  i.individual_id as individual_id_2023,
  i.name as name_2019
FROM
  VAR_DB.tmp_individual_genotype_single_bp igs INNER JOIN
  VAR_DB.individual i ON
    igs.individual_id = i.individual_id AND
    i.display IN ("REFERENCE","DEFAULT","DISPLAYABLE","MARTDISPLAYABLE") RIGHT OUTER JOIN
  VAR_DB.variation v ON igs.variation_id = v.variation_id
