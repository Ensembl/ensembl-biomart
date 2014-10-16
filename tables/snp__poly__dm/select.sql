SELECT DISTINCT
  v.variation_id as variation_id_2025_key,
  i.name as name_2019,
  concat(sg.allele_1, "|", sg.allele_2) as allele
FROM
  VAR_DB.MTMP_sample_genotype sg INNER JOIN
  VAR_DB.individual i ON
    sg.individual_id = i.individual_id AND
    i.display NOT IN ("LD", "UNDISPLAYABLE") RIGHT OUTER JOIN
  VAR_DB.variation v ON sg.variation_id = v.variation_id
