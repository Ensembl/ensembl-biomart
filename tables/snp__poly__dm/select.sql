SELECT DISTINCT
  v.variation_id as variation_id_2025_key,
  REPLACE(s.name, ",", "") as name_2019,
  concat(sg.allele_1, "|", sg.allele_2) as allele
FROM
  VAR_DB.MTMP_sample_genotype sg INNER JOIN
  VAR_DB.sample s ON
    sg.sample_id = s.sample_id AND
    s.display NOT IN ("LD", "UNDISPLAYABLE") RIGHT OUTER JOIN
  VAR_DB.variation v ON sg.variation_id = v.variation_id
