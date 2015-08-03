SELECT DISTINCT
  v.variation_id as variation_id_2025_key,
  REPLACE(s.name, ",", "") as name_2019,
  concat(sgm.allele_1, "|", sgm.allele_2) as allele
FROM
  VAR_DB.sample_genotype_multiple_bp sgm INNER JOIN
  VAR_DB.sample s ON
    sgm.sample_id = s.sample_id AND
    s.display NOT IN ("LD", "UNDISPLAYABLE") RIGHT OUTER JOIN
  VAR_DB.variation v ON sgm.variation_id = v.variation_id
