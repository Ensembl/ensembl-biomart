SELECT
  v.variation_id as variation_id_2025_key,
  REPLACE(v.name, ",", "") as name_2025,
  v.minor_allele as minor_allele_2025,
  v.minor_allele_count as minor_allele_count_2025,
  v.minor_allele_freq as minor_allele_freq_2025,
  v.clinical_significance as clinical_significance_2025,
  REPLACE(s.name, ",", "") as name_2021,
  s.description as description_2021,
  e.evidence as evidence_2025
FROM
  VAR_DB.variation v LEFT OUTER JOIN
  VAR_DB.source s ON v.source_id = s.source_id LEFT OUTER JOIN
  VAR_DB.MTMP_evidence e ON v.variation_id  = e.variation_id 
