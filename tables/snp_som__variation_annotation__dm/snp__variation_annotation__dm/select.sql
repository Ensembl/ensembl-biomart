SELECT
  v.variation_id as variation_id_2025_key,
  va.variation_names as variation_names_2035,
  va.associated_gene as associated_gene_2035,
  va.risk_allele as associated_variant_risk_allele_2035,
  va.p_value as p_value_2035,
  REPLACE(REPLACE(p.description, ",", ""), "&", "") as description_2033,
  st.external_reference as external_reference_20100,
  st.study_type as study_type_20100,
  st.description as description_20100,
  p.stable_id    as stable_id_2033,
  REPLACE(so.name, ",", "") as name_2021
FROM
  VAR_DB.MTMP_variation_annotation va INNER JOIN
  VAR_DB.phenotype p ON va.phenotype_id = p.phenotype_id LEFT OUTER JOIN
  VAR_DB.study st ON va.study_id = st.study_id LEFT OUTER JOIN
  VAR_DB.source so ON st.source_id = so.source_id RIGHT OUTER JOIN
  VAR_DB.variation v ON va.variation_id = v.variation_id
