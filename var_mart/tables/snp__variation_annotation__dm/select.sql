SELECT
  v.variation_id AS variation_id_2025_key,
  a4.value AS variation_names_2035,
  a1.value AS associated_gene_2035,
  a2.value AS associated_variant_risk_allele_2035,
  a3.value AS p_value_2035,
  REPLACE(REPLACE(p.description, ",", ""), "&", "") as description_2033,
  p.name as name_2033,
  st.external_reference as external_reference_20100,
  st.study_type as study_type_20100,
  REPLACE(st.name, ",", "") as name_20100,
  st.description as description_20100,
  REPLACE(so.name, ",", "") as name_2021
FROM
  VAR_DB.variation v
  LEFT JOIN (
    VAR_DB.phenotype_feature pf
  ) ON v.name = pf.object_id AND pf.type = "Variation"
  LEFT JOIN (
        VAR_DB.phenotype_feature_attrib a1
        JOIN VAR_DB.attrib_type at1 ON a1.attrib_type_id = at1.attrib_type_id
  ) ON (a1.phenotype_feature_id = pf.phenotype_feature_id AND at1.code = "associated_gene")
  LEFT JOIN (
        VAR_DB.phenotype_feature_attrib a2
        JOIN VAR_DB.attrib_type at2 ON a2.attrib_type_id = at2.attrib_type_id
  ) ON (a2.phenotype_feature_id = pf.phenotype_feature_id AND at2.code = "risk_allele")
  LEFT JOIN (
        VAR_DB.phenotype_feature_attrib a3
        JOIN VAR_DB.attrib_type at3 ON a3.attrib_type_id = at3.attrib_type_id
  ) ON (a3.phenotype_feature_id = pf.phenotype_feature_id AND at3.code = "p_value")
  LEFT JOIN (
        VAR_DB.phenotype_feature_attrib a4
        JOIN VAR_DB.attrib_type at4 ON a4.attrib_type_id = at4.attrib_type_id
  ) ON (a4.phenotype_feature_id = pf.phenotype_feature_id AND at4.code = "variation_names") LEFT JOIN
  VAR_DB.phenotype p ON pf.phenotype_id = p.phenotype_id LEFT JOIN
  VAR_DB.source so ON pf.source_id = so.source_id LEFT JOIN
  VAR_DB.study st ON st.study_id = pf.study_id;