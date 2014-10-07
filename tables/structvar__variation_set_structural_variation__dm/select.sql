SELECT
  sv.structural_variation_id as structural_variation_id_2072_key,
  replace(vs.name, ',', '') as name_2077,
  vs.description as description_2077
FROM
  VAR_DB.structural_variation sv LEFT OUTER JOIN
  VAR_DB.variation_set_structural_variation vssv ON sv.structural_variation_id = vssv.structural_variation_id LEFT OUTER JOIN
  VAR_DB.variation_set vs ON vssv.variation_set_id = vs.variation_set_id
