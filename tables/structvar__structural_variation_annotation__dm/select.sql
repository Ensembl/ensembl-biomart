SELECT
  sv.structural_variation_id as structural_variation_id_2072_key,
  REPLACE(s.name, ",", "") as name_2019,
  s.description as description_2019
FROM
  VAR_DB.structural_variation sv LEFT OUTER JOIN
  VAR_DB.structural_variation_sample svs ON sv.structural_variation_id = svs.structural_variation_id LEFT OUTER JOIN
  VAR_DB.sample s ON svs.sample_id = s.sample_id
