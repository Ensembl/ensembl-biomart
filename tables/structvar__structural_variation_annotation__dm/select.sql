SELECT
  sv.structural_variation_id as structural_variation_id_2072_key,
  svs.strain_id as strain_id_20138,
  i.name as name_2019,
  i.description as description_2019
FROM
  VAR_DB.structural_variation sv LEFT OUTER JOIN
  VAR_DB.structural_variation_sample svs ON sv.structural_variation_id = svs.structural_variation_id LEFT OUTER JOIN
  VAR_DB.individual i ON svs.strain_id = i.individual_id
