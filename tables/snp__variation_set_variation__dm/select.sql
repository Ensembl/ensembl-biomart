SELECT
  v.variation_id as variation_id_2025_key,
  replace(vs.name, ",", "") as name_2077,
  vs.description as description_2077
FROM
  VAR_DB.variation_set vs INNER JOIN
  VAR_DB.variation_set_variation vsv ON vs.variation_set_id = vsv.variation_set_id RIGHT OUTER JOIN
  VAR_DB.variation v ON vsv.variation_id = v.variation_id
