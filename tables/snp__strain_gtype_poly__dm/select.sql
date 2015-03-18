SELECT
  v.variation_id as variation_id_2025_key,
  REPLACE(sgp.sample_name, ",", "") as sample_name_2085
FROM
  VAR_DB.strain_gtype_poly sgp RIGHT OUTER JOIN
  VAR_DB.variation v ON sgp.variation_id = v.variation_id
