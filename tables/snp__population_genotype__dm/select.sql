SELECT
  v.variation_id as variation_id_2025_key,
  p.name as name_2019,
  concat(pg.allele_1, "|", pg.allele_2) as allele,
  p.size as size_2019,
  pg.frequency as frequency_2016
FROM
  VAR_DB.MTMP_population_genotype pg INNER JOIN
  VAR_DB.population p ON pg.population_id = p.population_id RIGHT OUTER JOIN
  VAR_DB.variation v ON pg.variation_id = v.variation_id
