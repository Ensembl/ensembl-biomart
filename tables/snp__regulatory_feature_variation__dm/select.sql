SELECT
  vf.variation_feature_id as variation_feature_id_2026_key,
  rfv.feature_stable_id as feature_stable_id_20126,
  rfv.allele_string as allele_string_20126,
  rfv.consequence_types as consequence_types_20126
FROM
  VAR_DB.MTMP_regulatory_feature_variation rfv RIGHT OUTER JOIN
  VAR_DB.variation_feature__main vf ON rfv.variation_feature_id = vf.variation_feature_id