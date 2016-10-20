SELECT
  vf.variation_feature_id as variation_feature_id_2026_key,
  mfv.feature_stable_id as feature_stable_id_20125,
  mfv.allele_string as allele_string_20125,
  mfv.motif_name as motif_name_20125,
  mfv.motif_start as motif_start_20125,
  mfv.motif_score_delta as motif_score_delta_20125,
  mfv.consequence_types as consequence_types_20125,
  mfv.in_informative_position as in_informative_position_20125
FROM
  VAR_DB.MTMP_motif_feature_variation mfv RIGHT OUTER JOIN
  VAR_DB.variation_feature vf ON mfv.variation_feature_id = vf.variation_feature_id
