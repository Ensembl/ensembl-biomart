SELECT
  vf.variation_feature_id as variation_feature_id_2026_key,
  g.stable_id as stable_id_1023,
  t.stable_id as stable_id_1066,
  t.biotype as biotype_1064,
  t.seq_region_strand as seq_region_strand_1064,
  tv.cdna_start as cdna_start_2090,
  tv.cdna_end as cdna_end_2090,
  tv.cds_start as cds_start_2090,
  tv.cds_end as cds_end_2090,
  tv.translation_start as translation_start_2090,
  tv.translation_end as translation_end_2090,
  tv.consequence_types as consequence_types_2090,
  tv.distance_to_transcript as distance_to_transcript_2090,
  tv.allele_string as allele_string_2090,
  tv.pep_allele_string as pep_allele_string_2090,
  tv.polyphen_score as polyphen_score_2090,
  tv.polyphen_prediction as polyphen_prediction_2090,
  tv.sift_score as sift_score_2090,
  tv.sift_prediction as sift_prediction_2090
FROM
  CORE_DB.gene g INNER JOIN
  CORE_DB.transcript t ON g.gene_id = t.gene_id INNER JOIN
  VAR_DB.MTMP_transcript_variation tv ON t.stable_id = tv.feature_stable_id RIGHT OUTER JOIN
  VAR_DB.variation_feature vf ON tv.variation_feature_id = vf.variation_feature_id
