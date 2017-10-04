SELECT
  t.transcript_id as transcript_id_1064_key,
  t.stable_id as stable_id_1066,
  tv.cdna_start as cdna_start_2076,
  tv.cds_start as cds_start_2076,
  tv.cds_end as cds_end_2076,
  tv.translation_start as translation_start_2076,
  tv.consequence_types as consequence_types_2076,
  tv.distance_to_transcript as distance_to_transcript_2076,
  tv.allele_string as allele_string_2076,
  tv.pep_allele_string as pep_allele_string_2076,
  tv.polyphen_score as polyphen_score_2076,
  tv.polyphen_prediction as polyphen_prediction_2076,
  tv.sift_score as sift_score_2076,
  tv.sift_prediction as sift_prediction_2076,
  vf.allele_string as allele_string_2026,
  vf.minor_allele_count as minor_allele_count_2025,
  vf.minor_allele_freq as minor_allele_freq_2025,
  vf.minor_allele as minor_allele_2025,
  vf.seq_region_end as seq_region_end_2026,
  vf.seq_region_start as seq_region_start_2026,
  vf.seq_region_strand as seq_region_strand_2026,
  vf.map_weight as map_weight_2026,
  vf.variation_name as name_2025,
  vf.clinical_significance as clinical_significance_2025,
  s.description as description_2021,
  s.name as name_2021,
  e.evidence as evidence_2025
FROM
  VAR_DB.MTMP_transcript_variation tv INNER JOIN
  VAR_DB.variation_feature vf ON tv.variation_feature_id = vf.variation_feature_id and (vf.somatic=1 and vf.display=1) INNER JOIN
  VAR_DB.source s ON vf.source_id = s.source_id LEFT JOIN
  VAR_DB.MTMP_evidence as e ON vf.variation_id = e.variation_id RIGHT JOIN
  CORE_DB.transcript t ON t.stable_id = tv.feature_stable_id