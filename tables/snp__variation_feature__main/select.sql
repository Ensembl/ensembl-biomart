SELECT
  v_m.variation_id_2025_key,
  v_m.name_2025,
  v_m.ancestral_allele_2025,
  v_m.minor_allele_2025,
  v_m.minor_allele_count_2025,
  v_m.minor_allele_freq_2025,
  v_m.clinical_significance_2025,
  v_m.name_2021,
  v_m.description_2021,
  v_m.evidence_2025,
  v_m.variation_annotation_bool,
  v_m.variation_citation_bool,
  v_m.variation_feature_count,
  vf.variation_feature_id as variation_feature_id_2026_key,
  vf.variation_name as variation_name_2026,
  vf.variation_set_id as variation_set_id_2026,
  vf.seq_region_id as seq_region_id_2026,
  sr.name as name_1059,
  vf.seq_region_start as seq_region_start_2026,
  vf.seq_region_end as seq_region_end_2026,
  vf.seq_region_strand as seq_region_strand_2026,
  vf.allele_string as allele_string_2026,
  vf.map_weight as map_weight_2026
FROM
  SPECIES_ABBREV_snp__variation__main v_m LEFT OUTER JOIN
  VAR_DB.variation_feature vf ON v_m.variation_id_2025_key = vf.variation_id LEFT OUTER JOIN
  CORE_DB.seq_region sr on vf.seq_region_id = sr.seq_region_id
