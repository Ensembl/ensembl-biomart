SELECT
  svf.structural_variation_feature_id as structural_variation_feature_id_20104_key,
  sv_m.structural_variation_id_2072_key,
  sv_m.variation_name_2072,
  sv_m.validation_status_2072,
  sv_m.clinical_significance_2072,
  sv_m.value_2092,
  sv_m.name_2021,
  sv_m.description_2021,
  sv_m.name_20100,
  sv_m.description_20100,
  sv_m.external_reference_20100,
  sv_m.structural_variation_feature_count,
  sr.name as name_2034,
  svf.outer_start as outer_start_20104,
  svf.seq_region_start as seq_region_start_20104,
  svf.inner_start as inner_start_20104,
  svf.inner_end as inner_end_20104,
  svf.seq_region_end as seq_region_end_20104,
  svf.outer_end as outer_end_20104,
  svf.seq_region_strand as seq_region_strand_20104,
  svf.allele_string as allele_string_20104,
  svf.breakpoint_order as breakpoint_order_20104
FROM
  SPECIES_ABBREV_structvar__structural_variation__main sv_m LEFT OUTER JOIN
  VAR_DB.structural_variation_feature svf ON sv_m.structural_variation_id_2072_key = svf.structural_variation_id LEFT OUTER JOIN
  VAR_DB.seq_region sr ON svf.seq_region_id = sr.seq_region_id  
