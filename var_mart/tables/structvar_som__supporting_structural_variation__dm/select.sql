SELECT
  sv.structural_variation_id as structural_variation_id_2072_key,
  ssv.supporting_structural_variation_id as supporting_structural_variation_id_20116,
  REPLACE(ssv.variation_name, ",", "") as variation_name_20116,
  ssv.class_name as class_name_20116,
  ssv.seq_region_name as seq_region_name_20116,
  ssv.outer_start as outer_start_20116,
  ssv.seq_region_start as seq_region_start_20116,
  ssv.inner_start as inner_start_20116,
  ssv.inner_end as inner_end_20116,
  ssv.seq_region_end as seq_region_end_20116,
  ssv.outer_end as outer_end_20116,
  ssv.seq_region_strand as seq_region_strand_20116,
  ssv.clinical_significance as clinical_significance_20116,
  ssv.copy_number as copy_number_20116,
  REPLACE(ssv.sample_name, ",", "") as sample_name_20116
FROM
  VAR_DB.structural_variation sv LEFT OUTER JOIN
  VAR_DB.MTMP_supporting_structural_variation ssv ON sv.structural_variation_id = ssv.structural_variation_id
