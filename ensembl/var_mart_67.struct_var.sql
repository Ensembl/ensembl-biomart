-- Copyright [2009-2014] EMBL-European Bioinformatics Institute
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE VAR_MART_DB.TEMP_SV1 SELECT sv.allele_string AS allele_string_2072,sv.structural_variation_id AS structural_variation_id_2072_key, sv.seq_region_id AS seq_region_id_2072, sv.seq_region_start AS seq_region_start_2072, sv.seq_region_end AS seq_region_end_2072, sv.seq_region_strand AS seq_region_strand_2072, sv.variation_name AS variation_name_2072, sv.class AS class_2072, sv.bound_start AS bound_start_2072, sv.bound_end AS bound_end_2072, sv.source_id AS source_id_2072, sr.name AS name_2034, s.name AS name_2021, s.description AS description_2021, s.version AS version_2021, s.somatic_status AS somatic_status_2021, sv.study_id AS study_id_2072, s.type AS type_2021 FROM VAR_DB.structural_variation sv, VAR_DB.source s, VAR_DB.seq_region sr WHERE sv.source_id = s.source_id AND sv.seq_region_id = sr.seq_region_id;
CREATE INDEX SV1_I1 ON VAR_MART_DB.TEMP_SV1(study_id_2072);
CREATE TABLE VAR_MART_DB.SPECIES_ABBREV_eg_structvar__structural_variation__main AS SELECT a.*, b.description AS description_20100, b.external_reference AS external_reference_20100, b.name AS name_20100, b.study_type AS study_type_20100, b.url AS url_20100, b.source_id as source_id_20100 FROM VAR_MART_DB.TEMP_SV1 a LEFT JOIN VAR_DB.study b ON a.study_id_2072 = b.study_id;
DROP TABLE VAR_MART_DB.TEMP_SV1;
CREATE INDEX I_78 on VAR_MART_DB.SPECIES_ABBREV_eg_structvar__structural_variation__main(structural_variation_id_2072_key);
