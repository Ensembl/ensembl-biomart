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

CREATE VIEW MTMP_supporting_structural_variation AS
SELECT
sv.structural_variation_id AS supporting_structural_variation_id,
sva.structural_variation_id,
sv.variation_name, a1.value AS class_name,
seq.name AS seq_region_name,
svf.outer_start,svf.seq_region_start,svf.inner_start,svf.inner_end,svf.seq_region_end,svf.outer_end,svf.seq_region_strand,
a2.value AS clinical_significance, s1.name AS sample_name, s2.name AS strain_name, p.description AS phenotype
FROM structural_variation sv,structural_variation_feature svf LEFT JOIN seq_region seq ON (svf.seq_region_id=seq.seq_region_id), attrib a1,
structural_variation_association sva LEFT JOIN structural_variation_annotation sa ON (sva.supporting_structural_variation_id=sa.structural_variation_id)
LEFT JOIN attrib a2 ON (a2.attrib_id=sa.clinical_attrib_id)
LEFT JOIN sample s1 ON (s1.sample_id=sa.sample_id)
LEFT JOIN sample s2 ON (s2.sample_id=sa.strain_id)
LEFT JOIN phenotype p ON (p.phenotype_id=sa.phenotype_id)
WHERE sv.structural_variation_id = sva.supporting_structural_variation_id
AND sva.supporting_structural_variation_id=svf.structural_variation_id
AND a1.attrib_id=sv.class_attrib_id;
