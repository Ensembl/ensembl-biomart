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

create table VAR_MART_DB.TEMP7 as select a.source_id as source_id_2021,a.description as description_2021,a.name as name_2021,a.version as version_2021,a.type as type_2021 from VAR_DB.source as a where a.source_id=SRC_ID;
create index I_11 on VAR_MART_DB.TEMP7(source_id_2021);
create table VAR_MART_DB.TEMP8 as select a.*,b.subsnp_id as subsnp_id_2030,b.name as name_2030,b.moltype as moltype_2030,b.variation_id as variation_id_2025_key,b.variation_synonym_id as variation_synonym_id_2030 from VAR_MART_DB.TEMP7 as a left join VAR_DB.variation_synonym as b on a.source_id_2021=b.source_id where VAR_ID_COND_b
drop table VAR_MART_DB.TEMP7;
create index I_12 on VAR_MART_DB.TEMP8(variation_id_2025_key);
create table VAR_MART_DB.TEMP10 as select a.* from VAR_MART_DB.TEMP8 as a inner join VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation__main as b on a.variation_id_2025_key=b.variation_id_2025_key;
drop table VAR_MART_DB.TEMP8;
create index I_13 on VAR_MART_DB.TEMP10(variation_id_2025_key);
create table VAR_MART_DB.TEMP11 as select a.variation_id_2025_key,b.description_2021,b.source_id_2021,b.version_2021,b.name_2021,b.type_2021,b.subsnp_id_2030,b.moltype_2030,b.variation_synonym_id_2030,b.name_2030 from VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation__main as a left join VAR_MART_DB.TEMP10 as b on a.variation_id_2025_key=b.variation_id_2025_key;
drop table VAR_MART_DB.TEMP10;
alter table VAR_MART_DB.TEMP11 drop column subsnp_id_2030;
create index I_14 on VAR_MART_DB.TEMP11(name_2021);
create index I_15 on VAR_MART_DB.TEMP11(name_2030);
rename table VAR_MART_DB.TEMP11 to VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation_synonym_SRC_NAME__dm;
create index I_16 on VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation_synonym_SRC_NAME__dm(variation_id_2025_key);
alter table VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation__main add column (variation_synonym_SRC_NAME_bool integer default 0);
update VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation__main a set variation_synonym_SRC_NAME_bool=(select case count(1) when 0 then null else 1 end from VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation_synonym_SRC_NAME__dm b where a.variation_id_2025_key=b.variation_id_2025_key and not (b.description_2021 is null and b.source_id_2021 is null and b.version_2021 is null and b.name_2021 is null and b.type_2021 is null and b.moltype_2030 is null and b.variation_synonym_id_2030 is null and b.name_2030 is null));
create index I_17 on VAR_MART_DB.SPECIES_ABBREV_eg_snp__variation__main(variation_synonym_SRC_NAME_bool);
