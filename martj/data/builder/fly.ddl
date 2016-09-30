
--
--       TRANSFORMATION NO 1      TARGET TABLE: FLY__GENE__MAIN
--
CREATE TABLE fmart.MTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.cvterm_id,cvterm.cv_id,cvterm.name AS name_MTEMP0,cvterm.definition,cvterm.dbxref_id AS dbxref_id_MTEMP0 FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='gene';
CREATE INDEX index12 ON fmart.MTEMP0 (feature_id);
CREATE TABLE fmart.MTEMP1  AS SELECT MTEMP0.feature_id,MTEMP0.dbxref_id,MTEMP0.organism_id,MTEMP0.name,MTEMP0.uniquename,MTEMP0.residues,MTEMP0.seqlen,MTEMP0.md5checksum,MTEMP0.type_id,MTEMP0.is_analysis,MTEMP0.timeaccessioned,MTEMP0.timelastmodified,MTEMP0.cvterm_id,MTEMP0.cv_id,MTEMP0.name_MTEMP0,MTEMP0.definition,MTEMP0.dbxref_id_MTEMP0,featureloc.fmin AS gene_start,featureloc.fmax AS gene_end,featureloc.strand,featureloc.srcfeature_id,featureloc.rank FROM fmart.MTEMP0 , featureloc WHERE featureloc.feature_id = fmart.MTEMP0.feature_id;
CREATE INDEX index13 ON fmart.MTEMP1 (srcfeature_id);
CREATE TABLE fmart.MTEMP2  AS SELECT MTEMP1.feature_id,MTEMP1.dbxref_id,MTEMP1.organism_id,MTEMP1.name,MTEMP1.uniquename,MTEMP1.residues,MTEMP1.seqlen,MTEMP1.md5checksum,MTEMP1.type_id,MTEMP1.is_analysis,MTEMP1.timeaccessioned,MTEMP1.timelastmodified,MTEMP1.cvterm_id,MTEMP1.cv_id,MTEMP1.name_MTEMP0,MTEMP1.definition,MTEMP1.dbxref_id_MTEMP0,MTEMP1.gene_start,MTEMP1.gene_end,MTEMP1.strand,MTEMP1.srcfeature_id,MTEMP1.rank,feature.name AS chromosome_acc,feature.uniquename AS chromosome FROM fmart.MTEMP1 , feature WHERE feature.feature_id = fmart.MTEMP1.srcfeature_id;
CREATE INDEX index14 ON fmart.MTEMP2 (organism_id);
CREATE TABLE fmart.main_interim  AS SELECT MTEMP2.feature_id,MTEMP2.dbxref_id,MTEMP2.organism_id,MTEMP2.name,MTEMP2.uniquename,MTEMP2.residues,MTEMP2.seqlen,MTEMP2.md5checksum,MTEMP2.type_id,MTEMP2.is_analysis,MTEMP2.timeaccessioned,MTEMP2.timelastmodified,MTEMP2.cvterm_id,MTEMP2.cv_id,MTEMP2.name_MTEMP0,MTEMP2.definition,MTEMP2.dbxref_id_MTEMP0,MTEMP2.gene_start,MTEMP2.gene_end,MTEMP2.strand,MTEMP2.srcfeature_id,MTEMP2.rank,MTEMP2.chromosome_acc,MTEMP2.chromosome,organism.organism_id AS organism_id_MTEMP3,organism.abbreviation,organism.genus,organism.species,organism.common_name,organism.comment FROM fmart.MTEMP2 , organism WHERE organism.organism_id = fmart.MTEMP2.organism_id;
DROP TABLE fmart.MTEMP0;
DROP TABLE fmart.MTEMP1;
DROP TABLE fmart.MTEMP2;


--
--       TRANSFORMATION NO 2      TARGET TABLE: FLY__DNA_MOTIF__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='DNA_motif';
CREATE INDEX index22 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index23 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__DNA_motif__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 3      TARGET TABLE: FLY__RNA_MOTIF__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='RNA_motif';
CREATE INDEX index32 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index33 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__RNA_motif__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 4      TARGET TABLE: FLY__ABERRATION_JUNCTION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='aberration_junction';
CREATE INDEX index42 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index43 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__aberration_junction__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 5      TARGET TABLE: FLY__ENHANCER__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='enhancer';
CREATE INDEX index52 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index53 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__enhancer__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 6      TARGET TABLE: FLY__INSERTION_SITE__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='insertion_site';
CREATE INDEX index62 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index63 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__insertion_site__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 7      TARGET TABLE: FLY__MRNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='mRNA';
CREATE INDEX index72 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index73 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__mRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 8      TARGET TABLE: FLY__NCRNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='ncRNA';
CREATE INDEX index82 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index83 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__ncRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 9      TARGET TABLE: FLY__POINT_MUTATION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='point_mutation';
CREATE INDEX index92 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index93 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__point_mutation__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 10      TARGET TABLE: FLY__PROTEIN_BINDING_SITE__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='protein_binding_site';
CREATE INDEX index102 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index103 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__protein_binding_site__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 11      TARGET TABLE: FLY__PSEUDOGENE__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='pseudogene';
CREATE INDEX index112 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index113 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__pseudogene__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 12      TARGET TABLE: FLY__RRNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='rRNA';
CREATE INDEX index122 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index123 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__rRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 13      TARGET TABLE: FLY__REGION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='region';
CREATE INDEX index132 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index133 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__region__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 14      TARGET TABLE: FLY__REGULATORY_REGION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='regulatory_region';
CREATE INDEX index142 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index143 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__regulatory_region__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 15      TARGET TABLE: FLY__REPEAT_REGION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='repeat_region';
CREATE INDEX index152 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index153 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__repeat_region__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 16      TARGET TABLE: FLY__RESCUE_FRAGMENT__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='rescue_fragment';
CREATE INDEX index162 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index163 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__rescue_fragment__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 17      TARGET TABLE: FLY__SEQUENCE_VARIANT__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='sequence_variant';
CREATE INDEX index172 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index173 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__sequence_variant__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 18      TARGET TABLE: FLY__SNRNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='snRNA';
CREATE INDEX index182 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index183 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__snRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 19      TARGET TABLE: FLY__SNORNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='snoRNA';
CREATE INDEX index192 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index193 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__snoRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 20      TARGET TABLE: FLY__TRNA__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.name AS type FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='tRNA';
CREATE INDEX index202 ON fmart.DTEMP0 (feature_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.feature_id,DTEMP0.dbxref_id,DTEMP0.organism_id,DTEMP0.name,DTEMP0.uniquename,DTEMP0.residues,DTEMP0.seqlen,DTEMP0.md5checksum,DTEMP0.type_id,DTEMP0.is_analysis,DTEMP0.timeaccessioned,DTEMP0.timelastmodified,DTEMP0.type,feature_relationship.object_id FROM fmart.DTEMP0 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP0.feature_id;
CREATE INDEX index203 ON fmart.DTEMP1 (object_id);
CREATE TABLE fmart.fly__tRNA__dm  AS SELECT DTEMP1.feature_id,DTEMP1.dbxref_id,DTEMP1.organism_id,DTEMP1.name,DTEMP1.uniquename,DTEMP1.residues,DTEMP1.seqlen,DTEMP1.md5checksum,DTEMP1.type_id,DTEMP1.is_analysis,DTEMP1.timeaccessioned,DTEMP1.timelastmodified,DTEMP1.type,DTEMP1.object_id,feature.feature_id AS feature_id_DTEMP2 FROM fmart.DTEMP1 , feature WHERE feature.feature_id = fmart.DTEMP1.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;


--
--       TRANSFORMATION NO 21      TARGET TABLE: FLY__GADFLY__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT dbxref.dbxref_id,dbxref.db_id,dbxref.accession,dbxref.version,dbxref.description,db.db_id AS db_id_DTEMP0,db.name,db.contact_id,db.description AS description_DTEMP0,db.urlprefix,db.url FROM dbxref , db WHERE db.db_id = dbxref.db_id AND db.name='Gadfly';
CREATE INDEX index212 ON fmart.DTEMP0 (dbxref_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.dbxref_id,DTEMP0.db_id,DTEMP0.accession,DTEMP0.version,DTEMP0.description,DTEMP0.db_id_DTEMP0,DTEMP0.name,DTEMP0.contact_id,DTEMP0.description_DTEMP0,DTEMP0.urlprefix,DTEMP0.url,feature_dbxref.feature_dbxref_id,feature_dbxref.feature_id,feature_dbxref.dbxref_id AS dbxref_id_DTEMP1,feature_dbxref.is_current FROM fmart.DTEMP0 , feature_dbxref WHERE feature_dbxref.dbxref_id = fmart.DTEMP0.dbxref_id;
CREATE INDEX index213 ON fmart.DTEMP1 (feature_id);
CREATE TABLE fmart.DTEMP2  AS SELECT DTEMP1.dbxref_id,DTEMP1.db_id,DTEMP1.accession,DTEMP1.version,DTEMP1.description,DTEMP1.db_id_DTEMP0,DTEMP1.name,DTEMP1.contact_id,DTEMP1.description_DTEMP0,DTEMP1.urlprefix,DTEMP1.url,DTEMP1.feature_dbxref_id,DTEMP1.feature_id,DTEMP1.dbxref_id_DTEMP1,DTEMP1.is_current,feature_relationship.feature_relationship_id,feature_relationship.subject_id,feature_relationship.object_id,feature_relationship.type_id,feature_relationship.rank FROM fmart.DTEMP1 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP1.feature_id;
CREATE INDEX index214 ON fmart.DTEMP2 (object_id);
CREATE TABLE fmart.fly__Gadfly__dm  AS SELECT DTEMP2.dbxref_id,DTEMP2.db_id,DTEMP2.accession,DTEMP2.version,DTEMP2.description,DTEMP2.db_id_DTEMP0,DTEMP2.name,DTEMP2.contact_id,DTEMP2.description_DTEMP0,DTEMP2.urlprefix,DTEMP2.url,DTEMP2.feature_dbxref_id,DTEMP2.feature_id,DTEMP2.dbxref_id_DTEMP1,DTEMP2.is_current,DTEMP2.feature_relationship_id,DTEMP2.subject_id,DTEMP2.object_id,DTEMP2.type_id,DTEMP2.rank,feature.feature_id AS feature_id_DTEMP3 FROM fmart.DTEMP2 , feature WHERE feature.feature_id = fmart.DTEMP2.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;
DROP TABLE fmart.DTEMP2;


--
--       TRANSFORMATION NO 22      TARGET TABLE: FLY__FLYBASE__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT dbxref.dbxref_id,dbxref.db_id,dbxref.accession,dbxref.version,dbxref.description,db.db_id AS db_id_DTEMP0,db.name,db.contact_id,db.description AS description_DTEMP0,db.urlprefix,db.url FROM dbxref , db WHERE db.db_id = dbxref.db_id AND db.name='FlyBase';
CREATE INDEX index222 ON fmart.DTEMP0 (dbxref_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.dbxref_id,DTEMP0.db_id,DTEMP0.accession,DTEMP0.version,DTEMP0.description,DTEMP0.db_id_DTEMP0,DTEMP0.name,DTEMP0.contact_id,DTEMP0.description_DTEMP0,DTEMP0.urlprefix,DTEMP0.url,feature_dbxref.feature_dbxref_id,feature_dbxref.feature_id,feature_dbxref.dbxref_id AS dbxref_id_DTEMP1,feature_dbxref.is_current FROM fmart.DTEMP0 , feature_dbxref WHERE feature_dbxref.dbxref_id = fmart.DTEMP0.dbxref_id;
CREATE INDEX index223 ON fmart.DTEMP1 (feature_id);
CREATE TABLE fmart.DTEMP2  AS SELECT DTEMP1.dbxref_id,DTEMP1.db_id,DTEMP1.accession,DTEMP1.version,DTEMP1.description,DTEMP1.db_id_DTEMP0,DTEMP1.name,DTEMP1.contact_id,DTEMP1.description_DTEMP0,DTEMP1.urlprefix,DTEMP1.url,DTEMP1.feature_dbxref_id,DTEMP1.feature_id,DTEMP1.dbxref_id_DTEMP1,DTEMP1.is_current,feature_relationship.feature_relationship_id,feature_relationship.subject_id,feature_relationship.object_id,feature_relationship.type_id,feature_relationship.rank FROM fmart.DTEMP1 , feature_relationship WHERE feature_relationship.subject_id = fmart.DTEMP1.feature_id;
CREATE INDEX index224 ON fmart.DTEMP2 (object_id);
CREATE TABLE fmart.fly__FlyBase__dm  AS SELECT DTEMP2.dbxref_id,DTEMP2.db_id,DTEMP2.accession,DTEMP2.version,DTEMP2.description,DTEMP2.db_id_DTEMP0,DTEMP2.name,DTEMP2.contact_id,DTEMP2.description_DTEMP0,DTEMP2.urlprefix,DTEMP2.url,DTEMP2.feature_dbxref_id,DTEMP2.feature_id,DTEMP2.dbxref_id_DTEMP1,DTEMP2.is_current,DTEMP2.feature_relationship_id,DTEMP2.subject_id,DTEMP2.object_id,DTEMP2.type_id,DTEMP2.rank,feature.feature_id AS feature_id_DTEMP3 FROM fmart.DTEMP2 , feature WHERE feature.feature_id = fmart.DTEMP2.object_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;
DROP TABLE fmart.DTEMP2;


--
--       TRANSFORMATION NO 23      TARGET TABLE: FLY__CELLULAR_COMPONENT__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT cvterm.cvterm_id,cvterm.cv_id,cvterm.name,cvterm.definition,cvterm.dbxref_id,cv.cv_id AS cv_id_DTEMP0 FROM cvterm , cv WHERE cv.cv_id = cvterm.cv_id AND cv.name='cellular_component';
CREATE INDEX index232 ON fmart.DTEMP0 (dbxref_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.cvterm_id,DTEMP0.cv_id,DTEMP0.name,DTEMP0.definition,DTEMP0.dbxref_id,DTEMP0.cv_id_DTEMP0,dbxref.accession FROM fmart.DTEMP0 , dbxref WHERE dbxref.dbxref_id = fmart.DTEMP0.dbxref_id;
CREATE INDEX index233 ON fmart.DTEMP1 (cvterm_id);
CREATE TABLE fmart.DTEMP2  AS SELECT DTEMP1.cvterm_id,DTEMP1.cv_id,DTEMP1.name,DTEMP1.definition,DTEMP1.dbxref_id,DTEMP1.cv_id_DTEMP0,DTEMP1.accession,feature_cvterm.feature_id FROM fmart.DTEMP1 , feature_cvterm WHERE feature_cvterm.cvterm_id = fmart.DTEMP1.cvterm_id;
CREATE INDEX index234 ON fmart.DTEMP2 (feature_id);
CREATE TABLE fmart.fly__cellular_component__dm  AS SELECT DTEMP2.cvterm_id,DTEMP2.cv_id,DTEMP2.name,DTEMP2.definition,DTEMP2.dbxref_id,DTEMP2.cv_id_DTEMP0,DTEMP2.accession,DTEMP2.feature_id,feature.feature_id AS feature_id_DTEMP3 FROM fmart.DTEMP2 , feature WHERE feature.feature_id = fmart.DTEMP2.feature_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;
DROP TABLE fmart.DTEMP2;


--
--       TRANSFORMATION NO 24      TARGET TABLE: FLY__MOLECULAR_FUNCTION__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT cvterm.cvterm_id,cvterm.cv_id,cvterm.name,cvterm.definition,cvterm.dbxref_id,cv.cv_id AS cv_id_DTEMP0 FROM cvterm , cv WHERE cv.cv_id = cvterm.cv_id AND cv.name='molecular_function';
CREATE INDEX index242 ON fmart.DTEMP0 (dbxref_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.cvterm_id,DTEMP0.cv_id,DTEMP0.name,DTEMP0.definition,DTEMP0.dbxref_id,DTEMP0.cv_id_DTEMP0,dbxref.accession FROM fmart.DTEMP0 , dbxref WHERE dbxref.dbxref_id = fmart.DTEMP0.dbxref_id;
CREATE INDEX index243 ON fmart.DTEMP1 (cvterm_id);
CREATE TABLE fmart.DTEMP2  AS SELECT DTEMP1.cvterm_id,DTEMP1.cv_id,DTEMP1.name,DTEMP1.definition,DTEMP1.dbxref_id,DTEMP1.cv_id_DTEMP0,DTEMP1.accession,feature_cvterm.feature_id FROM fmart.DTEMP1 , feature_cvterm WHERE feature_cvterm.cvterm_id = fmart.DTEMP1.cvterm_id;
CREATE INDEX index244 ON fmart.DTEMP2 (feature_id);
CREATE TABLE fmart.fly__molecular_function__dm  AS SELECT DTEMP2.cvterm_id,DTEMP2.cv_id,DTEMP2.name,DTEMP2.definition,DTEMP2.dbxref_id,DTEMP2.cv_id_DTEMP0,DTEMP2.accession,DTEMP2.feature_id,feature.feature_id AS feature_id_DTEMP3 FROM fmart.DTEMP2 , feature WHERE feature.feature_id = fmart.DTEMP2.feature_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;
DROP TABLE fmart.DTEMP2;


--
--       TRANSFORMATION NO 25      TARGET TABLE: FLY__BIOLOGICAL_PROCESS__DM
--
CREATE TABLE fmart.DTEMP0  AS SELECT cvterm.cvterm_id,cvterm.cv_id,cvterm.name,cvterm.definition,cvterm.dbxref_id,cv.cv_id AS cv_id_DTEMP0 FROM cvterm , cv WHERE cv.cv_id = cvterm.cv_id AND cv.name='biological_process';
CREATE INDEX index252 ON fmart.DTEMP0 (dbxref_id);
CREATE TABLE fmart.DTEMP1  AS SELECT DTEMP0.cvterm_id,DTEMP0.cv_id,DTEMP0.name,DTEMP0.definition,DTEMP0.dbxref_id,DTEMP0.cv_id_DTEMP0,dbxref.accession FROM fmart.DTEMP0 , dbxref WHERE dbxref.dbxref_id = fmart.DTEMP0.dbxref_id;
CREATE INDEX index253 ON fmart.DTEMP1 (cvterm_id);
CREATE TABLE fmart.DTEMP2  AS SELECT DTEMP1.cvterm_id,DTEMP1.cv_id,DTEMP1.name,DTEMP1.definition,DTEMP1.dbxref_id,DTEMP1.cv_id_DTEMP0,DTEMP1.accession,feature_cvterm.feature_id FROM fmart.DTEMP1 , feature_cvterm WHERE feature_cvterm.cvterm_id = fmart.DTEMP1.cvterm_id;
CREATE INDEX index254 ON fmart.DTEMP2 (feature_id);
CREATE TABLE fmart.fly__biological_process__dm  AS SELECT DTEMP2.cvterm_id,DTEMP2.cv_id,DTEMP2.name,DTEMP2.definition,DTEMP2.dbxref_id,DTEMP2.cv_id_DTEMP0,DTEMP2.accession,DTEMP2.feature_id,feature.feature_id AS feature_id_DTEMP3 FROM fmart.DTEMP2 , feature WHERE feature.feature_id = fmart.DTEMP2.feature_id;
DROP TABLE fmart.DTEMP0;
DROP TABLE fmart.DTEMP1;
DROP TABLE fmart.DTEMP2;


--
--       TRANSFORMATION NO null      TARGET TABLE: FLY__GENE__MAIN
--
CREATE TABLE fmart.CTEMP0 AS SELECT DISTINCT object_id FROM fmart.fly__DNA_motif__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index261 ON fmart.CTEMP0 (object_id);
CREATE INDEX index262 ON fmart.main_interim (feature_id);
CREATE TABLE fmart.CTEMP1  AS SELECT main_interim.feature_id,main_interim.dbxref_id,main_interim.organism_id,main_interim.name,main_interim.uniquename,main_interim.residues,main_interim.seqlen,main_interim.md5checksum,main_interim.type_id,main_interim.is_analysis,main_interim.timeaccessioned,main_interim.timelastmodified,main_interim.cvterm_id,main_interim.cv_id,main_interim.name_MTEMP0,main_interim.definition,main_interim.dbxref_id_MTEMP0,main_interim.gene_start,main_interim.gene_end,main_interim.strand,main_interim.srcfeature_id,main_interim.rank,main_interim.chromosome_acc,main_interim.chromosome,main_interim.organism_id_MTEMP3,main_interim.abbreviation,main_interim.genus,main_interim.species,main_interim.common_name,main_interim.comment,CTEMP0.object_id AS DNA_motif_bool FROM fmart.main_interim LEFT JOIN fmart.CTEMP0 ON fmart.CTEMP0.object_id = fmart.main_interim.feature_id;
CREATE TABLE fmart.CTEMP2 AS SELECT DISTINCT object_id FROM fmart.fly__RNA_motif__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index263 ON fmart.CTEMP2 (object_id);
CREATE INDEX index264 ON fmart.CTEMP1 (feature_id);
CREATE TABLE fmart.CTEMP3  AS SELECT CTEMP1.feature_id,CTEMP1.dbxref_id,CTEMP1.organism_id,CTEMP1.name,CTEMP1.uniquename,CTEMP1.residues,CTEMP1.seqlen,CTEMP1.md5checksum,CTEMP1.type_id,CTEMP1.is_analysis,CTEMP1.timeaccessioned,CTEMP1.timelastmodified,CTEMP1.cvterm_id,CTEMP1.cv_id,CTEMP1.name_MTEMP0,CTEMP1.definition,CTEMP1.dbxref_id_MTEMP0,CTEMP1.gene_start,CTEMP1.gene_end,CTEMP1.strand,CTEMP1.srcfeature_id,CTEMP1.rank,CTEMP1.chromosome_acc,CTEMP1.chromosome,CTEMP1.organism_id_MTEMP3,CTEMP1.abbreviation,CTEMP1.genus,CTEMP1.species,CTEMP1.common_name,CTEMP1.comment,CTEMP1.DNA_motif_bool,CTEMP2.object_id AS RNA_motif_bool FROM fmart.CTEMP1 LEFT JOIN fmart.CTEMP2 ON fmart.CTEMP2.object_id = fmart.CTEMP1.feature_id;
CREATE TABLE fmart.CTEMP4 AS SELECT DISTINCT object_id FROM fmart.fly__aberration_junction__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index265 ON fmart.CTEMP4 (object_id);
CREATE INDEX index266 ON fmart.CTEMP3 (feature_id);
CREATE TABLE fmart.CTEMP5  AS SELECT CTEMP3.feature_id,CTEMP3.dbxref_id,CTEMP3.organism_id,CTEMP3.name,CTEMP3.uniquename,CTEMP3.residues,CTEMP3.seqlen,CTEMP3.md5checksum,CTEMP3.type_id,CTEMP3.is_analysis,CTEMP3.timeaccessioned,CTEMP3.timelastmodified,CTEMP3.cvterm_id,CTEMP3.cv_id,CTEMP3.name_MTEMP0,CTEMP3.definition,CTEMP3.dbxref_id_MTEMP0,CTEMP3.gene_start,CTEMP3.gene_end,CTEMP3.strand,CTEMP3.srcfeature_id,CTEMP3.rank,CTEMP3.chromosome_acc,CTEMP3.chromosome,CTEMP3.organism_id_MTEMP3,CTEMP3.abbreviation,CTEMP3.genus,CTEMP3.species,CTEMP3.common_name,CTEMP3.comment,CTEMP3.DNA_motif_bool,CTEMP3.RNA_motif_bool,CTEMP4.object_id AS aberration_junction_bool FROM fmart.CTEMP3 LEFT JOIN fmart.CTEMP4 ON fmart.CTEMP4.object_id = fmart.CTEMP3.feature_id;
CREATE TABLE fmart.CTEMP6 AS SELECT DISTINCT object_id FROM fmart.fly__enhancer__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index267 ON fmart.CTEMP6 (object_id);
CREATE INDEX index268 ON fmart.CTEMP5 (feature_id);
CREATE TABLE fmart.CTEMP7  AS SELECT CTEMP5.feature_id,CTEMP5.dbxref_id,CTEMP5.organism_id,CTEMP5.name,CTEMP5.uniquename,CTEMP5.residues,CTEMP5.seqlen,CTEMP5.md5checksum,CTEMP5.type_id,CTEMP5.is_analysis,CTEMP5.timeaccessioned,CTEMP5.timelastmodified,CTEMP5.cvterm_id,CTEMP5.cv_id,CTEMP5.name_MTEMP0,CTEMP5.definition,CTEMP5.dbxref_id_MTEMP0,CTEMP5.gene_start,CTEMP5.gene_end,CTEMP5.strand,CTEMP5.srcfeature_id,CTEMP5.rank,CTEMP5.chromosome_acc,CTEMP5.chromosome,CTEMP5.organism_id_MTEMP3,CTEMP5.abbreviation,CTEMP5.genus,CTEMP5.species,CTEMP5.common_name,CTEMP5.comment,CTEMP5.DNA_motif_bool,CTEMP5.RNA_motif_bool,CTEMP5.aberration_junction_bool,CTEMP6.object_id AS enhancer_bool FROM fmart.CTEMP5 LEFT JOIN fmart.CTEMP6 ON fmart.CTEMP6.object_id = fmart.CTEMP5.feature_id;
CREATE TABLE fmart.CTEMP8 AS SELECT DISTINCT object_id FROM fmart.fly__insertion_site__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index269 ON fmart.CTEMP8 (object_id);
CREATE INDEX index270 ON fmart.CTEMP7 (feature_id);
CREATE TABLE fmart.CTEMP9  AS SELECT CTEMP7.feature_id,CTEMP7.dbxref_id,CTEMP7.organism_id,CTEMP7.name,CTEMP7.uniquename,CTEMP7.residues,CTEMP7.seqlen,CTEMP7.md5checksum,CTEMP7.type_id,CTEMP7.is_analysis,CTEMP7.timeaccessioned,CTEMP7.timelastmodified,CTEMP7.cvterm_id,CTEMP7.cv_id,CTEMP7.name_MTEMP0,CTEMP7.definition,CTEMP7.dbxref_id_MTEMP0,CTEMP7.gene_start,CTEMP7.gene_end,CTEMP7.strand,CTEMP7.srcfeature_id,CTEMP7.rank,CTEMP7.chromosome_acc,CTEMP7.chromosome,CTEMP7.organism_id_MTEMP3,CTEMP7.abbreviation,CTEMP7.genus,CTEMP7.species,CTEMP7.common_name,CTEMP7.comment,CTEMP7.DNA_motif_bool,CTEMP7.RNA_motif_bool,CTEMP7.aberration_junction_bool,CTEMP7.enhancer_bool,CTEMP8.object_id AS insertion_site_bool FROM fmart.CTEMP7 LEFT JOIN fmart.CTEMP8 ON fmart.CTEMP8.object_id = fmart.CTEMP7.feature_id;
CREATE TABLE fmart.CTEMP10 AS SELECT DISTINCT object_id FROM fmart.fly__mRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index271 ON fmart.CTEMP10 (object_id);
CREATE INDEX index272 ON fmart.CTEMP9 (feature_id);
CREATE TABLE fmart.CTEMP11  AS SELECT CTEMP9.feature_id,CTEMP9.dbxref_id,CTEMP9.organism_id,CTEMP9.name,CTEMP9.uniquename,CTEMP9.residues,CTEMP9.seqlen,CTEMP9.md5checksum,CTEMP9.type_id,CTEMP9.is_analysis,CTEMP9.timeaccessioned,CTEMP9.timelastmodified,CTEMP9.cvterm_id,CTEMP9.cv_id,CTEMP9.name_MTEMP0,CTEMP9.definition,CTEMP9.dbxref_id_MTEMP0,CTEMP9.gene_start,CTEMP9.gene_end,CTEMP9.strand,CTEMP9.srcfeature_id,CTEMP9.rank,CTEMP9.chromosome_acc,CTEMP9.chromosome,CTEMP9.organism_id_MTEMP3,CTEMP9.abbreviation,CTEMP9.genus,CTEMP9.species,CTEMP9.common_name,CTEMP9.comment,CTEMP9.DNA_motif_bool,CTEMP9.RNA_motif_bool,CTEMP9.aberration_junction_bool,CTEMP9.enhancer_bool,CTEMP9.insertion_site_bool,CTEMP10.object_id AS mRNA_bool FROM fmart.CTEMP9 LEFT JOIN fmart.CTEMP10 ON fmart.CTEMP10.object_id = fmart.CTEMP9.feature_id;
CREATE TABLE fmart.CTEMP12 AS SELECT DISTINCT object_id FROM fmart.fly__ncRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index273 ON fmart.CTEMP12 (object_id);
CREATE INDEX index274 ON fmart.CTEMP11 (feature_id);
CREATE TABLE fmart.CTEMP13  AS SELECT CTEMP11.feature_id,CTEMP11.dbxref_id,CTEMP11.organism_id,CTEMP11.name,CTEMP11.uniquename,CTEMP11.residues,CTEMP11.seqlen,CTEMP11.md5checksum,CTEMP11.type_id,CTEMP11.is_analysis,CTEMP11.timeaccessioned,CTEMP11.timelastmodified,CTEMP11.cvterm_id,CTEMP11.cv_id,CTEMP11.name_MTEMP0,CTEMP11.definition,CTEMP11.dbxref_id_MTEMP0,CTEMP11.gene_start,CTEMP11.gene_end,CTEMP11.strand,CTEMP11.srcfeature_id,CTEMP11.rank,CTEMP11.chromosome_acc,CTEMP11.chromosome,CTEMP11.organism_id_MTEMP3,CTEMP11.abbreviation,CTEMP11.genus,CTEMP11.species,CTEMP11.common_name,CTEMP11.comment,CTEMP11.DNA_motif_bool,CTEMP11.RNA_motif_bool,CTEMP11.aberration_junction_bool,CTEMP11.enhancer_bool,CTEMP11.insertion_site_bool,CTEMP11.mRNA_bool,CTEMP12.object_id AS ncRNA_bool FROM fmart.CTEMP11 LEFT JOIN fmart.CTEMP12 ON fmart.CTEMP12.object_id = fmart.CTEMP11.feature_id;
CREATE TABLE fmart.CTEMP14 AS SELECT DISTINCT object_id FROM fmart.fly__point_mutation__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index275 ON fmart.CTEMP14 (object_id);
CREATE INDEX index276 ON fmart.CTEMP13 (feature_id);
CREATE TABLE fmart.CTEMP15  AS SELECT CTEMP13.feature_id,CTEMP13.dbxref_id,CTEMP13.organism_id,CTEMP13.name,CTEMP13.uniquename,CTEMP13.residues,CTEMP13.seqlen,CTEMP13.md5checksum,CTEMP13.type_id,CTEMP13.is_analysis,CTEMP13.timeaccessioned,CTEMP13.timelastmodified,CTEMP13.cvterm_id,CTEMP13.cv_id,CTEMP13.name_MTEMP0,CTEMP13.definition,CTEMP13.dbxref_id_MTEMP0,CTEMP13.gene_start,CTEMP13.gene_end,CTEMP13.strand,CTEMP13.srcfeature_id,CTEMP13.rank,CTEMP13.chromosome_acc,CTEMP13.chromosome,CTEMP13.organism_id_MTEMP3,CTEMP13.abbreviation,CTEMP13.genus,CTEMP13.species,CTEMP13.common_name,CTEMP13.comment,CTEMP13.DNA_motif_bool,CTEMP13.RNA_motif_bool,CTEMP13.aberration_junction_bool,CTEMP13.enhancer_bool,CTEMP13.insertion_site_bool,CTEMP13.mRNA_bool,CTEMP13.ncRNA_bool,CTEMP14.object_id AS point_mutation_bool FROM fmart.CTEMP13 LEFT JOIN fmart.CTEMP14 ON fmart.CTEMP14.object_id = fmart.CTEMP13.feature_id;
CREATE TABLE fmart.CTEMP16 AS SELECT DISTINCT object_id FROM fmart.fly__protein_binding_site__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index277 ON fmart.CTEMP16 (object_id);
CREATE INDEX index278 ON fmart.CTEMP15 (feature_id);
CREATE TABLE fmart.CTEMP17  AS SELECT CTEMP15.feature_id,CTEMP15.dbxref_id,CTEMP15.organism_id,CTEMP15.name,CTEMP15.uniquename,CTEMP15.residues,CTEMP15.seqlen,CTEMP15.md5checksum,CTEMP15.type_id,CTEMP15.is_analysis,CTEMP15.timeaccessioned,CTEMP15.timelastmodified,CTEMP15.cvterm_id,CTEMP15.cv_id,CTEMP15.name_MTEMP0,CTEMP15.definition,CTEMP15.dbxref_id_MTEMP0,CTEMP15.gene_start,CTEMP15.gene_end,CTEMP15.strand,CTEMP15.srcfeature_id,CTEMP15.rank,CTEMP15.chromosome_acc,CTEMP15.chromosome,CTEMP15.organism_id_MTEMP3,CTEMP15.abbreviation,CTEMP15.genus,CTEMP15.species,CTEMP15.common_name,CTEMP15.comment,CTEMP15.DNA_motif_bool,CTEMP15.RNA_motif_bool,CTEMP15.aberration_junction_bool,CTEMP15.enhancer_bool,CTEMP15.insertion_site_bool,CTEMP15.mRNA_bool,CTEMP15.ncRNA_bool,CTEMP15.point_mutation_bool,CTEMP16.object_id AS protein_binding_site_bool FROM fmart.CTEMP15 LEFT JOIN fmart.CTEMP16 ON fmart.CTEMP16.object_id = fmart.CTEMP15.feature_id;
CREATE TABLE fmart.CTEMP18 AS SELECT DISTINCT object_id FROM fmart.fly__pseudogene__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index279 ON fmart.CTEMP18 (object_id);
CREATE INDEX index280 ON fmart.CTEMP17 (feature_id);
CREATE TABLE fmart.CTEMP19  AS SELECT CTEMP17.feature_id,CTEMP17.dbxref_id,CTEMP17.organism_id,CTEMP17.name,CTEMP17.uniquename,CTEMP17.residues,CTEMP17.seqlen,CTEMP17.md5checksum,CTEMP17.type_id,CTEMP17.is_analysis,CTEMP17.timeaccessioned,CTEMP17.timelastmodified,CTEMP17.cvterm_id,CTEMP17.cv_id,CTEMP17.name_MTEMP0,CTEMP17.definition,CTEMP17.dbxref_id_MTEMP0,CTEMP17.gene_start,CTEMP17.gene_end,CTEMP17.strand,CTEMP17.srcfeature_id,CTEMP17.rank,CTEMP17.chromosome_acc,CTEMP17.chromosome,CTEMP17.organism_id_MTEMP3,CTEMP17.abbreviation,CTEMP17.genus,CTEMP17.species,CTEMP17.common_name,CTEMP17.comment,CTEMP17.DNA_motif_bool,CTEMP17.RNA_motif_bool,CTEMP17.aberration_junction_bool,CTEMP17.enhancer_bool,CTEMP17.insertion_site_bool,CTEMP17.mRNA_bool,CTEMP17.ncRNA_bool,CTEMP17.point_mutation_bool,CTEMP17.protein_binding_site_bool,CTEMP18.object_id AS pseudogene_bool FROM fmart.CTEMP17 LEFT JOIN fmart.CTEMP18 ON fmart.CTEMP18.object_id = fmart.CTEMP17.feature_id;
CREATE TABLE fmart.CTEMP20 AS SELECT DISTINCT object_id FROM fmart.fly__rRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index281 ON fmart.CTEMP20 (object_id);
CREATE INDEX index282 ON fmart.CTEMP19 (feature_id);
CREATE TABLE fmart.CTEMP21  AS SELECT CTEMP19.feature_id,CTEMP19.dbxref_id,CTEMP19.organism_id,CTEMP19.name,CTEMP19.uniquename,CTEMP19.residues,CTEMP19.seqlen,CTEMP19.md5checksum,CTEMP19.type_id,CTEMP19.is_analysis,CTEMP19.timeaccessioned,CTEMP19.timelastmodified,CTEMP19.cvterm_id,CTEMP19.cv_id,CTEMP19.name_MTEMP0,CTEMP19.definition,CTEMP19.dbxref_id_MTEMP0,CTEMP19.gene_start,CTEMP19.gene_end,CTEMP19.strand,CTEMP19.srcfeature_id,CTEMP19.rank,CTEMP19.chromosome_acc,CTEMP19.chromosome,CTEMP19.organism_id_MTEMP3,CTEMP19.abbreviation,CTEMP19.genus,CTEMP19.species,CTEMP19.common_name,CTEMP19.comment,CTEMP19.DNA_motif_bool,CTEMP19.RNA_motif_bool,CTEMP19.aberration_junction_bool,CTEMP19.enhancer_bool,CTEMP19.insertion_site_bool,CTEMP19.mRNA_bool,CTEMP19.ncRNA_bool,CTEMP19.point_mutation_bool,CTEMP19.protein_binding_site_bool,CTEMP19.pseudogene_bool,CTEMP20.object_id AS rRNA_bool FROM fmart.CTEMP19 LEFT JOIN fmart.CTEMP20 ON fmart.CTEMP20.object_id = fmart.CTEMP19.feature_id;
CREATE TABLE fmart.CTEMP22 AS SELECT DISTINCT object_id FROM fmart.fly__region__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index283 ON fmart.CTEMP22 (object_id);
CREATE INDEX index284 ON fmart.CTEMP21 (feature_id);
CREATE TABLE fmart.CTEMP23  AS SELECT CTEMP21.feature_id,CTEMP21.dbxref_id,CTEMP21.organism_id,CTEMP21.name,CTEMP21.uniquename,CTEMP21.residues,CTEMP21.seqlen,CTEMP21.md5checksum,CTEMP21.type_id,CTEMP21.is_analysis,CTEMP21.timeaccessioned,CTEMP21.timelastmodified,CTEMP21.cvterm_id,CTEMP21.cv_id,CTEMP21.name_MTEMP0,CTEMP21.definition,CTEMP21.dbxref_id_MTEMP0,CTEMP21.gene_start,CTEMP21.gene_end,CTEMP21.strand,CTEMP21.srcfeature_id,CTEMP21.rank,CTEMP21.chromosome_acc,CTEMP21.chromosome,CTEMP21.organism_id_MTEMP3,CTEMP21.abbreviation,CTEMP21.genus,CTEMP21.species,CTEMP21.common_name,CTEMP21.comment,CTEMP21.DNA_motif_bool,CTEMP21.RNA_motif_bool,CTEMP21.aberration_junction_bool,CTEMP21.enhancer_bool,CTEMP21.insertion_site_bool,CTEMP21.mRNA_bool,CTEMP21.ncRNA_bool,CTEMP21.point_mutation_bool,CTEMP21.protein_binding_site_bool,CTEMP21.pseudogene_bool,CTEMP21.rRNA_bool,CTEMP22.object_id AS region_bool FROM fmart.CTEMP21 LEFT JOIN fmart.CTEMP22 ON fmart.CTEMP22.object_id = fmart.CTEMP21.feature_id;
CREATE TABLE fmart.CTEMP24 AS SELECT DISTINCT object_id FROM fmart.fly__regulatory_region__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index285 ON fmart.CTEMP24 (object_id);
CREATE INDEX index286 ON fmart.CTEMP23 (feature_id);
CREATE TABLE fmart.CTEMP25  AS SELECT CTEMP23.feature_id,CTEMP23.dbxref_id,CTEMP23.organism_id,CTEMP23.name,CTEMP23.uniquename,CTEMP23.residues,CTEMP23.seqlen,CTEMP23.md5checksum,CTEMP23.type_id,CTEMP23.is_analysis,CTEMP23.timeaccessioned,CTEMP23.timelastmodified,CTEMP23.cvterm_id,CTEMP23.cv_id,CTEMP23.name_MTEMP0,CTEMP23.definition,CTEMP23.dbxref_id_MTEMP0,CTEMP23.gene_start,CTEMP23.gene_end,CTEMP23.strand,CTEMP23.srcfeature_id,CTEMP23.rank,CTEMP23.chromosome_acc,CTEMP23.chromosome,CTEMP23.organism_id_MTEMP3,CTEMP23.abbreviation,CTEMP23.genus,CTEMP23.species,CTEMP23.common_name,CTEMP23.comment,CTEMP23.DNA_motif_bool,CTEMP23.RNA_motif_bool,CTEMP23.aberration_junction_bool,CTEMP23.enhancer_bool,CTEMP23.insertion_site_bool,CTEMP23.mRNA_bool,CTEMP23.ncRNA_bool,CTEMP23.point_mutation_bool,CTEMP23.protein_binding_site_bool,CTEMP23.pseudogene_bool,CTEMP23.rRNA_bool,CTEMP23.region_bool,CTEMP24.object_id AS regulatory_region_bool FROM fmart.CTEMP23 LEFT JOIN fmart.CTEMP24 ON fmart.CTEMP24.object_id = fmart.CTEMP23.feature_id;
CREATE TABLE fmart.CTEMP26 AS SELECT DISTINCT object_id FROM fmart.fly__repeat_region__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index287 ON fmart.CTEMP26 (object_id);
CREATE INDEX index288 ON fmart.CTEMP25 (feature_id);
CREATE TABLE fmart.CTEMP27  AS SELECT CTEMP25.feature_id,CTEMP25.dbxref_id,CTEMP25.organism_id,CTEMP25.name,CTEMP25.uniquename,CTEMP25.residues,CTEMP25.seqlen,CTEMP25.md5checksum,CTEMP25.type_id,CTEMP25.is_analysis,CTEMP25.timeaccessioned,CTEMP25.timelastmodified,CTEMP25.cvterm_id,CTEMP25.cv_id,CTEMP25.name_MTEMP0,CTEMP25.definition,CTEMP25.dbxref_id_MTEMP0,CTEMP25.gene_start,CTEMP25.gene_end,CTEMP25.strand,CTEMP25.srcfeature_id,CTEMP25.rank,CTEMP25.chromosome_acc,CTEMP25.chromosome,CTEMP25.organism_id_MTEMP3,CTEMP25.abbreviation,CTEMP25.genus,CTEMP25.species,CTEMP25.common_name,CTEMP25.comment,CTEMP25.DNA_motif_bool,CTEMP25.RNA_motif_bool,CTEMP25.aberration_junction_bool,CTEMP25.enhancer_bool,CTEMP25.insertion_site_bool,CTEMP25.mRNA_bool,CTEMP25.ncRNA_bool,CTEMP25.point_mutation_bool,CTEMP25.protein_binding_site_bool,CTEMP25.pseudogene_bool,CTEMP25.rRNA_bool,CTEMP25.region_bool,CTEMP25.regulatory_region_bool,CTEMP26.object_id AS repeat_region_bool FROM fmart.CTEMP25 LEFT JOIN fmart.CTEMP26 ON fmart.CTEMP26.object_id = fmart.CTEMP25.feature_id;
CREATE TABLE fmart.CTEMP28 AS SELECT DISTINCT object_id FROM fmart.fly__rescue_fragment__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index289 ON fmart.CTEMP28 (object_id);
CREATE INDEX index290 ON fmart.CTEMP27 (feature_id);
CREATE TABLE fmart.CTEMP29  AS SELECT CTEMP27.feature_id,CTEMP27.dbxref_id,CTEMP27.organism_id,CTEMP27.name,CTEMP27.uniquename,CTEMP27.residues,CTEMP27.seqlen,CTEMP27.md5checksum,CTEMP27.type_id,CTEMP27.is_analysis,CTEMP27.timeaccessioned,CTEMP27.timelastmodified,CTEMP27.cvterm_id,CTEMP27.cv_id,CTEMP27.name_MTEMP0,CTEMP27.definition,CTEMP27.dbxref_id_MTEMP0,CTEMP27.gene_start,CTEMP27.gene_end,CTEMP27.strand,CTEMP27.srcfeature_id,CTEMP27.rank,CTEMP27.chromosome_acc,CTEMP27.chromosome,CTEMP27.organism_id_MTEMP3,CTEMP27.abbreviation,CTEMP27.genus,CTEMP27.species,CTEMP27.common_name,CTEMP27.comment,CTEMP27.DNA_motif_bool,CTEMP27.RNA_motif_bool,CTEMP27.aberration_junction_bool,CTEMP27.enhancer_bool,CTEMP27.insertion_site_bool,CTEMP27.mRNA_bool,CTEMP27.ncRNA_bool,CTEMP27.point_mutation_bool,CTEMP27.protein_binding_site_bool,CTEMP27.pseudogene_bool,CTEMP27.rRNA_bool,CTEMP27.region_bool,CTEMP27.regulatory_region_bool,CTEMP27.repeat_region_bool,CTEMP28.object_id AS rescue_fragment_bool FROM fmart.CTEMP27 LEFT JOIN fmart.CTEMP28 ON fmart.CTEMP28.object_id = fmart.CTEMP27.feature_id;
CREATE TABLE fmart.CTEMP30 AS SELECT DISTINCT object_id FROM fmart.fly__sequence_variant__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index291 ON fmart.CTEMP30 (object_id);
CREATE INDEX index292 ON fmart.CTEMP29 (feature_id);
CREATE TABLE fmart.CTEMP31  AS SELECT CTEMP29.feature_id,CTEMP29.dbxref_id,CTEMP29.organism_id,CTEMP29.name,CTEMP29.uniquename,CTEMP29.residues,CTEMP29.seqlen,CTEMP29.md5checksum,CTEMP29.type_id,CTEMP29.is_analysis,CTEMP29.timeaccessioned,CTEMP29.timelastmodified,CTEMP29.cvterm_id,CTEMP29.cv_id,CTEMP29.name_MTEMP0,CTEMP29.definition,CTEMP29.dbxref_id_MTEMP0,CTEMP29.gene_start,CTEMP29.gene_end,CTEMP29.strand,CTEMP29.srcfeature_id,CTEMP29.rank,CTEMP29.chromosome_acc,CTEMP29.chromosome,CTEMP29.organism_id_MTEMP3,CTEMP29.abbreviation,CTEMP29.genus,CTEMP29.species,CTEMP29.common_name,CTEMP29.comment,CTEMP29.DNA_motif_bool,CTEMP29.RNA_motif_bool,CTEMP29.aberration_junction_bool,CTEMP29.enhancer_bool,CTEMP29.insertion_site_bool,CTEMP29.mRNA_bool,CTEMP29.ncRNA_bool,CTEMP29.point_mutation_bool,CTEMP29.protein_binding_site_bool,CTEMP29.pseudogene_bool,CTEMP29.rRNA_bool,CTEMP29.region_bool,CTEMP29.regulatory_region_bool,CTEMP29.repeat_region_bool,CTEMP29.rescue_fragment_bool,CTEMP30.object_id AS sequence_variant_bool FROM fmart.CTEMP29 LEFT JOIN fmart.CTEMP30 ON fmart.CTEMP30.object_id = fmart.CTEMP29.feature_id;
CREATE TABLE fmart.CTEMP32 AS SELECT DISTINCT object_id FROM fmart.fly__snRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index293 ON fmart.CTEMP32 (object_id);
CREATE INDEX index294 ON fmart.CTEMP31 (feature_id);
CREATE TABLE fmart.CTEMP33  AS SELECT CTEMP31.feature_id,CTEMP31.dbxref_id,CTEMP31.organism_id,CTEMP31.name,CTEMP31.uniquename,CTEMP31.residues,CTEMP31.seqlen,CTEMP31.md5checksum,CTEMP31.type_id,CTEMP31.is_analysis,CTEMP31.timeaccessioned,CTEMP31.timelastmodified,CTEMP31.cvterm_id,CTEMP31.cv_id,CTEMP31.name_MTEMP0,CTEMP31.definition,CTEMP31.dbxref_id_MTEMP0,CTEMP31.gene_start,CTEMP31.gene_end,CTEMP31.strand,CTEMP31.srcfeature_id,CTEMP31.rank,CTEMP31.chromosome_acc,CTEMP31.chromosome,CTEMP31.organism_id_MTEMP3,CTEMP31.abbreviation,CTEMP31.genus,CTEMP31.species,CTEMP31.common_name,CTEMP31.comment,CTEMP31.DNA_motif_bool,CTEMP31.RNA_motif_bool,CTEMP31.aberration_junction_bool,CTEMP31.enhancer_bool,CTEMP31.insertion_site_bool,CTEMP31.mRNA_bool,CTEMP31.ncRNA_bool,CTEMP31.point_mutation_bool,CTEMP31.protein_binding_site_bool,CTEMP31.pseudogene_bool,CTEMP31.rRNA_bool,CTEMP31.region_bool,CTEMP31.regulatory_region_bool,CTEMP31.repeat_region_bool,CTEMP31.rescue_fragment_bool,CTEMP31.sequence_variant_bool,CTEMP32.object_id AS snRNA_bool FROM fmart.CTEMP31 LEFT JOIN fmart.CTEMP32 ON fmart.CTEMP32.object_id = fmart.CTEMP31.feature_id;
CREATE TABLE fmart.CTEMP34 AS SELECT DISTINCT object_id FROM fmart.fly__snoRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index295 ON fmart.CTEMP34 (object_id);
CREATE INDEX index296 ON fmart.CTEMP33 (feature_id);
CREATE TABLE fmart.CTEMP35  AS SELECT CTEMP33.feature_id,CTEMP33.dbxref_id,CTEMP33.organism_id,CTEMP33.name,CTEMP33.uniquename,CTEMP33.residues,CTEMP33.seqlen,CTEMP33.md5checksum,CTEMP33.type_id,CTEMP33.is_analysis,CTEMP33.timeaccessioned,CTEMP33.timelastmodified,CTEMP33.cvterm_id,CTEMP33.cv_id,CTEMP33.name_MTEMP0,CTEMP33.definition,CTEMP33.dbxref_id_MTEMP0,CTEMP33.gene_start,CTEMP33.gene_end,CTEMP33.strand,CTEMP33.srcfeature_id,CTEMP33.rank,CTEMP33.chromosome_acc,CTEMP33.chromosome,CTEMP33.organism_id_MTEMP3,CTEMP33.abbreviation,CTEMP33.genus,CTEMP33.species,CTEMP33.common_name,CTEMP33.comment,CTEMP33.DNA_motif_bool,CTEMP33.RNA_motif_bool,CTEMP33.aberration_junction_bool,CTEMP33.enhancer_bool,CTEMP33.insertion_site_bool,CTEMP33.mRNA_bool,CTEMP33.ncRNA_bool,CTEMP33.point_mutation_bool,CTEMP33.protein_binding_site_bool,CTEMP33.pseudogene_bool,CTEMP33.rRNA_bool,CTEMP33.region_bool,CTEMP33.regulatory_region_bool,CTEMP33.repeat_region_bool,CTEMP33.rescue_fragment_bool,CTEMP33.sequence_variant_bool,CTEMP33.snRNA_bool,CTEMP34.object_id AS snoRNA_bool FROM fmart.CTEMP33 LEFT JOIN fmart.CTEMP34 ON fmart.CTEMP34.object_id = fmart.CTEMP33.feature_id;
CREATE TABLE fmart.CTEMP36 AS SELECT DISTINCT object_id FROM fmart.fly__tRNA__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index297 ON fmart.CTEMP36 (object_id);
CREATE INDEX index298 ON fmart.CTEMP35 (feature_id);
CREATE TABLE fmart.CTEMP37  AS SELECT CTEMP35.feature_id,CTEMP35.dbxref_id,CTEMP35.organism_id,CTEMP35.name,CTEMP35.uniquename,CTEMP35.residues,CTEMP35.seqlen,CTEMP35.md5checksum,CTEMP35.type_id,CTEMP35.is_analysis,CTEMP35.timeaccessioned,CTEMP35.timelastmodified,CTEMP35.cvterm_id,CTEMP35.cv_id,CTEMP35.name_MTEMP0,CTEMP35.definition,CTEMP35.dbxref_id_MTEMP0,CTEMP35.gene_start,CTEMP35.gene_end,CTEMP35.strand,CTEMP35.srcfeature_id,CTEMP35.rank,CTEMP35.chromosome_acc,CTEMP35.chromosome,CTEMP35.organism_id_MTEMP3,CTEMP35.abbreviation,CTEMP35.genus,CTEMP35.species,CTEMP35.common_name,CTEMP35.comment,CTEMP35.DNA_motif_bool,CTEMP35.RNA_motif_bool,CTEMP35.aberration_junction_bool,CTEMP35.enhancer_bool,CTEMP35.insertion_site_bool,CTEMP35.mRNA_bool,CTEMP35.ncRNA_bool,CTEMP35.point_mutation_bool,CTEMP35.protein_binding_site_bool,CTEMP35.pseudogene_bool,CTEMP35.rRNA_bool,CTEMP35.region_bool,CTEMP35.regulatory_region_bool,CTEMP35.repeat_region_bool,CTEMP35.rescue_fragment_bool,CTEMP35.sequence_variant_bool,CTEMP35.snRNA_bool,CTEMP35.snoRNA_bool,CTEMP36.object_id AS tRNA_bool FROM fmart.CTEMP35 LEFT JOIN fmart.CTEMP36 ON fmart.CTEMP36.object_id = fmart.CTEMP35.feature_id;
CREATE TABLE fmart.CTEMP38 AS SELECT DISTINCT object_id FROM fmart.fly__Gadfly__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index299 ON fmart.CTEMP38 (object_id);
CREATE INDEX index300 ON fmart.CTEMP37 (feature_id);
CREATE TABLE fmart.CTEMP39  AS SELECT CTEMP37.feature_id,CTEMP37.dbxref_id,CTEMP37.organism_id,CTEMP37.name,CTEMP37.uniquename,CTEMP37.residues,CTEMP37.seqlen,CTEMP37.md5checksum,CTEMP37.type_id,CTEMP37.is_analysis,CTEMP37.timeaccessioned,CTEMP37.timelastmodified,CTEMP37.cvterm_id,CTEMP37.cv_id,CTEMP37.name_MTEMP0,CTEMP37.definition,CTEMP37.dbxref_id_MTEMP0,CTEMP37.gene_start,CTEMP37.gene_end,CTEMP37.strand,CTEMP37.srcfeature_id,CTEMP37.rank,CTEMP37.chromosome_acc,CTEMP37.chromosome,CTEMP37.organism_id_MTEMP3,CTEMP37.abbreviation,CTEMP37.genus,CTEMP37.species,CTEMP37.common_name,CTEMP37.comment,CTEMP37.DNA_motif_bool,CTEMP37.RNA_motif_bool,CTEMP37.aberration_junction_bool,CTEMP37.enhancer_bool,CTEMP37.insertion_site_bool,CTEMP37.mRNA_bool,CTEMP37.ncRNA_bool,CTEMP37.point_mutation_bool,CTEMP37.protein_binding_site_bool,CTEMP37.pseudogene_bool,CTEMP37.rRNA_bool,CTEMP37.region_bool,CTEMP37.regulatory_region_bool,CTEMP37.repeat_region_bool,CTEMP37.rescue_fragment_bool,CTEMP37.sequence_variant_bool,CTEMP37.snRNA_bool,CTEMP37.snoRNA_bool,CTEMP37.tRNA_bool,CTEMP38.object_id AS Gadfly_bool FROM fmart.CTEMP37 LEFT JOIN fmart.CTEMP38 ON fmart.CTEMP38.object_id = fmart.CTEMP37.feature_id;
CREATE TABLE fmart.CTEMP40 AS SELECT DISTINCT object_id FROM fmart.fly__FlyBase__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index301 ON fmart.CTEMP40 (object_id);
CREATE INDEX index302 ON fmart.CTEMP39 (feature_id);
CREATE TABLE fmart.CTEMP41  AS SELECT CTEMP39.feature_id,CTEMP39.dbxref_id,CTEMP39.organism_id,CTEMP39.name,CTEMP39.uniquename,CTEMP39.residues,CTEMP39.seqlen,CTEMP39.md5checksum,CTEMP39.type_id,CTEMP39.is_analysis,CTEMP39.timeaccessioned,CTEMP39.timelastmodified,CTEMP39.cvterm_id,CTEMP39.cv_id,CTEMP39.name_MTEMP0,CTEMP39.definition,CTEMP39.dbxref_id_MTEMP0,CTEMP39.gene_start,CTEMP39.gene_end,CTEMP39.strand,CTEMP39.srcfeature_id,CTEMP39.rank,CTEMP39.chromosome_acc,CTEMP39.chromosome,CTEMP39.organism_id_MTEMP3,CTEMP39.abbreviation,CTEMP39.genus,CTEMP39.species,CTEMP39.common_name,CTEMP39.comment,CTEMP39.DNA_motif_bool,CTEMP39.RNA_motif_bool,CTEMP39.aberration_junction_bool,CTEMP39.enhancer_bool,CTEMP39.insertion_site_bool,CTEMP39.mRNA_bool,CTEMP39.ncRNA_bool,CTEMP39.point_mutation_bool,CTEMP39.protein_binding_site_bool,CTEMP39.pseudogene_bool,CTEMP39.rRNA_bool,CTEMP39.region_bool,CTEMP39.regulatory_region_bool,CTEMP39.repeat_region_bool,CTEMP39.rescue_fragment_bool,CTEMP39.sequence_variant_bool,CTEMP39.snRNA_bool,CTEMP39.snoRNA_bool,CTEMP39.tRNA_bool,CTEMP39.Gadfly_bool,CTEMP40.object_id AS FlyBase_bool FROM fmart.CTEMP39 LEFT JOIN fmart.CTEMP40 ON fmart.CTEMP40.object_id = fmart.CTEMP39.feature_id;
CREATE TABLE fmart.CTEMP42 AS SELECT DISTINCT feature_id FROM fmart.fly__cellular_component__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index303 ON fmart.CTEMP42 (feature_id);
CREATE INDEX index304 ON fmart.CTEMP41 (feature_id);
CREATE TABLE fmart.CTEMP43  AS SELECT CTEMP41.feature_id,CTEMP41.dbxref_id,CTEMP41.organism_id,CTEMP41.name,CTEMP41.uniquename,CTEMP41.residues,CTEMP41.seqlen,CTEMP41.md5checksum,CTEMP41.type_id,CTEMP41.is_analysis,CTEMP41.timeaccessioned,CTEMP41.timelastmodified,CTEMP41.cvterm_id,CTEMP41.cv_id,CTEMP41.name_MTEMP0,CTEMP41.definition,CTEMP41.dbxref_id_MTEMP0,CTEMP41.gene_start,CTEMP41.gene_end,CTEMP41.strand,CTEMP41.srcfeature_id,CTEMP41.rank,CTEMP41.chromosome_acc,CTEMP41.chromosome,CTEMP41.organism_id_MTEMP3,CTEMP41.abbreviation,CTEMP41.genus,CTEMP41.species,CTEMP41.common_name,CTEMP41.comment,CTEMP41.DNA_motif_bool,CTEMP41.RNA_motif_bool,CTEMP41.aberration_junction_bool,CTEMP41.enhancer_bool,CTEMP41.insertion_site_bool,CTEMP41.mRNA_bool,CTEMP41.ncRNA_bool,CTEMP41.point_mutation_bool,CTEMP41.protein_binding_site_bool,CTEMP41.pseudogene_bool,CTEMP41.rRNA_bool,CTEMP41.region_bool,CTEMP41.regulatory_region_bool,CTEMP41.repeat_region_bool,CTEMP41.rescue_fragment_bool,CTEMP41.sequence_variant_bool,CTEMP41.snRNA_bool,CTEMP41.snoRNA_bool,CTEMP41.tRNA_bool,CTEMP41.Gadfly_bool,CTEMP41.FlyBase_bool,CTEMP42.feature_id AS cellular_component_bool FROM fmart.CTEMP41 LEFT JOIN fmart.CTEMP42 ON fmart.CTEMP42.feature_id = fmart.CTEMP41.feature_id;
CREATE TABLE fmart.CTEMP44 AS SELECT DISTINCT feature_id FROM fmart.fly__molecular_function__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index305 ON fmart.CTEMP44 (feature_id);
CREATE INDEX index306 ON fmart.CTEMP43 (feature_id);
CREATE TABLE fmart.CTEMP45  AS SELECT CTEMP43.feature_id,CTEMP43.dbxref_id,CTEMP43.organism_id,CTEMP43.name,CTEMP43.uniquename,CTEMP43.residues,CTEMP43.seqlen,CTEMP43.md5checksum,CTEMP43.type_id,CTEMP43.is_analysis,CTEMP43.timeaccessioned,CTEMP43.timelastmodified,CTEMP43.cvterm_id,CTEMP43.cv_id,CTEMP43.name_MTEMP0,CTEMP43.definition,CTEMP43.dbxref_id_MTEMP0,CTEMP43.gene_start,CTEMP43.gene_end,CTEMP43.strand,CTEMP43.srcfeature_id,CTEMP43.rank,CTEMP43.chromosome_acc,CTEMP43.chromosome,CTEMP43.organism_id_MTEMP3,CTEMP43.abbreviation,CTEMP43.genus,CTEMP43.species,CTEMP43.common_name,CTEMP43.comment,CTEMP43.DNA_motif_bool,CTEMP43.RNA_motif_bool,CTEMP43.aberration_junction_bool,CTEMP43.enhancer_bool,CTEMP43.insertion_site_bool,CTEMP43.mRNA_bool,CTEMP43.ncRNA_bool,CTEMP43.point_mutation_bool,CTEMP43.protein_binding_site_bool,CTEMP43.pseudogene_bool,CTEMP43.rRNA_bool,CTEMP43.region_bool,CTEMP43.regulatory_region_bool,CTEMP43.repeat_region_bool,CTEMP43.rescue_fragment_bool,CTEMP43.sequence_variant_bool,CTEMP43.snRNA_bool,CTEMP43.snoRNA_bool,CTEMP43.tRNA_bool,CTEMP43.Gadfly_bool,CTEMP43.FlyBase_bool,CTEMP43.cellular_component_bool,CTEMP44.feature_id AS molecular_function_bool FROM fmart.CTEMP43 LEFT JOIN fmart.CTEMP44 ON fmart.CTEMP44.feature_id = fmart.CTEMP43.feature_id;
CREATE TABLE fmart.CTEMP46 AS SELECT DISTINCT feature_id FROM fmart.fly__biological_process__dm WHERE feature_id IS NOT NULL;
CREATE INDEX index307 ON fmart.CTEMP46 (feature_id);
CREATE INDEX index308 ON fmart.CTEMP45 (feature_id);
CREATE TABLE fmart.fly__gene__main  AS SELECT CTEMP45.feature_id,CTEMP45.dbxref_id,CTEMP45.organism_id,CTEMP45.name,CTEMP45.uniquename,CTEMP45.residues,CTEMP45.seqlen,CTEMP45.md5checksum,CTEMP45.type_id,CTEMP45.is_analysis,CTEMP45.timeaccessioned,CTEMP45.timelastmodified,CTEMP45.cvterm_id,CTEMP45.cv_id,CTEMP45.name_MTEMP0,CTEMP45.definition,CTEMP45.dbxref_id_MTEMP0,CTEMP45.gene_start,CTEMP45.gene_end,CTEMP45.strand,CTEMP45.srcfeature_id,CTEMP45.rank,CTEMP45.chromosome_acc,CTEMP45.chromosome,CTEMP45.organism_id_MTEMP3,CTEMP45.abbreviation,CTEMP45.genus,CTEMP45.species,CTEMP45.common_name,CTEMP45.comment,CTEMP45.DNA_motif_bool,CTEMP45.RNA_motif_bool,CTEMP45.aberration_junction_bool,CTEMP45.enhancer_bool,CTEMP45.insertion_site_bool,CTEMP45.mRNA_bool,CTEMP45.ncRNA_bool,CTEMP45.point_mutation_bool,CTEMP45.protein_binding_site_bool,CTEMP45.pseudogene_bool,CTEMP45.rRNA_bool,CTEMP45.region_bool,CTEMP45.regulatory_region_bool,CTEMP45.repeat_region_bool,CTEMP45.rescue_fragment_bool,CTEMP45.sequence_variant_bool,CTEMP45.snRNA_bool,CTEMP45.snoRNA_bool,CTEMP45.tRNA_bool,CTEMP45.Gadfly_bool,CTEMP45.FlyBase_bool,CTEMP45.cellular_component_bool,CTEMP45.molecular_function_bool,CTEMP46.feature_id AS biological_process_bool FROM fmart.CTEMP45 LEFT JOIN fmart.CTEMP46 ON fmart.CTEMP46.feature_id = fmart.CTEMP45.feature_id;
DROP TABLE fmart.CTEMP0;
DROP TABLE fmart.CTEMP1;
DROP TABLE fmart.CTEMP2;
DROP TABLE fmart.CTEMP3;
DROP TABLE fmart.CTEMP4;
DROP TABLE fmart.CTEMP5;
DROP TABLE fmart.CTEMP6;
DROP TABLE fmart.CTEMP7;
DROP TABLE fmart.CTEMP8;
DROP TABLE fmart.CTEMP9;
DROP TABLE fmart.CTEMP10;
DROP TABLE fmart.CTEMP11;
DROP TABLE fmart.CTEMP12;
DROP TABLE fmart.CTEMP13;
DROP TABLE fmart.CTEMP14;
DROP TABLE fmart.CTEMP15;
DROP TABLE fmart.CTEMP16;
DROP TABLE fmart.CTEMP17;
DROP TABLE fmart.CTEMP18;
DROP TABLE fmart.CTEMP19;
DROP TABLE fmart.CTEMP20;
DROP TABLE fmart.CTEMP21;
DROP TABLE fmart.CTEMP22;
DROP TABLE fmart.CTEMP23;
DROP TABLE fmart.CTEMP24;
DROP TABLE fmart.CTEMP25;
DROP TABLE fmart.CTEMP26;
DROP TABLE fmart.CTEMP27;
DROP TABLE fmart.CTEMP28;
DROP TABLE fmart.CTEMP29;
DROP TABLE fmart.CTEMP30;
DROP TABLE fmart.CTEMP31;
DROP TABLE fmart.CTEMP32;
DROP TABLE fmart.CTEMP33;
DROP TABLE fmart.CTEMP34;
DROP TABLE fmart.CTEMP35;
DROP TABLE fmart.CTEMP36;
DROP TABLE fmart.CTEMP37;
DROP TABLE fmart.CTEMP38;
DROP TABLE fmart.CTEMP39;
DROP TABLE fmart.CTEMP40;
DROP TABLE fmart.CTEMP41;
DROP TABLE fmart.CTEMP42;
DROP TABLE fmart.CTEMP43;
DROP TABLE fmart.CTEMP44;
DROP TABLE fmart.CTEMP45;
DROP TABLE fmart.CTEMP46;

ALTER TABLE fmart.main_interim RENAME organism_id TO feature_id_key;
CREATE INDEX index274 ON fmart.main_interim (feature_id_key);
ALTER TABLE fmart.fly__DNA_motif__dm RENAME object_id TO feature_id_key;
CREATE INDEX index283 ON fmart.fly__DNA_motif__dm (feature_id_key);
ALTER TABLE fmart.fly__RNA_motif__dm RENAME object_id TO feature_id_key;
CREATE INDEX index293 ON fmart.fly__RNA_motif__dm (feature_id_key);
ALTER TABLE fmart.fly__aberration_junction__dm RENAME object_id TO feature_id_key;
CREATE INDEX index303 ON fmart.fly__aberration_junction__dm (feature_id_key);
ALTER TABLE fmart.fly__enhancer__dm RENAME object_id TO feature_id_key;
CREATE INDEX index313 ON fmart.fly__enhancer__dm (feature_id_key);
ALTER TABLE fmart.fly__insertion_site__dm RENAME object_id TO feature_id_key;
CREATE INDEX index323 ON fmart.fly__insertion_site__dm (feature_id_key);
ALTER TABLE fmart.fly__mRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index333 ON fmart.fly__mRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__ncRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index343 ON fmart.fly__ncRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__point_mutation__dm RENAME object_id TO feature_id_key;
CREATE INDEX index353 ON fmart.fly__point_mutation__dm (feature_id_key);
ALTER TABLE fmart.fly__protein_binding_site__dm RENAME object_id TO feature_id_key;
CREATE INDEX index363 ON fmart.fly__protein_binding_site__dm (feature_id_key);
ALTER TABLE fmart.fly__pseudogene__dm RENAME object_id TO feature_id_key;
CREATE INDEX index373 ON fmart.fly__pseudogene__dm (feature_id_key);
ALTER TABLE fmart.fly__rRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index383 ON fmart.fly__rRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__region__dm RENAME object_id TO feature_id_key;
CREATE INDEX index393 ON fmart.fly__region__dm (feature_id_key);
ALTER TABLE fmart.fly__regulatory_region__dm RENAME object_id TO feature_id_key;
CREATE INDEX index403 ON fmart.fly__regulatory_region__dm (feature_id_key);
ALTER TABLE fmart.fly__repeat_region__dm RENAME object_id TO feature_id_key;
CREATE INDEX index413 ON fmart.fly__repeat_region__dm (feature_id_key);
ALTER TABLE fmart.fly__rescue_fragment__dm RENAME object_id TO feature_id_key;
CREATE INDEX index423 ON fmart.fly__rescue_fragment__dm (feature_id_key);
ALTER TABLE fmart.fly__sequence_variant__dm RENAME object_id TO feature_id_key;
CREATE INDEX index433 ON fmart.fly__sequence_variant__dm (feature_id_key);
ALTER TABLE fmart.fly__snRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index443 ON fmart.fly__snRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__snoRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index453 ON fmart.fly__snoRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__tRNA__dm RENAME object_id TO feature_id_key;
CREATE INDEX index463 ON fmart.fly__tRNA__dm (feature_id_key);
ALTER TABLE fmart.fly__Gadfly__dm RENAME object_id TO feature_id_key;
CREATE INDEX index474 ON fmart.fly__Gadfly__dm (feature_id_key);
ALTER TABLE fmart.fly__FlyBase__dm RENAME object_id TO feature_id_key;
CREATE INDEX index484 ON fmart.fly__FlyBase__dm (feature_id_key);
ALTER TABLE fmart.fly__cellular_component__dm RENAME feature_id TO feature_id_key;
CREATE INDEX index494 ON fmart.fly__cellular_component__dm (feature_id_key);
ALTER TABLE fmart.fly__molecular_function__dm RENAME feature_id TO feature_id_key;
CREATE INDEX index504 ON fmart.fly__molecular_function__dm (feature_id_key);
ALTER TABLE fmart.fly__biological_process__dm RENAME feature_id TO feature_id_key;
CREATE INDEX index514 ON fmart.fly__biological_process__dm (feature_id_key);
ALTER TABLE fmart.fly__gene__main RENAME feature_id TO feature_id_key;
CREATE INDEX index568 ON fmart.fly__gene__main (feature_id_key);

--
--       TRANSFORMATION NO 26      TARGET TABLE: FLSTRUCTURE__GENE_STRUCTURE__MAIN
--
CREATE TABLE fmart.MTEMP0  AS SELECT feature.feature_id,feature.dbxref_id,feature.organism_id,feature.name,feature.uniquename,feature.residues,feature.seqlen,feature.md5checksum,feature.type_id,feature.is_analysis,feature.timeaccessioned,feature.timelastmodified,cvterm.cvterm_id,cvterm.cv_id,cvterm.name AS name_MTEMP0,cvterm.definition,cvterm.dbxref_id AS dbxref_id_MTEMP0 FROM feature , cvterm WHERE cvterm.cvterm_id = feature.type_id AND cvterm.name='mRNA';
CREATE INDEX index583 ON fmart.MTEMP0 (feature_id);
CREATE TABLE fmart.MTEMP1  AS SELECT MTEMP0.feature_id,MTEMP0.dbxref_id,MTEMP0.organism_id,MTEMP0.name,MTEMP0.uniquename,MTEMP0.residues,MTEMP0.seqlen,MTEMP0.md5checksum,MTEMP0.type_id,MTEMP0.is_analysis,MTEMP0.timeaccessioned,MTEMP0.timelastmodified,MTEMP0.cvterm_id,MTEMP0.cv_id,MTEMP0.name_MTEMP0,MTEMP0.definition,MTEMP0.dbxref_id_MTEMP0,feature_relationship.subject_id FROM fmart.MTEMP0 , feature_relationship WHERE feature_relationship.object_id = fmart.MTEMP0.feature_id;
CREATE INDEX index584 ON fmart.MTEMP1 (subject_id);
CREATE TABLE fmart.MTEMP2  AS SELECT MTEMP1.feature_id,MTEMP1.dbxref_id,MTEMP1.organism_id,MTEMP1.name,MTEMP1.uniquename,MTEMP1.residues,MTEMP1.seqlen,MTEMP1.md5checksum,MTEMP1.type_id,MTEMP1.is_analysis,MTEMP1.timeaccessioned,MTEMP1.timelastmodified,MTEMP1.cvterm_id,MTEMP1.cv_id,MTEMP1.name_MTEMP0,MTEMP1.definition,MTEMP1.dbxref_id_MTEMP0,MTEMP1.subject_id,feature.feature_id AS feature_id_MTEMP2,feature.uniquename AS exon_name,feature.type_id AS exon_type_id FROM fmart.MTEMP1 , feature WHERE feature.feature_id = fmart.MTEMP1.subject_id;
CREATE INDEX index585 ON fmart.MTEMP2 (feature_id);
CREATE TABLE fmart.MTEMP3  AS SELECT MTEMP2.feature_id,MTEMP2.dbxref_id,MTEMP2.organism_id,MTEMP2.name,MTEMP2.uniquename,MTEMP2.residues,MTEMP2.seqlen,MTEMP2.md5checksum,MTEMP2.type_id,MTEMP2.is_analysis,MTEMP2.timeaccessioned,MTEMP2.timelastmodified,MTEMP2.cvterm_id,MTEMP2.cv_id,MTEMP2.name_MTEMP0,MTEMP2.definition,MTEMP2.dbxref_id_MTEMP0,MTEMP2.subject_id,MTEMP2.feature_id_MTEMP2,MTEMP2.exon_name,MTEMP2.exon_type_id,featureloc.fmin AS exon_start,featureloc.fmax AS exon_end,featureloc.strand,featureloc.srcfeature_id,featureloc.rank FROM fmart.MTEMP2 , featureloc WHERE featureloc.feature_id = fmart.MTEMP2.feature_id;
CREATE INDEX index586 ON fmart.MTEMP3 (srcfeature_id);
CREATE TABLE fmart.MTEMP4  AS SELECT MTEMP3.feature_id,MTEMP3.dbxref_id,MTEMP3.organism_id,MTEMP3.name,MTEMP3.uniquename,MTEMP3.residues,MTEMP3.seqlen,MTEMP3.md5checksum,MTEMP3.type_id,MTEMP3.is_analysis,MTEMP3.timeaccessioned,MTEMP3.timelastmodified,MTEMP3.cvterm_id,MTEMP3.cv_id,MTEMP3.name_MTEMP0,MTEMP3.definition,MTEMP3.dbxref_id_MTEMP0,MTEMP3.subject_id,MTEMP3.feature_id_MTEMP2,MTEMP3.exon_name,MTEMP3.exon_type_id,MTEMP3.exon_start,MTEMP3.exon_end,MTEMP3.strand,MTEMP3.srcfeature_id,MTEMP3.rank,feature.name AS chromosome_acc,feature.uniquename AS chromosome FROM fmart.MTEMP3 , feature WHERE feature.feature_id = fmart.MTEMP3.srcfeature_id;
CREATE INDEX index587 ON fmart.MTEMP4 (exon_type_id);
CREATE TABLE fmart.flstructure__gene_structure__main  AS SELECT MTEMP4.feature_id,MTEMP4.dbxref_id,MTEMP4.organism_id,MTEMP4.name,MTEMP4.uniquename,MTEMP4.residues,MTEMP4.seqlen,MTEMP4.md5checksum,MTEMP4.type_id,MTEMP4.is_analysis,MTEMP4.timeaccessioned,MTEMP4.timelastmodified,MTEMP4.cvterm_id,MTEMP4.cv_id,MTEMP4.name_MTEMP0,MTEMP4.definition,MTEMP4.dbxref_id_MTEMP0,MTEMP4.subject_id,MTEMP4.feature_id_MTEMP2,MTEMP4.exon_name,MTEMP4.exon_type_id,MTEMP4.exon_start,MTEMP4.exon_end,MTEMP4.strand,MTEMP4.srcfeature_id,MTEMP4.rank,MTEMP4.chromosome_acc,MTEMP4.chromosome,cvterm.name AS exon_coding_type FROM fmart.MTEMP4 , cvterm WHERE cvterm.cvterm_id = fmart.MTEMP4.exon_type_id;
DROP TABLE fmart.MTEMP0;
DROP TABLE fmart.MTEMP1;
DROP TABLE fmart.MTEMP2;
DROP TABLE fmart.MTEMP3;
DROP TABLE fmart.MTEMP4;


--
--       TRANSFORMATION NO null      TARGET TABLE: FLSTRUCTURE__GENE_STRUCTURE__MAIN
--
ALTER TABLE fmart.flstructure__gene_structure__main RENAME exon_type_id TO feature_id_key;
CREATE INDEX index607 ON fmart.flstructure__gene_structure__main (feature_id_key);
