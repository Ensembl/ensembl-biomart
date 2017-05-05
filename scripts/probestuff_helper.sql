DROP TABLE IF EXISTS MTMP_probestuff_helper;
CREATE TABLE MTMP_probestuff_helper (
  array_name            VARCHAR(40),
  transcript_stable_id  VARCHAR(128),
  display_label         VARCHAR(100),
  array_vendor_and_name VARCHAR(80),
  is_probeset_array     tinyint(1)
);

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    probe_set_transcript.stable_id AS transcript_stable_id,
    probe_set.name                 AS display_label,
    CONCAT(array.vendor, '_', REPLACE(REPLACE(array.name, '-', '_'), '.', '_'))
                                   AS array_vendor_and_name,
    array.is_probeset_array        AS is_probeset_array
from
  array
  join array_chip using (array_id)
  join probe using (array_chip_id)
  join probe_set using (probe_set_id)
  join probe_set_transcript using (probe_set_id)
where
  array.is_probeset_array=true;

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    probe_transcript.stable_id     AS transcript_stable_id,
    probe.name                     AS display_label,
    CONCAT(array.vendor, '_', REPLACE(REPLACE(array.name, '-', '_'), '.', '_'))
                                   AS array_vendor_and_name,
    array.is_probeset_array        AS is_probeset_array
from
  array
  join array_chip using (array_id)
  join probe using (array_chip_id)
  join probe_transcript using (probe_id)
where
  array.is_probeset_array=false;

ALTER TABLE MTMP_probestuff_helper
  ADD INDEX transcript_idx (transcript_stable_id),
  ADD INDEX vendor_name_idx (array_vendor_and_name);

-- The folowing probe name was to long for Rabbit and the name was exceding the mysql limit of 64 characters in biomart: ocuniculus_gene_ensembl__eFG_AGILENT_SurePrint_GPL16709_4x44k__dm

UPDATE MTMP_probestuff_helper SET array_vendor_and_name ='AGILENT_SurePrnt_GPL16709_4x44k' WHERE array_vendor_and_name='AGILENT_SurePrint_GPL16709_4x44k';

OPTIMIZE TABLE MTMP_probestuff_helper;
