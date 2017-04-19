DROP TABLE IF EXISTS MTMP_probestuff_helper;
CREATE TABLE MTMP_probestuff_helper (
  array_name            VARCHAR(40),
  transcript_stable_id  VARCHAR(128),
  display_label         VARCHAR(100),
  array_vendor_and_name VARCHAR(80)
);

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    probe_set_transcript.stable_id AS transcript_stable_id,
    probe_set.name                 AS display_label,
    CONCAT(array.vendor, '_', REPLACE(REPLACE(array.name, '-', '_'), '.', '_'))
                                   AS array_vendor_and_name
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
                                   AS array_vendor_and_name
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

OPTIMIZE TABLE MTMP_probestuff_helper;
