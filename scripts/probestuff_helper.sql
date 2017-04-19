DROP TABLE IF EXISTS MTMP_probestuff_helper;
CREATE TABLE MTMP_probestuff_helper (
  array_name            VARCHAR(40),
  array_format          VARCHAR(20),
  array_vendor          VARCHAR(40),
  array_descr           VARCHAR(255),
  array_type            VARCHAR(20),
  array_class           VARCHAR(20),
  transcript_stable_id  VARCHAR(40),
  display_label         VARCHAR(40),
  array_vendor_and_name VARCHAR(80)
);

-- The PROBESET insert is split into three 10 million chunks
INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    array.format                   AS array_format,
    array.vendor                   AS array_vendor,
    array.description              AS array_descr,
    array.type                     AS array_type,
    array.class                    AS array_class,
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
  array.is_probeset_array=true
LIMIT 0,10000000;

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    array.format                   AS array_format,
    array.vendor                   AS array_vendor,
    array.description              AS array_descr,
    array.type                     AS array_type,
    array.class                    AS array_class,
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
  array.is_probeset_array=true
LIMIT 10000000,10000000;

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    array.format                   AS array_format,
    array.vendor                   AS array_vendor,
    array.description              AS array_descr,
    array.type                     AS array_type,
    array.class                    AS array_class,
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
  array.is_probeset_array=true
LIMIT 20000000,10000000;

INSERT INTO MTMP_probestuff_helper
select distinct
    array.name                     AS array_name,
    array.format                   AS array_format,
    array.vendor                   AS array_vendor,
    array.description              AS array_descr,
    array.type                     AS array_type,
    array.class                    AS array_class,
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

ANALYZE TABLE MTMP_probestuff_helper; /* OPTIMIZE may be too slow */
