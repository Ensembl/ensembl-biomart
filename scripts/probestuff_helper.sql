
-- Look at existing MTMP tables, if any
SELECT
  COUNT(*) AS NUM_MTMP_TABLES
  #TABLE_NAME, TABLE_TYPE, TABLE_ROWS, CREATE_TIME, UPDATE_TIME
FROM
  information_schema.TABLES
WHERE
  TABLE_SCHEMA = DATABASE()
AND
  TABLE_NAME RLIKE 'MTMP'
;

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
SELECT DISTINCT
    a.name          AS array_name,
    a.format        AS array_format,
    a.vendor        AS array_vendor,
    a.description   AS array_descr,
    a.type          AS array_type,
    a.class         AS array_class,
    x.dbprimary_acc AS transcript_stable_id,
    ps.name         AS display_label,
    CONCAT(a.vendor, '_', REPLACE(REPLACE(a.name, '-', '_'), '.', '_'))
                    AS array_vendor_and_name
FROM    array a,
        probe_set ps,
        probe p,
        array_chip ac,
        xref x,
        object_xref ox
WHERE   a.array_id          = ac.array_id
  AND   ac.array_chip_id    = p.array_chip_id
  AND   p.probe_set_id      = ps.probe_set_id
  AND   p.probe_set_id      = ox.ensembl_id
  AND   ox.xref_id          = x.xref_id
  AND   ox.ensembl_object_type  = 'ProbeSet'
LIMIT 0,10000000;

INSERT INTO MTMP_probestuff_helper
SELECT DISTINCT
    a.name          AS array_name,
    a.format        AS array_format,
    a.vendor        AS array_vendor,
    a.description   AS array_descr,
    a.type          AS array_type,
    a.class         AS array_class,
    x.dbprimary_acc AS transcript_stable_id,
    ps.name         AS display_label,
    CONCAT(a.vendor, '_', REPLACE(REPLACE(a.name, '-', '_'), '.', '_'))
                    AS array_vendor_and_name
FROM    array a,
        probe_set ps,
        probe p,
        array_chip ac,
        xref x,
        object_xref ox
WHERE   a.array_id          = ac.array_id
  AND   ac.array_chip_id    = p.array_chip_id
  AND   p.probe_set_id      = ps.probe_set_id
  AND   p.probe_set_id      = ox.ensembl_id
  AND   ox.xref_id          = x.xref_id
  AND   ox.ensembl_object_type  = 'ProbeSet'
LIMIT 10000000,10000000;

INSERT INTO MTMP_probestuff_helper
SELECT DISTINCT
    a.name          AS array_name,
    a.format        AS array_format,
    a.vendor        AS array_vendor,
    a.description   AS array_descr,
    a.type          AS array_type,
    a.class         AS array_class,
    x.dbprimary_acc AS transcript_stable_id,
    ps.name         AS display_label,
    CONCAT(a.vendor, '_', REPLACE(REPLACE(a.name, '-', '_'), '.', '_'))
                    AS array_vendor_and_name
FROM    array a,
        probe_set ps,
        probe p,
        array_chip ac,
        xref x,
        object_xref ox
WHERE   a.array_id          = ac.array_id
  AND   ac.array_chip_id    = p.array_chip_id
  AND   p.probe_set_id      = ps.probe_set_id
  AND   p.probe_set_id      = ox.ensembl_id
  AND   ox.xref_id          = x.xref_id
  AND   ox.ensembl_object_type  = 'ProbeSet'
LIMIT 20000000,10000000;

INSERT INTO MTMP_probestuff_helper
SELECT DISTINCT
    a.name          AS array_name,
    a.format        AS array_format,
    a.vendor        AS array_vendor,
    a.description   AS array_descr,
    a.type          AS array_type,
    a.class         AS array_class,
    x.dbprimary_acc AS transcript_stable_id,
    p.name          AS display_label,
    CONCAT(a.vendor, '_', REPLACE(REPLACE(a.name, '-', '_'), '.', '_'))
                    AS array_vendor_and_name
FROM    array a,
        probe p,
        array_chip ac,
        xref x,
        object_xref ox
WHERE   a.array_id          = ac.array_id
  AND   ac.array_chip_id    = p.array_chip_id
  AND   p.probe_set_id      IS NULL
  AND   p.probe_id          = ox.ensembl_id
  AND   ox.xref_id          = x.xref_id
  AND   ox.ensembl_object_type  = 'Probe';

ALTER TABLE MTMP_probestuff_helper
  ADD INDEX transcript_idx (transcript_stable_id),
  ADD INDEX vendor_name_idx (array_vendor_and_name);

ANALYZE TABLE MTMP_probestuff_helper; /* OPTIMIZE may be too slow */
