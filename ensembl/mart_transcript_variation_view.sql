CREATE VIEW MTMP_transcript_variation AS
SELECT *,
LEFT (REPLACE(
consequence_types, ',', '&')
, 255)

AS mart_consequence_type FROM transcript_variation;

