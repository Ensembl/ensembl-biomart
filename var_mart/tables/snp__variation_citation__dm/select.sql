SELECT
  v.variation_id as variation_id_2025_key,
  p.authors as authors_20137,
  p.year as year_20137,
  concat(left(p.authors, instr(p.authors,",")-1), " et al. (", p.year, ")") as pub_short_ref,
  p.title as title_20137,
  p.pmid as pmid_20137,
  p.pmcid as pmcid_20137,
  p.ucsc_id as ucsc_id_20137,
  p.doi as doi_20137
FROM
  VAR_DB.publication p INNER JOIN
  VAR_DB.variation_citation vc ON p.publication_id = vc.publication_id RIGHT OUTER JOIN
  VAR_DB.variation v ON vc.variation_id = v.variation_id
