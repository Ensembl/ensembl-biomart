 SELECT 
    sv.structural_variation_id AS structural_variation_id_2072_key, 
    REPLACE(REPLACE(p.description, ",", ""), "&", "") as phenotype_name,
    s.name AS phenotype_source
  FROM 
    VAR_DB.structural_variation sv LEFT OUTER JOIN 
    VAR_DB.phenotype_feature pf ON (sv.variation_name=pf.object_id AND pf.type="StructuralVariation") LEFT OUTER JOIN 
    VAR_DB.phenotype p ON (p.phenotype_id=pf.phenotype_id) LEFT OUTER JOIN
    VAR_DB.source    s ON (pf.source_id=s.source_id)
