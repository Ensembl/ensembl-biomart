alter table kegg__gene__main change gene_id gene_id_key int(11);
alter table kegg__gene__main add index(gene_id_key);
alter table kegg__ec__dm change gene_id gene_id_key int(11);
alter table kegg__ec__dm add index(gene_id_key);
alter table kegg__pfam__dm change gene_id gene_id_key int(11);
alter table kegg__pfam__dm add index(gene_id_key);
alter table kegg__path__dm change gene_id gene_id_key int(11);
alter table kegg__path__dm add index(gene_id_key);
