CREATE TABLE TEMP0  SELECT gene.gname,gene.gene_id,gene.fref,gene.fstart,gene.fstop,gene.fstrand,gene.note,gene.name,gene.KO,gene.gene,temp_temp.gene_id AS gene_id_TEMP0 FROM gene LEFT JOIN temp_temp ON temp_temp.gene_id = gene.gene_id;
ALTER TABLE TEMP0 ADD INDEX (gene_id);


CREATE TABLE kegg__path__dm  SELECT path.gname,gene_path_link_left.gene_id,gene_path_link_left.gname AS gname_DTEMP0 FROM gene_path_link_left LEFT JOIN path ON path.gname = gene_path_link_left.gname;
ALTER TABLE kegg__path__dm ADD INDEX (gname);


CREATE TABLE kegg__ec__dm  SELECT ec.gname,gene_ec_link_left.gene_id,gene_ec_link_left.gname AS gname_DTEMP0 FROM gene_ec_link_left LEFT JOIN ec ON ec.gname = gene_ec_link_left.gname;
ALTER TABLE kegg__ec__dm ADD INDEX (gname);


CREATE TABLE kegg__pfam__dm  SELECT pfam.gname,pfam.pfam_id,pfam.fref,pfam.fstart,pfam.fstop,pfam.fstrand,pfam.note,pfam.name,gene_pfam_link_left.gene_id,gene_pfam_link_left.pfam_id AS pfam_id_DTEMP0 FROM gene_pfam_link_left LEFT JOIN pfam ON pfam.pfam_id = gene_pfam_link_left.pfam_id;
ALTER TABLE kegg__pfam__dm ADD INDEX (pfam_id);


CREATE TABLE CTEMP0 SELECT DISTINCT gene_id FROM kegg__path__dm WHERE gname IS NOT NULL;
ALTER TABLE CTEMP0 ADD INDEX (gene_id);
CREATE TABLE CCTEMP01  SELECT TEMP0.gname,TEMP0.gene_id,TEMP0.fref,TEMP0.fstart,TEMP0.fstop,TEMP0.fstrand,TEMP0.note,TEMP0.name,TEMP0.KO,TEMP0.gene,TEMP0.gene_id_TEMP0,CTEMP0.gene_id AS gene_path_link_left_bool FROM TEMP0 LEFT JOIN CTEMP0 ON CTEMP0.gene_id = TEMP0.gene_id;
ALTER TABLE CCTEMP01 ADD INDEX (gene_id);
CREATE TABLE CCCTEMP012 SELECT DISTINCT gene_id FROM kegg__ec__dm WHERE gname IS NOT NULL;
ALTER TABLE CCCTEMP012 ADD INDEX (gene_id);
CREATE TABLE CCCCTEMP0123  SELECT CCTEMP01.gname,CCTEMP01.gene_id,CCTEMP01.fref,CCTEMP01.fstart,CCTEMP01.fstop,CCTEMP01.fstrand,CCTEMP01.note,CCTEMP01.name,CCTEMP01.KO,CCTEMP01.gene,CCTEMP01.gene_id_TEMP0,CCTEMP01.gene_path_link_left_bool,CCCTEMP012.gene_id AS gene_ec_link_left_bool FROM CCTEMP01 LEFT JOIN CCCTEMP012 ON CCCTEMP012.gene_id = CCTEMP01.gene_id;
ALTER TABLE CCCCTEMP0123 ADD INDEX (gene_id);
CREATE TABLE CCCCCTEMP01234 SELECT DISTINCT gene_id FROM kegg__pfam__dm WHERE pfam_id IS NOT NULL;
ALTER TABLE CCCCCTEMP01234 ADD INDEX (gene_id);
CREATE TABLE kegg__gene__main  SELECT CCCCTEMP0123.gname,CCCCTEMP0123.gene_id,CCCCTEMP0123.fref,CCCCTEMP0123.fstart,CCCCTEMP0123.fstop,CCCCTEMP0123.fstrand,CCCCTEMP0123.note,CCCCTEMP0123.name,CCCCTEMP0123.KO,CCCCTEMP0123.gene,CCCCTEMP0123.gene_id_TEMP0,CCCCTEMP0123.gene_path_link_left_bool,CCCCTEMP0123.gene_ec_link_left_bool,CCCCCTEMP01234.gene_id AS gene_pfam_link_left_bool FROM CCCCTEMP0123 LEFT JOIN CCCCCTEMP01234 ON CCCCCTEMP01234.gene_id = CCCCTEMP0123.gene_id;
ALTER TABLE kegg__gene__main ADD INDEX (gene_id);
drop table CTEMP0;
drop table CCTEMP01;
drop table CCCTEMP012;
drop table CCCCTEMP0123;
drop table CCCCCTEMP01234;

