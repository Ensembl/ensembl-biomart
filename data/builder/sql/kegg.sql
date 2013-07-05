create table gene_temp select g.gname, d.fid as gene_id,d.fref,d.fstart,d.fstop,d.fstrand from kegg.fgroup g, kegg.fdata d where g.gid=d.gid and g.gclass ='gene' and d.ftypeid in (2,7,10);

create table gene_temp_temp select g.*,f.fattribute_value as note from gene_temp g left join kegg.fattribute_to_feature f on f.fid=g.gene_id and f.fattribute_id=1;

create table gene_ttt select g.*,f.fattribute_value as name from gene_temp_temp g left join kegg.fattribute_to_feature f on f.fid=g.gene_id and f.fattribute_id=2;

create table gene_tttt select g.*,f.fattribute_value as KO from gene_ttt g left join kegg.fattribute_to_feature f on f.fid=g.gene_id and f.fattribute_id=3;
                                                                                                                             
create table gene select g.*,f.fattribute_value as gene from gene_tttt g left join kegg.fattribute_to_feature f on f.fid=g.gene_id and f.fattribute_id=5;
                                                                                                                             

 

create table pfam_temp select g.gname, d.fid as pfam_id,d.fref,fstart,fstop,fstrand from kegg.fgroup g, kegg.fdata d where
g.gid=d.gid and g.gclass ='pfam';
 
create table pfam_temp_temp select g.*,f.fattribute_value as note from pfam_temp g left join kegg.fattribute_to_feature f on
f.fid=g.pfam_id and f.fattribute_id=1;
                                                                                                                             
create table pfam select g.*,f.fattribute_value as name from pfam_temp_temp g left join kegg.fattribute_to_feature f on f.fid=g.pfam_id and f.fattribute_id=2;
                                                                                                                             

create table path_temp_temp_temp select g.gname, d.fid ,d.fref,fstart,fstop,fstrand from kegg.fgroup g, kegg.fdata d where
g.gid=d.gid and g.gclass ='path';

#create table path_temp_temp select g.*,f.fattribute_value as note from path_temp g left join kegg.fattribute_to_feature f on
#f.fid=g.fid and f.fattribute_id=1;
                                                                                                                             
#create table path_temp_temp_temp select g.*,f.fattribute_value as name from path_temp_temp g left join kegg.fattribute_to_feature f on f.fid=g.fid and f.fattribute_id=2;
                                                                                                                             

 
create table ec_temp_temp_temp select g.gname, d.fid ,d.fref,fstart,fstop,fstrand from  kegg.fgroup g, kegg.fdata d where  g.gid=d.gid and g.gclass ='EC' ; 

#create table ec_temp_temp select g.*,f.fattribute_value as note from ec_temp g left join kegg.fattribute_to_feature f on
#f.fid=g.fid and f.fattribute_id=1;
                                                                                                                             
#create table ec_temp_temp_temp select g.*,f.fattribute_value as name from ec_temp_temp g left join kegg.fattribute_to_feature f on f.fid=g.fid and f.fattribute_id=2;
                                                                                                                             
                                                                                                                              
create table gene_pfam_link select g.gene_id, p.pfam_id from gene g, pfam_temp_temp p where g.fref=p.fref and g.fstart <=p.fstart and g.fstop>=p.fstop and g.fstrand=p.fstrand;
 


create table gene_path_link_temp select g.gene_id, p.gname from gene g, path_temp_temp_temp p where g.fref=p.fref and g.fstart <=p.fstart and g.fstop>=p.fstop and g.fstrand=p.fstrand;

create table gene_path_link select distinct * from gene_path_link_temp;

create table gene_ec_link_temp select g.gene_id, p.gname from gene g, ec_temp_temp_temp p where g.fref=p.fref and g.fstart <=p.fstart and g.fstop>=p.fstop and g.fstrand=p.fstrand;

create table gene_ec_link select distinct * from gene_ec_link_temp; 

#create table path select gname,note,name from path_temp_temp_temp group by gname, note,name;
#create table ec select gname,note, name from ec_temp_temp_temp group by gname, note,name;

create table path select gname from path_temp_temp_temp group by gname;
create table ec select gname from ec_temp_temp_temp group by gname;


create table temp_temp select gene_id from gene;

alter table gene modify gene_id int(11) primary key;
alter table pfam modify pfam_id int(11) primary key;
alter table path modify gname varchar(100) primary key;
alter table ec modify gname varchar(100) primary key;

alter table gene_ec_link add key(gname);
alter table gene_path_link add key(gname);
alter table gene_pfam_link add key(pfam_id);
alter table temp_temp add key(gene_id);

alter table gene_path_link add index (gene_id);
alter table gene_ec_link add index (gene_id);
alter table gene_pfam_link add index (gene_id);

create table gene_path_link_left select g.gene_id, l.gname from gene g left join gene_path_link l on g.gene_id=l.gene_id;
create table gene_ec_link_left select g.gene_id, l.gname from gene g left join gene_ec_link l on g.gene_id=l.gene_id;
create table gene_pfam_link_left select g.gene_id, l.pfam_id from gene g left join gene_pfam_link l on g.gene_id=l.gene_id;
 

create table meta_configuration (internalName varchar(100), displayName varchar(100), dataset varchar(100), description varchar(200), xml longblob, compressed_xml longblob, MessageDigest blob);

 
