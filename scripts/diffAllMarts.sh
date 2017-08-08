#!/bin/ksh
# Copyright [1999-2013] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


SERVER=$1
RELEASE=$2
EG_RELEASE=$3
GRCH37=$4

function get_species
{
  mart=$1
  template_type=$2

  if [ ${template_type} == "structvar" ]
  then
  mysql $($SERVER details mysql) -BN \
    -e 'show tables like "%structvar%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_snp_som" ]
  then
     mysql $($SERVER details mysql) -BN \
    -e 'show tables like "%snp%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_strucvar_som" ]
  then
  mysql $($SERVER details mysql) -BN \
   -e 'show tables like "%structvar%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "external_feature" ]
  then
  mysql $($SERVER details mysql) -BN \
   -e 'show tables like "%external_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "segmentation_feature" ]
  then
  mysql $($SERVER details mysql) -BN \
   -e 'show tables like "%segmentation_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "regulatory_feature" ]
  then
  mysql $($SERVER details mysql) -BN \
   -e 'show tables like "%regulatory_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "default" ] 
  then
  mysql $($SERVER details mysql) -BN \
    -e 'show tables like "%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u 
  fi
}



scriptdir=$( dirname $0 )
NEW_MART=http://ens-prod-1.ebi.ac.uk:10301/biomart/martservice


for division in "ensembl" "plants" "metazoa" "protists" "fungi" 
do 

if [ ${division} == "ensembl" ]
then
  prefix=
  suffix=
  OLD_MART=http://www.ensembl.org/biomart/martservice
  oldvs="default"
  release=$RELEASE
else
  prefix="${division}_"
  suffix="_eg"
  OLD_MART=http://${division}.ensembl.org/biomart/martservice
  release=$EG_RELEASE
  oldvs="${division}_mart"
fi

newvs="${division}_mart_${release}"
mart_type="default"
filename="diffAllMarts_${division}.log"

if [ ${GRCH37} == "1" ]
then
  OLD_MART=http://grch37.ensembl.org/biomart/martservice
  newvs="${division}_mart_${release}_GRCh37"
  filename="diffAllMarts_${division}_GRCh37.log"
fi

if [ ${division} == "protists" ]
then
  oldvs="protist_mart"
elif [ ${division} == "fungi" ]
then
  oldvs="fungal_mart"
fi

cat >$filename <<EOT
========================================================================
${division} GENE MART
========================================================================
EOT
if [ ${division} == "ensembl" ]
then
  new_gene_mart="ensembl_mart_${release}"
  old_gene_mart="ensembl_mart_$(( release - 1 ))"
  gene_mart_suffix="_ensembl"
else
  new_gene_mart="${prefix}mart_${release}"
  old_gene_mart="${prefix}mart_$(( release - 1 ))"
  gene_mart_suffix=
fi
for ds in $( get_species $new_gene_mart ${mart_type}); do
  $scriptdir/diffMart.sh \
    ${oldvs} $old_gene_mart ${ds}${suffix}_gene${gene_mart_suffix} $OLD_MART \
    ${newvs} $new_gene_mart ${ds}${suffix}_gene${gene_mart_suffix} $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT
========================================================================
${division} MOUSE MART
========================================================================
EOT
new_mouse_mart="mouse_mart_${release}"
old_mouse_mart="mouse_mart_$(( release - 1 ))"
mouse_mart_suffix="_ensembl"
for ds in $( get_species $new_mouse_mart ${mart_type}); do
  $scriptdir/diffMart.sh \
    ${oldvs} $old_mouse_mart ${ds}${suffix}_gene${mouse_mart_suffix} $OLD_MART \
    ${newvs} $new_mouse_mart ${ds}${suffix}_gene${mouse_mart_suffix} $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT
========================================================================
${division} SNP MART
========================================================================
EOT
for ds in $( get_species ${prefix}snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${prefix}snp_mart_$(( release - 1 )) ${ds}${suffix}_snp $OLD_MART \
    ${newvs} ${prefix}snp_mart_${release} ${ds}${suffix}_snp $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT

========================================================================
${division} SNP MART - STRUCVAR
========================================================================
EOT
mart_type="structvar"
for ds in $( get_species ${prefix}snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${prefix}snp_mart_$(( release - 1 )) ${ds}${suffix}_strucvar $OLD_MART \
    ${newvs} ${prefix}snp_mart_${release} ${ds}${suffix}_strucvar $NEW_MART
  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>$filename


if [ ${division} == "ensembl" ]
then
cat >>$filename <<EOT

========================================================================
SNP MART - hsapiens SOM
========================================================================
EOT

mart_type="hsap_snp_som"
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${prefix}snp_mart_$(( release - 1 )) ${ds}_snp_som $OLD_MART \
    ${newvs} ${prefix}snp_mart_${release} ${ds}_snp_som $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>$filename

cat >>$filename <<EOT

========================================================================
SNP MART - hsapiens STRUCVAR  SOM
========================================================================
EOT
mart_type="hsap_strucvar_som"
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${prefix}snp_mart_$(( release - 1 )) ${ds}_snp_strucvar_som $OLD_MART \
    ${newvs} ${prefix}snp_mart_${release} ${ds}_snp_strucvar_som $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>$filename

cat >>$filename <<EOT


========================================================================
REGULATION MART - annotated feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulation_mart_$(( release - 1 )) ${ds}_annotated_feature $OLD_MART \
    ${newvs} regulation_mart_${release} ${ds}_annotated_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT

========================================================================
REGULATION MART - external feature
========================================================================
EOT
mart_type="external_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulation_mart_$(( release - 1 )) ${ds}_external_feature $OLD_MART \
    ${newvs} regulation_mart_${release} ${ds}_external_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT

========================================================================
REGULATION MART - regulatory feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulation_mart_$(( release - 1 )) ${ds}_regulatory_feature $OLD_MART \
    ${newvs} regulation_mart_${release} ${ds}_regulatory_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT

========================================================================
REGULATION MART - miRNA target feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulation_mart_$(( release - 1 )) ${ds}_mirna_target_feature $OLD_MART \
    ${newvs} regulation_mart_${release} ${ds}_mirna_target_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

cat >>$filename <<EOT

========================================================================
REGULATION MART - motif feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulation_mart_$(( release - 1 )) ${ds}_motif_feature $OLD_MART \
    ${newvs} regulation_mart_${release} ${ds}_motif_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>$filename

fi


done
