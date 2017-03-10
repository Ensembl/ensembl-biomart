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


function get_species
{
  mart=$1
  template_type=$2

  if [ ${template_type} == "structvar" ]
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
    -e 'show tables like "%structvar%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_snp_som" ]
  then
     mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
    -e 'show tables like "%snp%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_strucvar_som" ]
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
   -e 'show tables like "%structvar%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "external_feature" ]
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
   -e 'show tables like "%external_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "segmentation_feature" ]
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
   -e 'show tables like "%segmentation_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "regulatory_feature" ]
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
   -e 'show tables like "%regulatory_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "default" ] 
  then
  mysql -h mysql-ens-sta-1 -P 4519 -u ensro -BN \
    -e 'show tables like "%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u 
  fi
}



scriptdir=$( dirname $0 )
OLD_MART=http://ens-prod-1.ebi.ac.uk:10301/biomart/martservice
NEW_MART=http://ens-prod-1.ebi.ac.uk:10301/biomart/martservice

release=88
oldvs="ensembl_mart_$(( release - 1 ))"
newvs="ensembl_mart_${release}"
mart_type="default"

cat >diffAllMarts.log <<EOT
========================================================================
ENSEMBL MART
========================================================================
EOT
for ds in $( get_species ensembl_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ensembl ${ds}_gene_ensembl $OLD_MART \
    ${newvs} ensembl ${ds}_gene_ensembl $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT
========================================================================
MOUSE MART
========================================================================
EOT
for ds in $( get_species mouse_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} mouse ${ds}_gene_ensembl $OLD_MART \
    ${newvs} mouse ${ds}_gene_ensembl $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT
========================================================================
VEGA MART
========================================================================
EOT
for ds in $( get_species vega_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} vega ${ds}_gene_vega $OLD_MART \
    ${newvs} vega ${ds}_gene_vega $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT
========================================================================
SNP MART
========================================================================
EOT
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} snp ${ds}_snp $OLD_MART \
    ${newvs} snp ${ds}_snp $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
SNP MART - STRUCVAR
========================================================================
EOT
mart_type="structvar"
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} snp ${ds}_strucvar $OLD_MART \
    ${newvs} snp ${ds}_strucvar $NEW_MART
  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
SNP MART - hsapiens SOM
========================================================================
EOT

mart_type="hsap_snp_som"
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} snp ${ds}_snp_som $OLD_MART \
    ${newvs} snp ${ds}_snp_som $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
SNP MART - hsapiens STRUCVAR  SOM
========================================================================
EOT
mart_type="hsap_strucvar_som"
for ds in $( get_species snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} snp ${ds}_snp_strucvar_som $OLD_MART \
    ${newvs} snp ${ds}_snp_strucvar_som $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT


========================================================================
REGULATION MART - annotated feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} annotated_feature ${ds}_annotated_feature $OLD_MART \
    ${newvs} annotated_feature ${ds}_annotated_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
REGULATION MART - external feature
========================================================================
EOT
mart_type="external_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} external_feature ${ds}_external_feature $OLD_MART \
    ${newvs} external_feature ${ds}_external_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
REGULATION MART - regulatory feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} regulatory_feature ${ds}_regulatory_feature $OLD_MART \
    ${newvs} regulatory_feature ${ds}_regulatory_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
REGULATION MART - miRNA target feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} mirna_target_feature ${ds}_mirna_target_feature $OLD_MART \
    ${newvs} mirna_target_feature ${ds}_mirna_target_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
REGULATION MART - motif feature
========================================================================
EOT
mart_type="regulatory_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} motif_feature ${ds}_motif_feature $OLD_MART \
    ${newvs} motif_feature ${ds}_motif_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log

cat >>diffAllMarts.log <<EOT

========================================================================
REGULATION MART - segmentation feature
========================================================================
EOT
mart_type="segmentation_feature"
for ds in $( get_species regulation_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} segmentation_feature ${ds}_segmentation_feature $OLD_MART \
    ${newvs} segmentation_feature ${ds}_segmentation_feature $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts.log
