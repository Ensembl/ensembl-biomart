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
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
    -e 'show tables like "%structvar%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_snp_som" ]
  then
     mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
    -e 'show tables like "%snp%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u
  
  elif [ ${template_type} == "hsap_strucvar_som" ]
  then
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
   -e 'show tables like "%structvar%som%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "external_feature" ]
  then
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
   -e 'show tables like "%external_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "segmentation_feature" ]
  then
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
   -e 'show tables like "%segmentation_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "regulatory_feature" ]
  then
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
   -e 'show tables like "%regulatory_feature%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u

  elif [ ${template_type} == "default" ] 
  then
  mysql -h mysql-eg-staging-1 -P 4160 -u ensro -BN \
    -e 'show tables like "%main"' ${mart} |
  sed -e 's/_.*//' -e '/meta/d' |
  sort -u 
  fi
}


scriptdir=$( dirname $0 )
OLD_MART=http://ens-prod-1.ebi.ac.uk:10301/biomart/martservice
NEW_MART=http://ens-prod-1.ebi.ac.uk:10301/biomart/martservice

for division in "plants" "metazoa" "protists" "fungi" 
do 

release=35
oldvs="${division}_mart_$(( release - 1 ))"
newvs="${division}_mart_${release}"
mart_type="default"

cat >>diffAllMarts_eg.log <<EOT
========================================================================
${division} GENE MART
========================================================================
EOT
for ds in $( get_species ${division}_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${division}_mart_${release} ${ds}_eg_gene $OLD_MART \
    ${newvs} ${division}_mart_${release} ${ds}_eg_gene $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts_eg.log

cat >>diffAllMarts_eg.log <<EOT
========================================================================
${division} SNP MART
========================================================================
EOT
for ds in $( get_species ${division}_snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${division}_snp_mart_${release} ${ds}_eg_snp $OLD_MART \
    ${newvs} ${division}_snp_mart_${release} ${ds}_eg_snp $NEW_MART

  if let $?; then
    print -u2 "Problem"
    exit 1
  fi

done >>diffAllMarts_eg.log

cat >>diffAllMarts_eg.log <<EOT

========================================================================
${division} SNP MART - STRUCVAR
========================================================================
EOT
mart_type="structvar"
for ds in $( get_species ${division}_snp_mart_${release} ${mart_type}); do

  $scriptdir/diffMart.sh \
    ${oldvs} ${division}_snp_mart_${release} ${ds}_eg_strucvar $OLD_MART \
    ${newvs} ${division}_snp_mart_${release} ${ds}_eg_strucvar $NEW_MART
  if let $?; then
    print -u2 "Problem"
    exit 1
  fi  

done >>diffAllMarts_eg.log

done
