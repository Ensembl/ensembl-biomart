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


oldvs=$1
oldmart=$2
oldds=$3
oldurl=$4

newvs=$5
newmart=$6
newds=$7
newurl=$8

cat <<EOT

========================================================================
$oldds ($oldvs $oldmart) vs $newds ($newvs $newmart)
========================================================================
EOT

print -u2 "$oldds"

trap 'rm -f ${oldfile} ${newfile} ${s_oldfile} ${s_newfile}' TERM INT EXIT

for what in attributes filters; do
  cat <<EOT

${what}
-------------------------------

EOT

  oldfile=$(mktemp)
  newfile=$(mktemp)

  wget --retry-connrefused -O ${oldfile} \
    "$oldurl?type=${what}&mart=$oldmart&dataset=$oldds&virtualschema=$oldvs" >/dev/null 2>&1

  if [[ ! -s ${oldfile} ]]; then
    print -u2 "No ${what} in old mart!"
    exit 1
  fi

  wget --retry-connrefused -O ${newfile} \
    "$newurl?type=${what}&mart=$newmart&dataset=$newds&virtualschema=$newvs" >/dev/null 2>&1

  if [[ ! -s ${newfile} ]]; then
    print -u2 "No ${what} in new mart!"
    exit 1
  fi

  s_oldfile=$(mktemp)
  s_newfile=$(mktemp)

  sed 's/\(\w\+\).*/\1/' ${oldfile} | cut -f1 | sort -u -o ${s_oldfile}
  sed 's/\(\w\+\).*/\1/' ${newfile} | cut -f1 | sort -u -o ${s_newfile}

  diff -u -i ${s_oldfile} ${s_newfile} | sed -n '/^[+-]/p'

  rm -f ${oldfile} ${newfile}
  rm -f ${s_oldfile} ${s_newfile}
done
