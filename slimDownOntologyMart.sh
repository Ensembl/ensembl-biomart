#!/bin/bash --

MY_DIR=`dirname $0`
source $MY_DIR/../SetEnv

echo "Slim down the egontology_mart tables to reference only those in the"
echo "specified ontology."

c1=`mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT $DBNAMEMART -s -N -e "SHOW TABLES;"`

for o in ${c1}
do
  echo ${o}
  if [[ ${o} == closure* ]]
  then
    echo "  DELETE FROM ${o} where name_302 IS NULL;"
    c2=`mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT $DBNAMEMART -s -N -e "DELETE FROM ${o} where name_302 IS NULL;"`
    
    echo "  OPTIMIZE TABLE ${o};"
    c3=`mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT $DBNAMEMART -s -N -e "OPTIMIZE TABLE ${o};"`
    
    #echo "  SELECT COUNT(*) FROM ${o};"
    c4=`mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT $DBNAMEMART -s -N -e "SELECT COUNT(*) FROM ${o};"`

    if [ ${c4} -eq 0 ]
    then
      echo "  DROP TABLE ${o};"
      c5=`mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT $DBNAMEMART -s -N -e "DROP TABLE ${o};"`
    fi
  fi
done
