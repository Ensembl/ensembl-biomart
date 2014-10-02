#!/bin/bash --

DBUSER=
DBPASS=
DBHOST=
DBPORT=
DBNAMEMART=

if ( ! getopts "HpuPm" opt); then
  echo "Usage: `basename $0` -H HOST -p PORT -u USER -P PASSWORD -m DB_MART";
  exit 10
fi

while getopts ":H:p:u:P:m:" opt; do
  case $opt in
    H)
      # Host
      DBHOST=$OPTARG
      ;;
    p)
      # Port
      DBPORT=$OPTARG
      ;;
    u)
      DBUSER=$OPTARG
      ;;
    P)
      DBPASS=$OPTARG
      ;;
    m)
      DBNAMEMART=$OPTARG
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 11
      ;;
  esac
done

if [ -z $DBHOST ] || [ -z $DBPORT ] || [ -z $DBUSER ] || [ -z $DBPASS ] || [ -z $DBNAMEMART ]
then
  echo "WARNING: Missing parameters"
  echo " -H : Host     = $DBHOST"
  echo " -p : Port     = $DBPORT"
  echo " -u : User     = $DBUSER"
  echo " -P : Password = $DBPASS"
  echo " -m : Mart     = $DBNAMEMART"
  exit 10
fi

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
