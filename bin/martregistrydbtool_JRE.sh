#!/bin/sh

# Note: /bin/sh doesn't work on Alphas (need to use bash thexre) but
# works everywhere else.

# Starts the interactive Mart Shell program.

# Usage:
#
# prompt> bin/martshell.sh

# unless python.cachedir is set in ~/.jython a cachedir will be
# automatically created. The cache speeds up the shell. It can be
# safely deleted but will be automatically recreated next time you run
# this program.

CACHE_DIR=${HOME}/.martshell_cachedir

TMP_ROOT=`dirname $0`/..

TMP_CLASSPATH=${TMP_ROOT}
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/build/classes 
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/martj.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/mysql-connector-java-3.1.14-bin.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/java-getopt-1.0.9.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/jdom.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/libreadline-java.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ensj-util.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ecp1_0beta.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/jdbc2_0-stdext.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ojdbc14.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/postgresql-8.3-604.jdbc3.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${CLASSPATH}

PLATFORM=`uname -ms`
case "$PLATFORM" in
[Ll]inux*)
  JAVA="${TMP_ROOT}/jre/linux/bin/java"
  ;;
*alpha*)
  JAVA="${TMP_ROOT}/jre/alpha/bin/java"
  ;;
*)
  echo "warning, this platform is not known to be supported, using linux libraries\n"
  JAVA="${TMP_ROOT}/jre/linux/bin/java"
  ;;
esac

# Note: If you get Java "Out of memory" errors, try increasing the numbers
# in the -Xmx and -Xms parameters in the java command below. For performance
# sake it is best if they are both the same value.

$JAVA -Xmx128m -Xms128m -ea -classpath ${TMP_CLASSPATH} org.ensembl.mart.util.MartRegistryDBTool $@
