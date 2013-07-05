#!/bin/sh

# Note: /bin/sh doesn't work on Alphas (need to use bash thexre) but
# works everywhere else.

# Starts the MartExplorer GUI application.

# Usage:
#
# prompt> bin/martgui.sh
TMP_ROOT=`dirname $0`/..
 
TMP_CLASSPATH=${TMP_ROOT}
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/build/classes
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/mysql-connector-java-3.1.14-bin.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/log4j-1.2.6.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/jython.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ensj-util.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/jdom.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/martj.jar 
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/p6spy.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ojdbc14.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/ecp1_0beta.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${TMP_ROOT}/lib/postgresql-8.3-604.jdbc3.jar
TMP_CLASSPATH=${TMP_CLASSPATH}:${CLASSPATH}

# Note: If you get Java "Out of memory" errors, try increasing the numbers
# in the -Xmx and -Xms parameters in the java command below. For performance
# sake it is best if they are both the same value.
java -Xmx128m -Xms128m -cp $TMP_CLASSPATH org.ensembl.mart.example.SimpleLibraryUsageExample




