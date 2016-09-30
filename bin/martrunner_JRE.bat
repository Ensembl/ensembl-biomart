REM Note: If you get Java "Out of memory" errors, try increasing the numbers
REM in the -Xmx and -Xms parameters in the java command below. For performance
REM sake it is best if they are both the same value.
..\jre\windows\bin\java -Xmx128m -Xms128m -ea -cp ..\build\classes;..\lib\martj.jar;..\lib\activation.jar;..\lib\dsn.jar;..\lib\imap.jar;..\lib\mailapi.jar;..\lib\pop3.jar;..\lib\smtp.jar;..\lib\log4j-1.2.6.jar;..\lib\ecp1_0beta.jar;..\lib\mysql-connector-java-3.1.14-bin.jar;..\lib\postgresql-8.3-604.jdbc3.jar;..\lib\ensj-util.jar;..\lib\jdom.jar;..\lib\ojdbc14.jar;.. org.biomart.runner.view.gui.MartRunner %1
