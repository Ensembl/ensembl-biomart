/*
	Copyright (C) 2003 EBI, GRL

	This library is free software; you can redistribute it and/or
	modify it under the terms of the GNU Lesser General Public
	License as published by the Free Software Foundation; either
	version 2.1 of the License, or (at your option) any later version.

	This library is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
	Lesser General Public License for more details.

	You should have received a copy of the GNU Lesser General Public
	License along with this library; if not, write to the Free Software
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.lib;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

//import javax.naming.ConfigurationException;
import javax.sql.DataSource;
import javax.swing.JOptionPane;

import org.ewin.common.util.Log;
import org.ewin.javax.sql.DefaultPoolingAlgorithm;
import org.ewin.javax.sql.DriverManagerDataSource;
import org.ewin.javax.sql.PoolingAlgorithmDataSource;

/**
 * Datasource with extra functionality:
 * 
 * <ul>
 * <li> parameters are available via getters.
 * <li> lazy loads connection (useful when used offline and connection not needed)
 * <li> offers connection pooling
 * <li> implements toString() which prints something user friendly. 
 * </ul>
 * 
 * Users should <code>Connection conn = dataSource.getConnection()</code> 
 * to retrieve a connection from the pool and <code>conn.close()</code> to return it to the pool.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */
public class DetailedDataSource implements DataSource {

  // Disable logging from ewin connection pool package
  static {

    //temporarily disable stdout
    PrintStream n = new PrintStream(new ByteArrayOutputStream());
    PrintStream o = new PrintStream(System.out);

    System.setOut(n);

    // Configure Ewin logging - remove all defined loggers
    Iterator ewinLoggers = Log.loggers();
    while (ewinLoggers.hasNext()) {
      Log.removeLogger((Log.Logger) ewinLoggers.next());
    }

    //reset stdout
    System.setOut(o);

  }

  private final static Logger logger =
    Logger.getLogger(DetailedDataSource.class.getName());

  public static final String DEFAULTDATABASETYPE = "mysql";
  public static final String DEFAULTDRIVER = "com.mysql.jdbc.Driver";
  public static final int DEFAULTPOOLSIZE = 10;
  public static final String DEFAULTPORT = "3306";
  public static final String VERSION = "0.4";
  private static final String ORACLEAT = "@";
  public static final String ORACLE = "oracle";
  public static final String POSTGRES = "postgres";
  public static final String ORACLEDRIVER = "oracle.jdbc.driver.OracleDriver";
  public static final String POSTGRESDRIVER = "org.postgresql.Driver";
  private static final String SYBASE = "sybase:Tds";
  private static final String SQLSERVER = "sqlserver";
  private static final String SQLSERVERDRIVER = "net.sourceforge.jtds.jdbc.Driver";
  

  private String databaseType;
  private String host;
  private String port;
  private String databaseName;
  private String schema;
  private int maxPoolSize;
  private String password;
  private String user;
  private String martUser;
  private String jdbcDriverClassName;
  private String name;
  private DataSource dataSource;
  private String connectionString;

  /**
   * Creates a datasource backed by a connection pool. connectionString should 
   * match the host, port, and dbType.
   *  
   * @param dbType database type e.g. mysql.
   * @param host host name e.g. ensembldb.ensembl.org
   * @param port port number. e.g. 3306.
   * @param database name of database on database server, can be null for "meta" queries e.g. what databasea are available  
   * @param connectionString database connectionString, e.g. jdbc:mysql://ensembldb.ensembl.org:3036
   * @param user username
   * @param password password, can be null
   * @param maxPoolSize maximum poolsize
   * @param jdbcDriverClassName name of jdbc driver to back the datasource.
   * @param displayName displayName for datasource, if null a default is set
   **/
  public DetailedDataSource(
    String dbType,
    String host,
    String port,
    String databaseName,
	String schema,
    String connectionString,
    String user,
    String martUser,
    String password,
    int maxPoolSize,
    String jdbcDriverClassName,
    String displayName) {

	  /**
    assert dbType != null : "dbType is null";
    assert host != null : "host is null";
    assert port != null : "port is null";
    assert connectionString != null : "connectionString is null";
    assert databaseName == null
      || connectionString.indexOf(databaseName)
        != -1 : "database is null or is not in connection string";
    
    assert schema != null : "schema is null";
    
    assert user != null : "user is null";
    assert maxPoolSize >= 0;
    assert jdbcDriverClassName != null : "jdbcDriver is null";
	   */
	  
    this.databaseType = dbType;
    this.host = host;
    this.port = port;
    this.databaseName = databaseName;
    this.schema = schema;
    this.connectionString = connectionString;
    this.user = user;
    this.martUser = martUser;
    this.password = password;
    this.maxPoolSize = maxPoolSize;
    this.jdbcDriverClassName = jdbcDriverClassName;
    this.name = displayName;
    if ( this.name==null ) this.name = defaultName();
    
    //logger.warning(this.toString());
  }

  /**
   * Creates a datasource backed by a connection pool. connectionString should 
   * match the host, port, and dbType. Sets default displayName.
   *  
   * @param dbType database type e.g. mysql.
   * @param host host name e.g. ensembldb.ensembl.org
   * @param port port number. e.g. 3306.
   * @param database name of database on database server, can be null for "meta" queries e.g. what databasea are available  
   * @param connectionString database connectionString, e.g. jdbc:mysql://ensembldb.ensembl.org:3036
   * @param user username
   * @param password password, can be null
   * @param maxPoolSize maximum poolsize
   * @param jdbcDriverClassName name of jdbc driver to back the datasource.
   **/
  public DetailedDataSource(
    String dbType,
    String host,
    String port,
    String databaseName,
	String schema,
    String connectionString,
    String user,
    String password,
    int maxPoolSize,
    String jdbcDriverClassName) {
  	this(dbType, host,port, databaseName, schema,connectionString, user, "", password, maxPoolSize, jdbcDriverClassName, null);
  
  }
  

  
  /**
   * Convenience method which calls createDataSource(DEFAULTDATABASETYPE, host, DEFAULTPORT, database, user, password, DEFAULTPOOLSIZE, DEFAULTDRIVER);
   * 
   * @param host host name e.g. ensembldb.ensembl.org
   * @param database name of database on database server  
   * @param user username
   * @param password password, can be null
   * @throws ConfigurationException thrown if a problem occurs creating the datasource
   */
  public DetailedDataSource(
    String host,
    String database,
	String schema,
    String user,
    String password) {
    this(
      DEFAULTDATABASETYPE,
      host,
      DEFAULTPORT,
      database,
	  schema,
      user,
      password,
      DEFAULTPOOLSIZE,
      DEFAULTDRIVER,
      null); 
    
  }

  /**
   * Datasource constructed by specific parameters. The connection is automatically
   * derived from the host, port and databaseType.
   * 
   * @param databaseType database type e.g. mysql.
   * @param host host name e.g. ensembldb.ensembl.org
   * @param port port number. e.g. 3306.
   * @param database name of database on database server  
   * @param user username
   * @param password password, can be null
   * @param maxPoolSize maximum poolsize.
   * @param jdbcDriverClassName name of jdbc driver to back the datasource.
   */
  public DetailedDataSource(
    String databaseType,
    String host,
    String port,
    String database,
	String schema,
    String user,
    String password,
    int maxPoolSize,
    String jdbcDriverClassName) {
  	
      this(databaseType, host, port, database, schema,user, password, maxPoolSize, jdbcDriverClassName, defaultName(host, port, database, schema,user));
      
  }
    
  /**
   * Datasource constructed by specific parameters. The connection is automatically
   * derived from the host, port and databaseType.
   * 
   * @param databaseType database type e.g. mysql.
   * @param host host name e.g. ensembldb.ensembl.org
   * @param port port number. e.g. 3306.
   * @param database name of database on database server  
   * @param user username
   * @param password password, can be null
   * @param maxPoolSize maximum poolsize.
   * @param jdbcDriverClassName name of jdbc driver to back the datasource.
   * @param displayName name to give to DetailedDataSource
   */
  public DetailedDataSource(
    String databaseType,
    String host,
    String port,
    String database,
	String schema,
    String user,
    String password,
    int maxPoolSize,
    String jdbcDriverClassName,
    String displayName) {

    this(
      databaseType,
      host,
      port,
      database,
	  schema,
      connectionURL(databaseType, host, port, database),
      user,
      "",
      password,
      maxPoolSize,
      jdbcDriverClassName, 
      displayName);

  }

  /**
   * Returns a connection URL for jdbc.  This could differ from RDBMS to RDBMS.
   * Currently supports oracle, mysql, postgres and any other
   * database whose connnection URL syntax matches one of these. The dbType will produce
   * different connection strings:
   * <ul>
   * <li>oracle:thin --> jdbc:oracle:thin:@host:port:dbname
   * <li>sybase:Tds --> jdbc:sybase:Tds:host:port/dbname
   * <li>postgresSQL/mySQL --> jdbc:x://host:port/dbname
   * <li>MS SQL Server --> jdbc:jtds:sqlserver://host:port/dbname
   * </ul>
   * 
   * @param databaseType database type e.g. mysql.
   * @param host host name e.g. ensembldb.ensembl.org
   * @param port port number. e.g. 3306.
   * @param databaseName of database on database server  
   * @return String connectionURL
   */
  public static String connectionURL(
    String dbType,
    String host,
    String port,
    String databaseName) {

    if (dbType.equals(ORACLE)) {
      host = ORACLEAT + host;
      databaseName = ":" + databaseName;
    } else if (dbType.equals(SYBASE)) {
      databaseName = "/" + databaseName;
    } else {
      host = "//" + host;
      databaseName = "/" + databaseName;
    }

    StringBuffer dbURL = new StringBuffer();
    //defaults to oracle:thin, the driver settings needs to be done nicer
    if (dbType.equals(ORACLE))
    	dbType="oracle:thin";
    else if (dbType.equals(SQLSERVER))
    	dbType="jtds:sqlserver";
    else if (dbType.equals(POSTGRES))
    	dbType="postgresql";
    
    dbURL.append("jdbc:").append(dbType).append(":");
    dbURL.append(host);
    if (port != null && !"".equals(port))
      dbURL.append(":").append(port);

    if (databaseName != null && !databaseName.equals(""))
      dbURL.append(databaseName);
    //System.out.println("CONNECTING: "+dbURL.toString());
    return dbURL.toString();
  }

  public static String getJDBCDriverClassNameFor(String databaseType) {
    if (databaseType == null)
      return null;
    else if (databaseType.equals(DEFAULTDATABASETYPE))
      return DEFAULTDRIVER;
    else if (databaseType.equals(ORACLE))
      return ORACLEDRIVER;
    else if (databaseType.equals(POSTGRES))
    	return POSTGRESDRIVER;
    else if (databaseType.equals(SQLSERVER))
    	return SQLSERVERDRIVER;
    else
      return null; //add new ones as needed
  }
 
  /**
   * Convenience method for closing a connection and handling any SQLException
   * by printing a stack trace.
   * 
   * @param conn connection to be closed, method does nothing if conn=null.
   */
  public static void close(Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public String[] databaseNames() throws SQLException {
    List databases = new ArrayList();

    Connection conn = getConnection();
    // TODO check this works with non-mysql databases
    ResultSet rs = conn.createStatement().executeQuery("show databases");
    while (rs.next()) {
      databases.add(rs.getString(1));
    }
    close(conn);

    return (String[]) databases.toArray(new String[databases.size()]);
  }

  /**
   * @return databaseName@host:port
   */
  public String defaultName() {
    return defaultName(host, port, databaseName,schema, user);
  }

  
  /**
   * @return databaseName@host:port
   */
  public static String defaultName(String host, String port, String databaseName, String schema,String user) {
    return user +"/" +databaseName + "@" + host + ":" + port;
  }

  
  /**
   * A connection pool is created when this merthod is first called
   * and then connections are returned from it.
   * 
   * @return Connection to the database specified.
   * @throws java.sql.SQLException if any problem occurs making the connection.
   */
  public Connection getConnectionNoVersionCheck() throws SQLException {

    if (dataSource == null) {
      try {
        // load driver
        
        //System.out.println ("lodading ..."+jdbcDriverClassName);
        Class.forName(jdbcDriverClassName).newInstance();
        dataSource =
          new DriverManagerDataSource(
            jdbcDriverClassName,
            connectionString,
            user,
            password);

        // Wrap data source in connection pool
        PoolingAlgorithmDataSource tmp =
          new PoolingAlgorithmDataSource(dataSource);
        DefaultPoolingAlgorithm poolAlgorithm = new DefaultPoolingAlgorithm();
        poolAlgorithm.setPoolMax(maxPoolSize);
        tmp.setPoolingAlgorithm(poolAlgorithm);

        dataSource = tmp;

      } catch (InstantiationException e) {
        e.printStackTrace();
        throw new SQLException(
          "Failed to initialise database connection pool "
            + "(is the connection pool jar available?) : ");
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw new SQLException(
          "Failed to initialise database connection pool for "
            + jdbcDriverClassName
            + " (is the connection pool jar available?) : ");
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new SQLException(
          "Failed to initialise database connection pool "
            + "(is the connection pool jar available?) : ");
      } catch (NoClassDefFoundError e) {
        e.printStackTrace();
        throw new SQLException(
          "Failed to initialise database connection pool "
            + "(is the connection pool jar available?) : ");
      }

    }
    

    	return dataSource.getConnection();	
  }
  
  /**
   * A connection pool is created when this merthod is first called
   * and then connections are returned from it.
   * 
   * @return Connection to the database specified.
   * @throws java.sql.SQLException if any problem occurs making the connection.
   */
  public Connection getConnection() throws SQLException {
	  try {
	  try {
		// load driver
		if (dataSource != null) {
				return dataSource.getConnection();
		}        
		//System.out.println ("lodading ..."+jdbcDriverClassName);
		Class.forName(jdbcDriverClassName).newInstance();
		dataSource =
		  new DriverManagerDataSource(
			jdbcDriverClassName,
			connectionString,
			user,
			password);

		// Wrap data source in connection pool
		PoolingAlgorithmDataSource tmp =
		  new PoolingAlgorithmDataSource(dataSource);
		DefaultPoolingAlgorithm poolAlgorithm = new DefaultPoolingAlgorithm();
		poolAlgorithm.setPoolMax(maxPoolSize);
		tmp.setPoolingAlgorithm(poolAlgorithm);

		dataSource = tmp;

		Connection conn;
		String version = null;
			//try {
 	    conn = dataSource.getConnection();
		
		// remove version check 
        /*  
		ResultSet vr = conn.getMetaData().getTables(conn.getCatalog(), this.schema, "meta_version__version__main", null);
		//expect at most one result, if no results, tcheck will remain null
		String tcheck = null;
		if (vr.next())
			tcheck = vr.getString(3);

		vr.close();

		if (tcheck == null) {// don't check databases with no version table yet
			return conn;
		}
		
		String[] schemas = null;
		if(getDatabaseType().equals("oracle")) schemas = getSchema().toUpperCase().split(";");
		else schemas = getSchema().split(";");
		
		// remove version check 
 
		PreparedStatement ps = conn.prepareStatement("select version from "+schemas[0]+".meta_version__version__main");
		ResultSet rs = ps.executeQuery();
		rs.next();
		version = rs.getString(1);
		rs.close();
		if (!version.equals(VERSION)){
			throw new SQLException("Database version "+version+" and software version "+VERSION+" do not match");	
		 }
		 */
		 return conn;


	  } catch (InstantiationException e) {
		e.printStackTrace();
		throw new SQLException(
		  "Failed to initialise database connection pool "
			+ "(is the connection pool jar available?) : ");
	  } catch (ClassNotFoundException e) {
		e.printStackTrace();
		throw new SQLException(
		  "Failed to initialise database connection pool for "
			+ jdbcDriverClassName
			+ " (is the connection pool jar available?) : ");
	  } catch (IllegalAccessException e) {
		e.printStackTrace();
		throw new SQLException(
		  "Failed to initialise database connection pool "
			+ "(is the connection pool jar available?) : ");
	  } catch (NoClassDefFoundError e) {
		e.printStackTrace();
		throw new SQLException(
		  "Failed to initialise database connection pool "
			+ "(is the connection pool jar available?) : ");
	  }
	  }
	  catch (SQLException e) {
		  String message = e.getMessage();
		  if (message.indexOf('\n')>=0)
			  message = message.substring(0, message.indexOf('\n'));
		  JOptionPane.showMessageDialog(null,message);
			  //return null;
			   //e.printStackTrace();
			  throw e;
	} 	  
}
/*    
	Connection conn;
	String version = null;
	try {
		  conn = dataSource.getConnection();
		  
		  ResultSet vr = conn.getMetaData().getTables(conn.getCatalog(), this.schema, "meta_version__version__main", null);
		  //expect at most one result, if no results, tcheck will remain null
		  String tcheck = null;
		  if (vr.next())
			tcheck = vr.getString(3);

		  vr.close();

		  if (tcheck == null) {// don't check databases with no version table yet
				return conn;
		  }
		  String[] schemas = null;
		  if(getDatabaseType().equals("oracle")) schemas = getSchema().toUpperCase().split(";");
		  else schemas = getSchema().split(";");
		  
		  PreparedStatement ps = conn.prepareStatement("select version from "+schemas[0]+".meta_version__version__main");
		  ResultSet rs = ps.executeQuery();
		  rs.next();
		  version = rs.getString(1);
		  rs.close();
		  if (!version.equals(VERSION)){
			throw new SQLException("Database version "+version+" and software version "+VERSION+" do not match");	
		  }
		  return conn;
	} catch (SQLException e) {
		JOptionPane.showMessageDialog(null,"Include a correct meta_version__version__main table entry:" + e);
		throw new SQLException("");
	} 	 
	 * 
	 */  
  

  /**
   * @param username
   * @param password
   * @return Connection
   * @throws java.sql.SQLException
   */
  public Connection getConnection(String username, String password)
    throws SQLException {
    return dataSource.getConnection(username, password);
  }

  /**
   * @return int loginTimeout
   * @throws java.sql.SQLException
   */
  public int getLoginTimeout() throws SQLException {
    return dataSource.getLoginTimeout();
  }

  /**
   * @return PrintWriter logWriter
   * @throws java.sql.SQLException
   */
  public PrintWriter getLogWriter() throws SQLException {
    return dataSource.getLogWriter();
  }

  /**
   * @param seconds
   * @throws java.sql.SQLException
   */
  public void setLoginTimeout(int seconds) throws SQLException {
    dataSource.setLoginTimeout(seconds);
  }

  /**
   * @param out
   * @throws java.sql.SQLException
   */
  public void setLogWriter(PrintWriter out) throws SQLException {
    dataSource.setLogWriter(out);
  }

  /**
   * @return String databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @return String databaseType
   */
  public String getDatabaseType() {
    return databaseType;
  }

  /**
   * @return DataSource dataSource
   */
  public DataSource getDatasource() {
    return dataSource;
  }

  /**
   * @return String name
   */
  public String getName() {
    return name;
  }

  
  public String getSchema() {
    return schema;
  }
  
  
  
  /**
   * @return String user
   */
  public String getUser() {
    return user;
  }
  
  /**
   * @return String user
   */
  public String getMartUser() {
	return martUser;
  }

  /**
   * @return String host
   */
  public String getHost() {
    return host;
  }

  /**
   * @return String jdbcDriverClassName
   */ 
  public String getJdbcDriverClassName() {
    return jdbcDriverClassName;
  }

  /**
   * @return String port
   */
  public String getPort() {
    return port;
  }

  /**
   * @return String JDBC Connection String
   */
  public String getConnectionString() {
    return connectionString;
  }

  /**
   * @return int maxPoolSize
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  /**
   * @return String password
   */
  public String getPassword() {
    return password;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();

    buf.append("[");
    buf.append(" databaseType=").append(databaseType);
    buf.append(", host=").append(host);
    buf.append(", port=").append(port);
    buf.append(", databaseName=").append(databaseName);
    buf.append(", schema=").append(schema);
    buf.append(", maxPoolSize=").append(maxPoolSize);
    buf.append(", password=").append(password);
    buf.append(", user=").append(user);
    buf.append(", jdbcDriverClassName=").append(jdbcDriverClassName);
    buf.append(", displayName=").append(name);
    buf.append(", dataSource=").append(dataSource);
    buf.append(", connectionString=").append(connectionString);
    buf.append("]");

    return buf.toString();
  }

/* (non-Javadoc)
 * @see java.sql.Wrapper#unwrap(java.lang.Class)
 */
@Override
public <T> T unwrap(Class<T> iface) throws SQLException {
	throw new UnsupportedOperationException();
}

/* (non-Javadoc)
 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
 */
@Override
public boolean isWrapperFor(Class<?> iface) throws SQLException {
	throw new UnsupportedOperationException();
}

}
