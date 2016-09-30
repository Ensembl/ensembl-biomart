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
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

package org.ensembl.mart.lib.config;

import java.sql.SQLException;

import org.ensembl.mart.lib.DetailedDataSource;

/**
 * Object representing a RegistryDBLocation element in a DatasetConfigLocation element
 * within a MartRegistry.dtd compliant XML document.  
 * Note, this object has the capability of storing database passwords, but does not do anything
 * to make them secure.  Users are encouraged to use readonly, passwordless access, or user-password
 * combinations for users with limited privileges.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class RegistryDBLocation extends MartLocationBase {

	private final String HOST_KEY = "host";
	private final String PORT_KEY = "port";
	private final String DATABASE_TYPE_KEY = "databaseType";
	private final String INSTANCE_NAME_KEY = "database";
	private final String SCHEMA_KEY = "schema";
	private final String USER_KEY = "user";
	private final String PASSWORD_KEY = "password";

	public RegistryDBLocation() {
		super();
		type = MartLocationBase.REGISTRYDB;
	}

	public RegistryDBLocation(
		String host,
		String port,
		String databaseType,
		String instanceName,
		String schema,
		String user,
		String password,
		String name,
        String visibleString)
		throws ConfigurationException {
		super(name, visibleString, MartLocationBase.DATABASE);

		if (host == null || instanceName == null || user == null || schema == null)
			throw new ConfigurationException("RegistryDBLocation Objects must contain a host, user, schema and database\n");

		setAttribute(HOST_KEY, host);
		setAttribute(INSTANCE_NAME_KEY, instanceName);
		setAttribute(SCHEMA_KEY, schema);
		setAttribute(USER_KEY, user);
		setAttribute(DATABASE_TYPE_KEY, databaseType);
		setAttribute(PORT_KEY, port);
		setAttribute(PASSWORD_KEY, password);
	}

	/**
	 * Returns the type of RDBMS serving this location.  This may be null.
	 * @return String databaseType
	 */
	public String getDatabaseType() {
		return getAttribute(DATABASE_TYPE_KEY);
	}

	public void setDatabaseType(String databaseType) {
		setAttribute(DATABASE_TYPE_KEY, databaseType);
	}

	/**
	 * Returns the host for the RDBMS serving this location.
	 * @return String host
	 */
	public String getHost() {
		return getAttribute(HOST_KEY);
	}

	public void setHost(String host) {
		setAttribute(HOST_KEY, host);
	}

	
	public String getSchema() {
		return getAttribute(SCHEMA_KEY);
	}

	public void setSchema(String schema) {
		setAttribute(SCHEMA_KEY, schema);
	}

	
	
	
	
	/**
	 * Returns the name of the Mart instance.
	 * @return String instanceName
	 */
	public String getInstanceName() {
		return getAttribute(INSTANCE_NAME_KEY);
	}

	public void setInstanceName(String instanceName) {
		setAttribute(INSTANCE_NAME_KEY, instanceName);
	}

	/**
	 * Returns the password for the RDBMS serving this location.  This may be null.
	 * @return String password
	 */
	public String getPassword() {
		return getAttribute(PASSWORD_KEY);
	}

	public void setPassword(String password) {
		setAttribute(PASSWORD_KEY, password);
	}

	/**
	 * Returns the port for the RDBMS serving this location.  This may be null.
	 * @return String port
	 */
	public String getPort() {
		return getAttribute(PORT_KEY);
	}

	public void setPort(String port) {
		setAttribute(PORT_KEY, port);
	}

	/**
	 * Returns the user for the RDBMS serving this location.
	 * @return String user
	 */
	public String getUser() {
		return getAttribute(USER_KEY);
	}

	public void setUser(String user) {
		setAttribute(USER_KEY, user);
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
		buf.append("]");

		return buf.toString();
	}

	public DetailedDataSource getDetailedDataSource() throws ConfigurationException {
		String host = getHost();
		String user = getUser();
		String instanceName = getInstanceName();
		String schema = getSchema();
		String port = getPort();
		String password = getPassword();
		String databaseType = getDatabaseType();
		String jdbcDriverClassName = DetailedDataSource.getJDBCDriverClassNameFor(databaseType);
		String name = getName();

		// apply defaults only if both dbtype and jdbcdriver are null
		if (databaseType == null && jdbcDriverClassName == null) {
			databaseType = DetailedDataSource.DEFAULTDATABASETYPE;
			jdbcDriverClassName = DetailedDataSource.DEFAULTDRIVER;
		}

		String connectionString = DetailedDataSource.connectionURL(databaseType, host, port, instanceName);
		// use default name
		if (name == null || "".equals(name))
			name = connectionString;

		//use the default poolsize of 10        
		DetailedDataSource dsource =
			new DetailedDataSource(
				databaseType,
				host,
				port,
				instanceName,
				//connectionString,
				schema,
				user,
				password,
				DetailedDataSource.DEFAULTPOOLSIZE,
				jdbcDriverClassName,
				name);

		try {
			dsource.getConnection();
		} catch (SQLException e) {
			throw new ConfigurationException(
				"Recieved invalid DataSource specification for Registry DataSource: " + e.getMessage() + "\n",
				e);
		}
		return dsource;
	}

	/**
	 * Allows Equality Comparisons manipulation of RegistryDBLocation objects
	 */
	public boolean equals(Object o) {
		return o instanceof RegistryDBLocation && hashCode() == o.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return super.hashCode();
	}

	/**
	 * @return "name" if set, otherwise uses DetailedDataSource.simpleRepresentation( getHost(), getPort(), getInstanceName() )
	 * @see org.ensembl.mart.lib.config.MartLocation#getName()
	 */
	public String getName() {
		String name = super.getName();
		if (name == null || "".equals(name))
			name = DetailedDataSource.defaultName(getHost(), getPort(), getInstanceName(), getSchema(),getUser());
		return name;
	}
}
