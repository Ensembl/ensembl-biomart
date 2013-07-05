/*
 Copyright (C) 2006 EBI
 
 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the itmplied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.biomart.builder.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * This interface defines the methods required to connect to a data source. It
 * doesn't define any data source specific methods, only those that are required
 * to make the rest of the system work without worrying about where the data is
 * coming from.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.15 $, $Date: 2007-09-13 14:00:33 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public interface DataLink {

	/**
	 * Gets the data source database name.
	 * 
	 * @return the data source database name.
	 */
	public String getDataLinkDatabase();

	/**
	 * Sets the data source database name.
	 * 
	 * @param databaseName
	 *            the data source database name.
	 */
	public void setDataLinkDatabase(String databaseName);

	/**
	 * Gets the data source schema name.
	 * 
	 * @return the data source schema name.
	 */
	public String getDataLinkSchema();

	/**
	 * Sets the data source schema name.
	 * 
	 * @param schemaName
	 *            the data source schema name.
	 */
	public void setDataLinkSchema(String schemaName);

	/**
	 * Checks to see if this datalink 'cohabits' with another one. Cohabitation
	 * means that it would be possible to write a single SQL statement that
	 * could read or write data from both this datalink and the specified
	 * partner simultaneously.
	 * 
	 * @param partner
	 *            the other datalink to test for cohabitation.
	 * @return <tt>true</tt> if the two can cohabit, <tt>false</tt> if not.
	 */
	public boolean canCohabit(DataLink partner);

	/**
	 * Checks to see if this datalink is working properly. Returns <tt>true</tt>
	 * if it is, otherwise throws an exception describing the problem. Should
	 * never return <tt>false</tt>.
	 * 
	 * @return <tt>true</tt> if the link is working. Should never return
	 *         <tt>false</tt>, as an exception will always be thrown if there
	 *         is a problem.
	 * @throws Exception
	 *             if there is a problem connecting to the data link. This
	 *             exception could be one of a number of types - usually it is
	 *             likely to be a {@link SQLException} if talking to a JDBC data
	 *             source, or a {@link IOException} if talking to a file-based
	 *             data source.
	 */
	public boolean test() throws Exception;

	/**
	 * This interface defines methods required for JDBC connections only. Note
	 * that the schema name is the name of the owner of the tables. This is
	 * distinct from the database name which is part of the JDBC URL.
	 */
	public interface JDBCDataLink extends DataLink {
		/**
		 * Returns a JDBC connection connected to this database using the data
		 * supplied to all the other methods in this interface.
		 * 
		 * @param overrideDataLinkSchema
		 *            the schema to connect to, if any. <tt>null</tt> is used
		 *            where the default main schema is suitable.
		 * @return the connection for this database.
		 * @throws SQLException
		 *             if there was any problem connecting.
		 */
		public Connection getConnection(final String overrideDataLinkSchema)
				throws SQLException;

		/**
		 * Getter for the name of the driver class, eg.
		 * <tt>com.mysql.jdbc.Driver</tt>
		 * 
		 * @return the name of the driver class.
		 */
		public String getDriverClassName();

		/**
		 * Gets the JDBC URL.
		 * 
		 * @return the JDBC URL.
		 */
		public String getUrl();

		/**
		 * Gets the password. May be <tt>null</tt> which would indicate that
		 * no password is required.
		 * 
		 * @return the password.
		 */
		public String getPassword();

		/**
		 * Gets the username.
		 * 
		 * @return the username.
		 */
		public String getUsername();

		/**
		 * Sets the name of the driver class, eg. <tt>com.mysql.jdbc.Driver</tt>
		 * 
		 * @param driverClassName
		 *            the name of the driver class.
		 */
		public void setDriverClassName(String driverClassName);

		/**
		 * Sets the JDBC URL.
		 * 
		 * @param url
		 *            the JDBC URL.
		 */
		public void setUrl(String url);

		/**
		 * Sets the password. If <tt>null</tt>, then no password will be
		 * used.
		 * 
		 * @param password
		 *            the password.
		 */
		public void setPassword(String password);

		/**
		 * Sets the username.
		 * 
		 * @param username
		 *            the username.
		 */
		public void setUsername(String username);
	}

	/**
	 * This interface defines methods required for XML files only.
	 * <p>
	 * TODO: Work out how it's going to work, then define it.
	 */
	public interface XMLDataLink extends DataLink {
	}
}
